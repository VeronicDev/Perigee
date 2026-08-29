//! # Perigee Job Queue Module
//!
//! Implements a dedicated background job queue using `tokio::spawn` with a
//! bounded semaphore to prevent unbounded task proliferation. Long-running
//! tasks (batch rotations, reconciliation, etc.) are offloaded from the main
//! async runtime so they never block API handlers.
//!
//! ## Features
//! - **Bounded concurrency** — a `tokio::sync::Semaphore` limits how many
//!   jobs run simultaneously (default: 8).
//! - **Status tracking** — every job transitions through
//!   `PENDING → RUNNING → COMPLETED | FAILED`.
//! - **REST endpoint** — `GET /jobs/{id}` returns the current job state for
//!   client-side polling.
//! - **Typed results** — jobs return `serde_json::Value` results that callers
//!   can inspect.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use utoipa::ToSchema;
use uuid::Uuid;

// ── Job status ───────────────────────────────────────────────────────────────

/// Lifecycle states for a background job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum JobStatus {
    /// Waiting for an available worker slot.
    Pending,
    /// Actively executing.
    Running,
    /// Finished without error.
    Completed,
    /// Finished with an error.
    Failed,
}

// ── Job record ───────────────────────────────────────────────────────────────

/// A single background job with full lifecycle metadata.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Job {
    /// Unique identifier.
    pub id: String,
    /// Human-readable task name.
    pub name: String,
    /// Current lifecycle state.
    pub status: JobStatus,
    /// When the job was submitted.
    pub created_at: DateTime<Utc>,
    /// When execution started (populated once the worker picks it up).
    pub started_at: Option<DateTime<Utc>>,
    /// When execution finished (populated on completion or failure).
    pub completed_at: Option<DateTime<Utc>>,
    /// Error message when `status == FAILED`.
    pub error: Option<String>,
    /// JSON-serialised result when `status == COMPLETED`.
    pub result: Option<serde_json::Value>,
}

// ── Error type ───────────────────────────────────────────────────────────────

/// Errors specific to the jobs subsystem.
#[derive(Debug, thiserror::Error)]
pub enum JobError {
    #[error("Job not found: {0}")]
    NotFound(String),

    #[error("Job queue is at capacity — try again later")]
    QueueFull,

    #[error("Internal error: {0}")]
    Internal(String),
}

impl IntoResponse for JobError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            JobError::NotFound(id) => (StatusCode::NOT_FOUND, format!("Job not found: {}", id)),
            JobError::QueueFull => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Job queue is at capacity".into(),
            ),
            JobError::Internal(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal error: {}", msg),
            ),
        };

        let body = serde_json::json!({ "error": message });
        (status, Json(body)).into_response()
    }
}

// ── Job handle (returned to callers for submission) ───────────────────────────

/// A thin handle returned by [`JobQueue::submit`]. Contains the job ID and a
/// oneshot-style result receiver.
pub struct JobHandle {
    pub id: String,
}

// ── Job queue ────────────────────────────────────────────────────────────────

/// Type-erased async job function.
type BoxFuture = Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'static>>;

struct PendingJob {
    id: String,
    name: String,
    created_at: DateTime<Utc>,
    future: BoxFuture,
}

/// The central job queue. Clone it (cheap `Arc`) and share across handlers.
#[derive(Clone)]
pub struct JobQueue {
    jobs: Arc<Mutex<HashMap<String, Job>>>,
    semaphore: Arc<Semaphore>,
    /// Sender side of the pending-jobs channel.
    tx: tokio::sync::mpsc::UnboundedSender<PendingJob>,
}

impl JobQueue {
    /// Create a new queue with the given maximum concurrent worker count.
    pub fn new(max_concurrency: usize) -> Self {
        let jobs: Arc<Mutex<HashMap<String, Job>>> = Arc::new(Mutex::new(HashMap::new()));
        let semaphore = Arc::new(Semaphore::new(max_concurrency));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<PendingJob>();

        // Spawn a dedicated dispatcher task that pulls jobs from the channel
        // and runs them on the tokio runtime with semaphore-gated concurrency.
        let jobs_clone = jobs.clone();
        let sem_clone = semaphore.clone();
        tokio::spawn(Self::dispatcher_loop(rx, jobs_clone, sem_clone));

        Self {
            jobs,
            semaphore,
            tx,
        }
    }

    /// Create a queue with the default concurrency limit (8 workers).
    pub fn default_queue() -> Self {
        Self::new(8)
    }

    // ── Public API ──────────────────────────────────────────────────────

    /// Submit a named background job. Returns a [`JobHandle`] with the ID
    /// that callers can use with [`GET /jobs/{id}`] to poll status.
    pub async fn submit<F>(&self, name: &str, future: F) -> Result<JobHandle, JobError>
    where
        F: Future<Output = Result<serde_json::Value, String>> + Send + 'static,
    {
        let id = Uuid::new_v4().to_string();
        let created_at = Utc::now();

        // Record the job as PENDING immediately so the polling endpoint can
        // see it before the dispatcher picks it up.
        {
            let mut jobs = self.jobs.lock().await;
            jobs.insert(
                id.clone(),
                Job {
                    id: id.clone(),
                    name: name.to_string(),
                    status: JobStatus::Pending,
                    created_at,
                    started_at: None,
                    completed_at: None,
                    error: None,
                    result: None,
                },
            );
        }

        let pending = PendingJob {
            id: id.clone(),
            name: name.to_string(),
            created_at,
            future: Box::pin(future),
        };

        self.tx
            .send(pending)
            .map_err(|_| JobError::QueueFull)?;

        Ok(JobHandle { id })
    }

    /// Look up a job by ID.
    pub async fn get(&self, id: &str) -> Option<Job> {
        let jobs = self.jobs.lock().await;
        jobs.get(id).cloned()
    }

    /// List all jobs (newest first).
    pub async fn list(&self) -> Vec<Job> {
        let jobs = self.jobs.lock().await;
        let mut list: Vec<Job> = jobs.values().cloned().collect();
        list.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        list
    }

    /// Number of available worker slots (0 means all slots occupied).
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    // ── Internal dispatcher ─────────────────────────────────────────────

    async fn dispatcher_loop(
        mut rx: tokio::sync::mpsc::UnboundedReceiver<PendingJob>,
        jobs: Arc<Mutex<HashMap<String, Job>>>,
        semaphore: Arc<Semaphore>,
    ) {
        while let Some(pending) = rx.recv().await {
            let jobs = jobs.clone();
            let sem = semaphore.clone();

            // We intentionally do NOT `.await` the semaphore acquire here
            // because that would block the dispatcher from draining the
            // channel. Instead we spawn each job into its own task that
            // acquires the permit before running.
            tokio::spawn(async move {
                // Wait for a free slot.
                let _permit = sem.acquire().await.expect("semaphore closed");

                // Transition: PENDING → RUNNING
                {
                    let mut jobs = jobs.lock().await;
                    if let Some(job) = jobs.get_mut(&pending.id) {
                        job.status = JobStatus::Running;
                        job.started_at = Some(Utc::now());
                    }
                }

                // Execute the actual work.
                let outcome = pending.future.await;

                // Transition: → COMPLETED | FAILED
                {
                    let mut jobs = jobs.lock().await;
                    if let Some(job) = jobs.get_mut(&pending.id) {
                        job.completed_at = Some(Utc::now());
                        match outcome {
                            Ok(val) => {
                                job.status = JobStatus::Completed;
                                job.result = Some(val);
                            }
                            Err(msg) => {
                                job.status = JobStatus::Failed;
                                job.error = Some(msg);
                            }
                        }
                    }
                }
                // `_permit` dropped here → slot released.
            });
        }
    }
}

// ── Axum handlers ────────────────────────────────────────────────────────────

/// Application state shared across job handlers.
#[derive(Clone)]
pub struct JobsState {
    pub queue: JobQueue,
}

/// POST /jobs — Submit a new background job.
///
/// Request body contains the job name and optional JSON payload that will be
/// passed through to the job function (implementation-specific).
#[utoipa::path(
    post,
    path = "/jobs",
    request_body = SubmitJobRequest,
    responses(
        (status = 202, description = "Job accepted", body = Job),
        (status = 503, description = "Queue full"),
        (status = 500, description = "Internal server error")
    ),
    tag = "Jobs"
)]
pub async fn submit_job_handler(
    State(_state): State<JobsState>,
    Json(_payload): Json<SubmitJobRequest>,
) -> Result<(StatusCode, Json<Job>), JobError> {
    // NOTE: In production the actual job closure would be wired up here
    // based on `payload.name`. This is the integration point where the
    // router maps job names to concrete async functions.
    //
    // Example:
    //   let handle = state.queue.submit(&payload.name, async move {
    //       // ... do work ...
    //       Ok(serde_json::json!({ "status": "done" }))
    //   }).await?;
    //
    // For now we return a placeholder to demonstrate the endpoint contract.
    Err(JobError::Internal(
        "Job dispatch not yet wired — attach a handler in submit_job_handler".into(),
    ))
}

/// Request body for `POST /jobs`.
#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct SubmitJobRequest {
    /// A human-readable name for the job (e.g. `"batch_rotation"`,
    /// `"reconciliation"`).
    pub name: String,
    /// Arbitrary JSON payload for the job.
    #[serde(default)]
    pub payload: Option<serde_json::Value>,
}

/// GET /jobs — List all jobs.
#[utoipa::path(
    get,
    path = "/jobs",
    responses(
        (status = 200, description = "List of jobs", body = Vec<Job>),
    ),
    tag = "Jobs"
)]
pub async fn list_jobs_handler(
    State(state): State<JobsState>,
) -> Json<Vec<Job>> {
    Json(state.queue.list().await)
}

/// GET /jobs/{id} — Poll job status.
#[utoipa::path(
    get,
    path = "/jobs/{id}",
    params(("id" = String, Path, description = "Job ID")),
    responses(
        (status = 200, description = "Job details", body = Job),
        (status = 404, description = "Job not found"),
    ),
    tag = "Jobs"
)]
pub async fn get_job_handler(
    State(state): State<JobsState>,
    Path(id): Path<String>,
) -> Result<Json<Job>, JobError> {
    state
        .queue
        .get(&id)
        .await
        .map(Json)
        .ok_or(JobError::NotFound(id))
}

/// Build an Axum router with the `/jobs` endpoints.
pub fn jobs_routes(state: JobsState) -> Router<JobsState> {
    Router::new()
        .route("/jobs", post(submit_job_handler).get(list_jobs_handler))
        .route("/jobs/{id}", get(get_job_handler))
        .with_state(state)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn submit_and_track_job_lifecycle() {
        let queue = JobQueue::new(2);

        // Submit a job that completes immediately.
        let handle = queue
            .submit("test_task", async {
                Ok(serde_json::json!({ "processed": true }))
            })
            .await
            .expect("submit should succeed");

        // Wait for the job to complete.
        let mut job = None;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let j = queue.get(&handle.id).await.unwrap();
            if j.status == JobStatus::Completed || j.status == JobStatus::Failed {
                job = Some(j);
                break;
            }
        }

        let job = job.expect("job should have completed");
        assert_eq!(job.status, JobStatus::Completed);
        assert!(job.started_at.is_some());
        assert!(job.completed_at.is_some());
        assert_eq!(
            job.result,
            Some(serde_json::json!({ "processed": true }))
        );
    }

    #[tokio::test]
    async fn failed_job_records_error() {
        let queue = JobQueue::new(2);

        let handle = queue
            .submit("failing_task", async { Err("boom".into()) })
            .await
            .expect("submit should succeed");

        let mut job = None;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let j = queue.get(&handle.id).await.unwrap();
            if j.status == JobStatus::Failed {
                job = Some(j);
                break;
            }
        }

        let job = job.expect("job should have failed");
        assert_eq!(job.status, JobStatus::Failed);
        assert_eq!(job.error, Some("boom".into()));
        assert!(job.result.is_none());
    }

    #[tokio::test]
    async fn bounded_concurrency_limits_parallelism() {
        // Only 1 concurrent job.
        let queue = JobQueue::new(1);
        let counter = Arc::new(tokio::sync::Mutex::new(0u32));
        let max_seen = Arc::new(tokio::sync::Mutex::new(0u32));

        let mut handles = Vec::new();
        for i in 0..5 {
            let counter = counter.clone();
            let max_seen = max_seen.clone();
            let h = queue
                .submit(
                    &format!("job_{}", i),
                    async move {
                        let mut c = counter.lock().await;
                        *c += 1;
                        let current = *c;
                        drop(c);

                        let mut ms = max_seen.lock().await;
                        if current > *ms {
                            *ms = current;
                        }
                        drop(ms);

                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                        let mut c = counter.lock().await;
                        *c -= 1;
                        drop(c);

                        Ok(serde_json::json!({ "job": i }))
                    },
                )
                .await
                .unwrap();
            handles.push(h);
        }

        // Wait for all jobs to complete.
        for _ in 0..200 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let mut completed_count = 0;
            for h in &handles {
                if let Some(j) = queue.get(&h.id).await {
                    if j.status == JobStatus::Completed {
                        completed_count += 1;
                    }
                }
            }
            if completed_count == handles.len() {
                break;
            }
        }

        let ms = *max_seen.lock().await;
        assert!(
            ms <= 1,
            "Expected max 1 concurrent job, saw {}",
            ms
        );
    }

    #[tokio::test]
    async fn get_nonexistent_job_returns_none() {
        let queue = JobQueue::new(2);
        assert!(queue.get("nonexistent-id").await.is_none());
    }

    #[tokio::test]
    async fn list_returns_jobs_newest_first() {
        let queue = JobQueue::new(4);

        let h1 = queue
            .submit("first", async { Ok(serde_json::json!(1)) })
            .await
            .unwrap();
        let h2 = queue
            .submit("second", async { Ok(serde_json::json!(2)) })
            .await
            .unwrap();

        // Wait for both to land in the store.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let list = queue.list().await;
        assert!(list.len() >= 2);
        // Newest first — second should come before first.
        let pos1 = list.iter().position(|j| j.id == h1.id).unwrap();
        let pos2 = list.iter().position(|j| j.id == h2.id).unwrap();
        assert!(
            pos2 < pos1,
            "Second job should appear before first in newest-first list"
        );
    }

    #[tokio::test]
    async fn job_pending_status_visible_immediately() {
        let queue = JobQueue::new(1);

        // Saturate the single slot.
        let blocker = queue
            .submit("blocker", async {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                Ok(serde_json::json!("done"))
            })
            .await
            .unwrap();

        // Let the blocker grab the slot.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // This job should stay PENDING.
        let delayed = queue
            .submit("delayed", async { Ok(serde_json::json!("ok")) })
            .await
            .unwrap();

        // Immediately check — should be PENDING.
        let job = queue.get(&delayed.id).await.unwrap();
        assert_eq!(job.status, JobStatus::Pending);
    }
}
