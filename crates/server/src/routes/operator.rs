use axum::{
  Json,
  Router,
  extract::{Query, State},
  http::Extensions,
  routing::get,
};
use serde::Deserialize;

use crate::{error::ApiError, operator, permissions, state::AppState};

async fn overview(
  State(state): State<AppState>,
  extensions: Extensions,
) -> Result<Json<operator::OperatorOverview>, ApiError> {
  Ok(Json(
    operator::overview(
      &state,
      permissions::check(&extensions, permissions::Permission::Admin),
    )
    .await?,
  ))
}

async fn failures(
  State(state): State<AppState>,
) -> Result<Json<Vec<operator::OperatorBuild>>, ApiError> {
  Ok(Json(operator::failures(&state).await?))
}

async fn recent_builds(
  State(state): State<AppState>,
) -> Result<Json<Vec<operator::OperatorBuild>>, ApiError> {
  Ok(Json(operator::recent_builds(&state).await?))
}

async fn projects(
  State(state): State<AppState>,
  extensions: Extensions,
) -> Result<Json<Vec<operator::OperatorProject>>, ApiError> {
  Ok(Json(
    operator::projects(
      &state,
      permissions::check(&extensions, permissions::Permission::Admin),
    )
    .await?,
  ))
}

#[derive(Debug, Deserialize)]
struct QueueParams {
  /// Comma-separated features of the asking pool.
  features: Option<String>,
}

async fn queue(
  State(state): State<AppState>,
  Query(params): Query<QueueParams>,
) -> Result<Json<Vec<operator::OperatorQueueSystem>>, ApiError> {
  let features = params.features.map(|features| {
    features
      .split(',')
      .filter(|feature| !feature.is_empty())
      .map(str::to_owned)
      .collect::<Vec<_>>()
  });
  Ok(Json(operator::queue(&state, features.as_deref()).await?))
}

async fn workers(
  State(state): State<AppState>,
) -> Result<Json<Vec<operator::OperatorWorker>>, ApiError> {
  Ok(Json(operator::worker_summary(&state).await?))
}

pub fn router() -> Router<AppState> {
  Router::new()
    .route("/operator/overview", get(overview))
    .route("/operator/failures", get(failures))
    .route("/operator/recent-builds", get(recent_builds))
    .route("/operator/projects", get(projects))
    .route("/operator/queue", get(queue))
    .route("/operator/workers", get(workers))
}
