use std::{
  sync::{Arc, Mutex},
  time::{Duration, Instant},
};

use axum::{
  Extension,
  body::Body,
  extract::Request,
  middleware::Next,
  response::{IntoResponse, Response},
};
use uuid::Uuid;

struct WebhookBucket {
  tokens:        f64,
  last_refilled: Instant,
}

impl WebhookBucket {
  const fn new(tokens: f64, now: Instant) -> Self {
    Self {
      tokens,
      last_refilled: now,
    }
  }

  fn is_expired(&self, now: Instant) -> bool {
    now.duration_since(self.last_refilled) >= WEBHOOK_BUCKET_TTL
  }

  fn refill(&mut self, rps: f64, burst: f64, now: Instant) {
    let elapsed = now.duration_since(self.last_refilled).as_secs_f64();
    self.tokens = elapsed.mul_add(rps, self.tokens).min(burst);
    self.last_refilled = now;
  }

  fn try_consume(&mut self) -> bool {
    if self.tokens < 1.0 {
      return false;
    }
    self.tokens -= 1.0;
    true
  }
}

pub(super) struct WebhookRateLimiter {
  buckets:      dashmap::DashMap<Uuid, WebhookBucket>,
  app_bucket:   Mutex<WebhookBucket>,
  rps:          f64,
  burst:        f64,
  app_rps:      f64,
  app_burst:    f64,
  last_cleanup: Mutex<Instant>,
}

pub(super) const WEBHOOK_PROJECT_RATE_LIMIT: u32 = 10;
pub(super) const WEBHOOK_RATE_LIMIT_WINDOW: Duration = Duration::from_mins(1);
const WEBHOOK_APP_RATE_LIMIT: u32 = 600;
const WEBHOOK_APP_BURST: u32 = 100;
const WEBHOOK_BUCKET_TTL: Duration = Duration::from_mins(5);
const WEBHOOK_BUCKET_CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

impl WebhookRateLimiter {
  pub(super) fn new() -> Self {
    Self::new_at(Instant::now())
  }

  pub(super) fn new_at(now: Instant) -> Self {
    Self {
      buckets:      dashmap::DashMap::new(),
      app_bucket:   Mutex::new(WebhookBucket::new(
        f64::from(WEBHOOK_APP_BURST),
        now,
      )),
      rps:          f64::from(WEBHOOK_PROJECT_RATE_LIMIT)
        / WEBHOOK_RATE_LIMIT_WINDOW.as_secs_f64(),
      burst:        f64::from(WEBHOOK_PROJECT_RATE_LIMIT),
      app_rps:      f64::from(WEBHOOK_APP_RATE_LIMIT)
        / WEBHOOK_RATE_LIMIT_WINDOW.as_secs_f64(),
      app_burst:    f64::from(WEBHOOK_APP_BURST),
      last_cleanup: Mutex::new(now),
    }
  }

  pub(super) fn allow(&self, project_id: Uuid, now: Instant) -> bool {
    self.cleanup_expired(now);

    let mut bucket = self
      .buckets
      .entry(project_id)
      .or_insert_with(|| WebhookBucket::new(self.burst, now));

    bucket.refill(self.rps, self.burst, now);
    bucket.try_consume()
  }

  pub(super) fn allow_app(&self, now: Instant) -> bool {
    let Ok(mut bucket) = self.app_bucket.lock() else {
      return false;
    };
    bucket.refill(self.app_rps, self.app_burst, now);
    bucket.try_consume()
  }

  fn cleanup_expired(&self, now: Instant) {
    let Ok(mut last_cleanup) = self.last_cleanup.try_lock() else {
      return;
    };
    if now.duration_since(*last_cleanup) < WEBHOOK_BUCKET_CLEANUP_INTERVAL {
      return;
    }

    self.buckets.retain(|_, bucket| !bucket.is_expired(now));
    *last_cleanup = now;
  }
}

pub(super) async fn middleware(
  Extension(limiter): Extension<Arc<WebhookRateLimiter>>,
  request: Request<Body>,
  next: Next,
) -> Response {
  if let Some(project_id) = project_id_from_path(request.uri().path())
    && !limiter.allow(project_id, Instant::now())
  {
    return super::rate_limit_response().into_response();
  }

  next.run(request).await
}

fn project_id_from_path(path: &str) -> Option<Uuid> {
  let mut segments = path.split('/');
  if segments.next()? != ""
    || segments.next()? != "api"
    || segments.next()? != "v1"
    || segments.next()? != "webhooks"
  {
    return None;
  }
  let candidate = segments.next()?;
  if candidate == "github" {
    return None;
  }
  Uuid::parse_str(candidate).ok()
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn project_id_is_read_only_from_project_scoped_paths() {
    let id = Uuid::new_v4();
    assert_eq!(
      project_id_from_path(&format!("/api/v1/webhooks/{id}/github")),
      Some(id)
    );
    assert_eq!(project_id_from_path("/api/v1/webhooks/github"), None);
    assert_eq!(
      project_id_from_path("/api/v1/webhooks/not-a-uuid/github"),
      None
    );
  }

  #[test]
  fn generic_app_bucket_has_its_own_limit() {
    let now = Instant::now();
    let limiter = WebhookRateLimiter::new_at(now);
    for _ in 0..WEBHOOK_APP_BURST {
      assert!(limiter.allow_app(now));
    }
    assert!(!limiter.allow_app(now));
  }
}
