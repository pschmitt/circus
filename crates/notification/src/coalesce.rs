use std::{
  collections::HashSet,
  future::Future,
  sync::{Arc, Mutex, PoisonError},
  time::Duration,
};

use tokio::sync::watch;
use uuid::Uuid;

/// Runs background tasks, merging requests for a key that is already waiting.
pub struct Coalescer {
  waiting: Arc<Mutex<HashSet<Uuid>>>,
  active:  watch::Sender<usize>,
}

impl Default for Coalescer {
  fn default() -> Self {
    Self {
      waiting: Arc::default(),
      active:  watch::Sender::new(0),
    }
  }
}

/// Counts a task as finished even when it panics.
struct ActiveGuard(watch::Sender<usize>);

impl Drop for ActiveGuard {
  fn drop(&mut self) {
    self.0.send_modify(|active| *active -= 1);
  }
}

impl Coalescer {
  /// Run `task` after `delay`, unless one for `key` is still waiting. A request
  /// made after that task started schedules another, so the last one always
  /// sees the latest state.
  pub fn schedule(
    &self,
    key: Uuid,
    delay: Duration,
    task: impl Future<Output = ()> + Send + 'static,
  ) {
    if !self
      .waiting
      .lock()
      .unwrap_or_else(PoisonError::into_inner)
      .insert(key)
    {
      return;
    }
    self.active.send_modify(|active| *active += 1);
    let guard = ActiveGuard(self.active.clone());
    let waiting = Arc::clone(&self.waiting);
    tokio::spawn(async move {
      let _guard = guard;
      tokio::time::sleep(delay).await;
      waiting
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&key);
      task.await;
    });
  }

  /// True once no task is waiting or running, false after `timeout`.
  pub async fn wait_idle(&self, timeout: Duration) -> bool {
    let mut active = self.active.subscribe();
    tokio::time::timeout(timeout, active.wait_for(|active| *active == 0))
      .await
      .is_ok_and(|result| result.is_ok())
  }
}

#[cfg(test)]
mod tests {
  use std::sync::atomic::{AtomicUsize, Ordering};

  use super::*;

  const DELAY: Duration = Duration::from_millis(50);
  const LIMIT: Duration = Duration::from_secs(5);

  fn count(runs: &Arc<AtomicUsize>) -> impl Future<Output = ()> + use<> {
    let runs = Arc::clone(runs);
    async move {
      runs.fetch_add(1, Ordering::SeqCst);
    }
  }

  #[tokio::test]
  async fn merges_waiting_requests_per_key() {
    let coalescer = Coalescer::default();
    let (first, second) = (Uuid::new_v4(), Uuid::new_v4());
    let runs = Arc::new(AtomicUsize::new(0));
    for _ in 0..100 {
      coalescer.schedule(first, DELAY, count(&runs));
      coalescer.schedule(second, DELAY, count(&runs));
    }
    assert!(coalescer.wait_idle(LIMIT).await);
    assert_eq!(runs.load(Ordering::SeqCst), 2);
  }

  #[tokio::test]
  async fn request_during_a_run_schedules_another() {
    let coalescer = Coalescer::default();
    let key = Uuid::new_v4();
    let runs = Arc::new(AtomicUsize::new(0));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let first = count(&runs);
    coalescer.schedule(key, Duration::ZERO, async move {
      let _ = started_tx.send(());
      let _ = release_rx.await;
      first.await;
    });
    assert!(started_rx.await.is_ok());
    coalescer.schedule(key, Duration::ZERO, count(&runs));
    let _ = release_tx.send(());
    assert!(coalescer.wait_idle(LIMIT).await);
    assert_eq!(runs.load(Ordering::SeqCst), 2);
  }

  #[tokio::test]
  async fn wait_idle_gives_up_after_the_timeout() {
    let coalescer = Coalescer::default();
    coalescer.schedule(Uuid::new_v4(), LIMIT, async {});
    assert!(!coalescer.wait_idle(DELAY).await);
  }
}
