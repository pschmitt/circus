//! Cross-process demand signal for the slow, emulated ARM overflow pool.

use std::{io, path::Path};

use serde::{Deserialize, Serialize};

/// A short-lived snapshot published by the native ARM autoscaler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OverflowDemandReport {
  /// Unix timestamp in seconds when this report was written.
  pub published_at:             i64,
  /// Mature trusted ARM builds left after current native and native-launch
  /// capacity has been accounted for.
  pub overflow_count:           u32,
  /// Native free slots that matched eligible queued builds.
  pub native_live_capacity:     u32,
  /// Unregistered native workflow launches that may still provide capacity.
  pub native_inflight_capacity: u32,
}

/// Write a complete status snapshot and atomically replace the previous one.
///
/// # Errors
///
/// Returns an error if the containing directory or temporary file cannot be
/// written, or if the atomic rename fails.
pub async fn write_atomic(
  path: &Path,
  report: OverflowDemandReport,
) -> io::Result<()> {
  let parent = path.parent().unwrap_or_else(|| Path::new("."));
  tokio::fs::create_dir_all(parent).await?;
  let temporary = parent.join(format!(
    ".{}.{}.tmp",
    path
      .file_name()
      .and_then(|name| name.to_str())
      .unwrap_or("overflow"),
    uuid::Uuid::new_v4()
  ));
  let bytes = serde_json::to_vec(&report).map_err(io::Error::other)?;
  let result = async {
    tokio::fs::write(&temporary, bytes).await?;
    tokio::fs::rename(&temporary, path).await
  }
  .await;
  if result.is_err() {
    let _ = tokio::fs::remove_file(&temporary).await;
  }
  result
}

/// Read a status snapshot only while it is strictly fresh.
///
/// Missing, malformed, future-dated, or expired reports return `Ok(None)` so
/// callers fail closed and never create overflow capacity from stale state.
///
/// # Errors
///
/// Returns an error for file I/O failures other than a missing file.
pub async fn read_fresh(
  path: &Path,
  max_age_secs: u64,
) -> io::Result<Option<OverflowDemandReport>> {
  let bytes = match tokio::fs::read(path).await {
    Ok(bytes) => bytes,
    Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
    Err(error) => return Err(error),
  };
  let Ok(report) = serde_json::from_slice::<OverflowDemandReport>(&bytes)
  else {
    return Ok(None);
  };
  let now = jiff::Timestamp::now().as_second();
  let Some(age) = now.checked_sub(report.published_at) else {
    return Ok(None);
  };
  if age < 0 || age >= i64::try_from(max_age_secs).unwrap_or(i64::MAX) {
    return Ok(None);
  }
  Ok(Some(report))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn atomic_report_round_trips_and_expires() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("overflow-demand.json");
    let report = OverflowDemandReport {
      published_at:             jiff::Timestamp::now().as_second(),
      overflow_count:           2,
      native_live_capacity:     0,
      native_inflight_capacity: 1,
    };

    write_atomic(&path, report).await.expect("write report");
    assert_eq!(read_fresh(&path, 30).await.unwrap(), Some(report));
    assert_eq!(read_fresh(&path, 0).await.unwrap(), None);

    let expired = OverflowDemandReport {
      published_at: report.published_at.saturating_sub(31),
      ..report
    };
    write_atomic(&path, expired)
      .await
      .expect("write expired report");
    assert_eq!(read_fresh(&path, 30).await.unwrap(), None);
  }

  #[tokio::test]
  async fn malformed_and_future_reports_fail_closed() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("overflow-demand.json");
    tokio::fs::write(&path, b"not json").await.unwrap();
    assert_eq!(read_fresh(&path, 30).await.unwrap(), None);

    let future = OverflowDemandReport {
      published_at:             jiff::Timestamp::now()
        .as_second()
        .saturating_add(60),
      overflow_count:           1,
      native_live_capacity:     0,
      native_inflight_capacity: 0,
    };
    write_atomic(&path, future)
      .await
      .expect("write future report");
    assert_eq!(read_fresh(&path, 300).await.unwrap(), None);
  }
}
