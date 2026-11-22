use std::{
  collections::BTreeSet,
  ffi::OsStr,
  fs,
  os::unix::ffi::OsStrExt,
  path::Path,
  process::Command,
};

use misstep::{Result, bail};

const MAX_BYTES: usize = 16 * 1024 * 1024;

pub(super) fn archive_checkout(checkout: &Path) -> Result<Vec<u8>> {
  let root = git(checkout, &["rev-parse", "--show-toplevel"])?;
  let root = root.strip_suffix(b"\n").unwrap_or(&root);
  let root = Path::new(OsStr::from_bytes(root));
  let files = git(root, &[
    "ls-files",
    "-z",
    "--cached",
    "--others",
    "--exclude-standard",
  ])?;
  let paths = files
    .split(|&byte| byte == 0)
    .filter(|path| !path.is_empty())
    .collect::<BTreeSet<_>>();
  let mut archive = tar::Builder::new(Vec::new());
  archive.follow_symlinks(false);
  for path in paths {
    let relative = Path::new(OsStr::from_bytes(path));
    let source = root.join(relative);
    let metadata = match fs::symlink_metadata(&source) {
      Ok(metadata) => metadata,
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
      Err(error) => return Err(error.into()),
    };
    if !(metadata.is_file() || metadata.is_symlink()) {
      bail!("unsupported snapshot entry: {}", relative.display());
    }
    if metadata.len() > (MAX_BYTES - archive.get_ref().len()) as u64 {
      bail!("working-tree snapshot exceeds 16 MiB");
    }
    archive.append_path_with_name(&source, relative)?;
    if archive.get_ref().len() >= MAX_BYTES {
      bail!("working-tree snapshot exceeds 16 MiB");
    }
  }
  let archive = archive.into_inner()?;
  if archive.len() > MAX_BYTES {
    bail!("working-tree snapshot exceeds 16 MiB");
  }
  Ok(archive)
}

fn git(checkout: &Path, args: &[&str]) -> Result<Vec<u8>> {
  let output = Command::new("git")
    .current_dir(checkout)
    .args(args)
    .output()?;
  if !output.status.success() {
    bail!(
      "git {} failed: {}",
      args[0],
      String::from_utf8_lossy(&output.stderr)
    );
  }
  Ok(output.stdout)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn snapshots_changes_new_files_and_deletions_without_staging() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    git(root, &["init", "--quiet"])?;
    fs::write(root.join(".gitignore"), "ignored\n")?;
    fs::write(root.join("modified"), "staged")?;
    fs::write(root.join("deleted"), "staged")?;
    git(root, &["add", "."])?;
    fs::write(root.join("modified"), "working tree")?;
    fs::remove_file(root.join("deleted"))?;
    fs::write(root.join("new"), "untracked")?;
    fs::write(root.join("ignored"), "ignored")?;
    let index_before = git(root, &["ls-files", "--stage"])?;

    let archive = archive_checkout(root)?;
    let mut files = std::collections::BTreeMap::new();
    for entry in tar::Archive::new(archive.as_slice()).entries()? {
      use std::io::Read;
      let mut entry = entry?;
      let path = entry.path()?.into_owned();
      let mut text = String::new();
      entry.read_to_string(&mut text)?;
      files.insert(path, text);
    }
    assert_eq!(
      files.get(Path::new("modified")).map(String::as_str),
      Some("working tree")
    );
    assert_eq!(
      files.get(Path::new("new")).map(String::as_str),
      Some("untracked")
    );
    assert!(!files.contains_key(Path::new("ignored")));
    assert!(!files.contains_key(Path::new("deleted")));
    assert_eq!(git(root, &["ls-files", "--stage"])?, index_before);
    Ok(())
  }
}
