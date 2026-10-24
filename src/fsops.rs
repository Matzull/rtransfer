//! Cross-platform filesystem operations: directory scanning,
//! include/exclude matching, manifest building, and durable file placement
//! (temp file + fsync + atomic rename), sized for Windows and Unix alike.

use std::io;
use std::path::{Path, PathBuf};

use anyhow::Context;
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use walkdir::WalkDir;

use crate::ioengine::IoCtx;
use crate::protocol::FileEntry;

/// String representation of a relative path using only `/`.
pub fn rel_string(rel: &Path) -> String {
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// Local `Path` for a unix-style relative path (Windows accepts `/` too).
pub fn join_rel(root: &Path, rel: &str) -> PathBuf {
    root.join(Path::new(rel).components().collect::<PathBuf>())
}

fn compile_set(patterns: &[String]) -> anyhow::Result<Option<GlobSet>> {
    if patterns.is_empty() {
        return Ok(None);
    }
    let mut b = GlobSetBuilder::new();
    for p in patterns {
        b.add(
            GlobBuilder::new(p)
                .literal_separator(true)
                .case_insensitive(cfg!(target_os = "windows"))
                .build()
                .with_context(|| format!("bad pattern {p:?}"))?,
        );
    }
    Ok(Some(b.build()?))
}

/// Include-first, exclude-second; both match the `/`-separated relative path.
#[derive(Debug, Default, Clone)]
pub struct Filters {
    include: Option<GlobSet>,
    exclude: Option<GlobSet>,
}

impl Filters {
    pub fn new(include: &[String], exclude: &[String]) -> anyhow::Result<Self> {
        Ok(Filters {
            include: compile_set(include)?,
            exclude: compile_set(exclude)?,
        })
    }

    pub fn keep(&self, rel: &str) -> bool {
        // Matches are unanchored like rsync: `*.cache` hits the basename at
        // any depth, `sub/**` hits the subtree of a source-relative `sub`
        // even though the rel string carries the source display name.
        let any_suffix = |gs: &GlobSet| -> bool {
            let parts: Vec<&str> = rel.split('/').collect();
            gs.is_match(rel) || (0..parts.len()).any(|i| gs.is_match(parts[i..].join("/")))
        };
        if let Some(inc) = &self.include {
            if !any_suffix(inc) {
                return false;
            }
        }
        if let Some(exc) = &self.exclude {
            if any_suffix(exc) {
                return false;
            }
        }
        true
    }
}

/// Scanned source tree listing.
#[derive(Debug, Default)]
pub struct Scan {
    pub files: Vec<(u64, PathBuf)>, // (size, abs path), rel-path order
    pub skipped: Vec<String>,
}

/// Recursively list all regular files under `src`. `name` prefixes the
/// relative path of every entry ("DEST name" on the receiving side).
pub fn scan_into(src: &Path, name: &str, follow: bool, filters: &Filters) -> anyhow::Result<Scan> {
    let mut scan = Scan::default();
    for entry in WalkDir::new(src).follow_links(follow) {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                scan.skipped.push(match e.path() {
                    Some(p) => format!("{}: {e}", p.display()),
                    None => format!("{e}"),
                });
                continue;
            }
        };
        if !entry.file_type().is_file() {
            // Directories are implicit in the manifest; symlinks/fifos/...
            // are skipped with a warning unless followed.
            if entry.file_type().is_symlink() || !follow {
                scan.skipped.push(rel_string(entry.path()));
            }
            continue;
        }
        let rel = {
            let stripped = match entry.path().strip_prefix(src) {
                Ok(r) => r,
                Err(_) => continue,
            };
            let base = rel_string(stripped);
            match (stripped.as_os_str().is_empty(), name.is_empty()) {
                (true, _) => name.to_string(),
                (false, true) => base,
                (false, false) => format!("{name}/{base}"),
            }
        };
        if !filters.keep(&rel) {
            continue;
        }
        let md = match entry.metadata() {
            Ok(md) => md,
            Err(e) => {
                scan.skipped.push(format!("{rel}: {e}"));
                continue;
            }
        };
        let size = md.len();
        scan.files.push((size, entry.path().to_path_buf()));
    }
    Ok(scan)
}

/// Build the whole session manifest out of `sources` (display-name, path
/// pairs). File sources take their basename; dir sources contribute their
/// contents. Names must not collide (fatal, they would race on the wire).
/// Returns the entries in manifest order (ids assigned by position) together
/// with each entry's local absolute path, plus non-fatal warnings.
pub fn build_manifest(
    sources: &[(String, PathBuf)],
    filters: &Filters,
    follow: bool,
) -> anyhow::Result<(Vec<(FileEntry, PathBuf)>, Vec<String>)> {
    let mut out: Vec<(String, PathBuf, u64, Option<i64>)> = Vec::new();
    let mut skipped: Vec<String> = Vec::new();
    /// Wire-side rel prefix for a source: empty string = tree-merge mode
    /// (contents land directly in the target directory, like `rclone copy`).
    fn rel_prefix(name: &str, path: &Path, merge_single: bool) -> String {
        if merge_single && std::fs::metadata(path).map(|m| m.is_dir()).unwrap_or(false) {
            String::new()
        } else {
            name.to_string()
        }
    }
    let merge_single = sources.len() == 1;
    for (name, path) in sources {
        let prefix = rel_prefix(name, path, merge_single);
        let scan = scan_into(path, &prefix, follow, filters)?;
        for (size, p) in scan.files {
            // Empty `prefix` = tree-merge mode for this source: rels stay
            // source-relative so the tree fills the target directory.
            // Otherwise (named-forest planting) disk-layout rels keep the
            // display-name prefix, mirroring scan_into's matching rule.
            let stripped = p
                .strip_prefix(path)
                .with_context(|| format!("path {} escaped root {}", p.display(), path.display()))?;
            let rel = if stripped.as_os_str().is_empty() {
                name.clone()
            } else if prefix.is_empty() {
                rel_string(stripped)
            } else {
                format!("{prefix}/{}", rel_string(stripped))
            };
            let mtime = file_mtime_millis(&p);
            out.push((rel, p, size, mtime));
        }
        skipped.extend(scan.skipped);
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    let mut entries = Vec::with_capacity(out.len());
    for (i, (rel, p, size, mtime)) in out.into_iter().enumerate() {
        entries.push((FileEntry { id: i as u32, rel, size, mtime: mtime }, p));
    }
    {
        let mut seen = std::collections::HashSet::new();
        for (e, _) in &entries {
            if !seen.insert(e.rel.clone()) {
                anyhow::bail!("conflicting file merged from sources: {}", e.rel);
            }
        }
    }
    Ok((entries, skipped))
}

/// `Some(unix millis)` of the modification time when representable.
/// Milliseconds avoid 1-second truncation errors in the size+mtime quick
/// check (rsync's classic false-skip when several pushes share a second).
pub fn file_mtime_millis(path: &Path) -> Option<i64> {
    let millis =
        std::fs::metadata(path).ok()?.modified().ok()?.duration_since(std::time::UNIX_EPOCH).ok()?.as_millis();
    i64::try_from(millis).ok()
}

/// Create all parent directories of `path`.
pub async fn mkdirs_parent(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    Ok(())
}

/// Name of the in-progress sidecar file for `dest`.
pub fn part_file(dest: &Path) -> PathBuf {
    match dest.file_name() {
        Some(n) => dest.with_file_name(format!(".{}.{}.rpart", n.to_string_lossy(), std::process::id())),
        None => dest.with_extension("rpart"),
    }
}

/// Atomically install `part` as `dest` and restore the source mtime, then
/// fsync the containing directory is left to the caller (best effort there).
/// `mtime` is unix milliseconds (the manifest convention).
pub async fn install_file(part: &Path, dest: &Path, mtime: Option<i64>) -> io::Result<()> {
    // rename over an existing file is refused on Windows: remove first.
    if tokio::fs::try_exists(dest).await.unwrap_or(false) {
        matches!(tokio::fs::remove_file(dest).await, Ok(()) | Err(_));
    }
    tokio::fs::rename(part, dest).await?;
    if let Some(millis) = mtime {
        set_mtime_millis(dest, millis);
    }
    Ok(())
}

/// Best-effort: set `path`'s mtime to `millis` (unix milliseconds).
pub fn set_mtime_millis(path: &Path, millis: i64) {
    let t = std::time::UNIX_EPOCH + std::time::Duration::from_millis(millis.unsigned_abs());
    if let Err(e) = filetime::set_file_mtime(path, filetime::FileTime::from_system_time(t)) {
        tracing::debug!("set mtime of {}: {e}", path.display());
    }
}

/// Remove a leftover partial file.
pub async fn cleanup_part(part: &Path) {
    let _ = tokio::fs::remove_file(part).await;
}

/// Delete `rels` under `root` (files only, best effort); return removed ones.
pub async fn delete_extras(root: &Path, rels: &[String]) -> Vec<String> {
    let mut done = Vec::new();
    for rel in rels {
        let p = join_rel(root, rel);
        if tokio::fs::remove_file(&p).await.is_ok() {
            done.push(rel.clone());
        }
    }
    done
}

/// Remove directories left empty (typically after `--delete`), bottom-up.
/// Never removes `dir` itself — only descendants, and only when truly empty.
pub async fn prune_empty_dirs(dir: &Path) {
    // Collect first, walking deepest-last; removing after reverting order
    // lets parents see themselves as empty in a single pass.
    let mut dirs: Vec<(usize, PathBuf)> = WalkDir::new(dir)
        .follow_links(false)
        .into_iter()
        .filter_map(|x| x.ok())
        .filter(|x| x.file_type().is_dir() && x.path() != dir)
        .map(|x| (x.depth(), x.path().to_path_buf()))
        .collect();
    dirs.sort_by(|a, b| b.0.cmp(&a.0)); // deepest first; siblings are disjoint
    for (_, d) in dirs {
        let _ = tokio::fs::remove_dir(&d).await; // fails while non-empty: fine
    }
}

/// Blake3 hash of a whole file, read through the [`IoCtx`] in `chunk` steps.
pub async fn hash_file(ctx: &IoCtx, path: &Path, chunk: u64) -> io::Result<[u8; 32]> {
    let md = tokio::fs::metadata(path).await?;
    let rdr = crate::ioengine::Reader::open(ctx, path).await?;
    let mut h = blake3::Hasher::new();
    let mut off = 0u64;
    while off < md.len() {
        let len = chunk.min(md.len() - off);
        let buf = rdr.read_at(off, len).await?;
        if buf.is_empty() {
            break;
        }
        h.update(&buf);
        off += buf.len() as u64;
    }
    Ok(*h.finalize().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::fill_random;
    use crate::ioengine::IoMode;

    fn tmpdir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("rtransfer-test-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn scan_and_glob_filtering() {
        let root = tmpdir("glob");
        std::fs::create_dir_all(root.join("sub/deep")).unwrap();
        std::fs::write(root.join("a.txt"), b"x").unwrap();
        std::fs::write(root.join("sub/b.rs"), b"y").unwrap();
        std::fs::write(root.join("sub/deep/c.cache"), [0u8; 8]).unwrap();

        let excl = Filters::new(&[], &["*.cache".into()]).unwrap();
        let scan = scan_into(&root, "name", false, &excl).unwrap();
        let rels: Vec<String> = scan
            .files
            .iter()
            .map(|(_, p)| {
                format!("name/{}", rel_string(p.strip_prefix(&root).unwrap()))
            })
            .collect();
        assert!(rels.contains(&"name/a.txt".to_string()));
        assert!(rels.contains(&"name/sub/b.rs".to_string()));
        assert!(!rels.iter().any(|r| r.ends_with(".cache")));

        let inc = Filters::new(&["sub/**".into()], &[]).unwrap();
        let scan = scan_into(&root, "n", false, &inc).unwrap();
        assert_eq!(scan.files.len(), 2);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn manifest_from_multiple_sources_and_collision_detection() {
        let base = tmpdir("mani");
        let a = base.join("a");
        let b = base.join("b");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        std::fs::write(a.join("f1"), b"aa").unwrap();
        std::fs::write(b.join("f1"), b"cc").unwrap();
        std::fs::write(base.join("single"), b"dd").unwrap();

        let err = build_manifest(
            &[("x".into(), a.clone()), ("x".into(), b.clone())],
            &Filters::default(),
            false,
        );
        assert!(err.is_err(), "same top name in two sources must collide");

        let ok = build_manifest(&[("a".into(), a), ("b".into(), b)], &Filters::default(), false)
            .unwrap();
        assert_eq!(ok.0.len(), 2);
        assert!(ok.0.iter().any(|(e, _)| e.rel == "a/f1"));
        assert!(ok.0.iter().any(|(e, _)| e.rel == "b/f1"));
        assert_eq!(ok.0[0].0.id, 0);

        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn part_install_clobbers_and_sets_mtime() {
        let base = tmpdir("install");
        let dest = base.join("out.bin");
        let part = part_file(&dest);
        std::fs::write(&part, vec![1u8; 10]).unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(install_file(&part, &dest, None)).unwrap();
        assert!(dest.exists());
        assert!(!part.exists());

        std::fs::write(&part, vec![2u8; 5]).unwrap();
        rt.block_on(install_file(&part, &dest, Some(1_700_000_000)))
            .unwrap();
        assert_eq!(std::fs::read(&dest).unwrap().len(), 5);
        assert_eq!(file_mtime_millis(&dest), Some(1_700_000_000));
        std::fs::remove_dir_all(base).unwrap();
    }

    #[tokio::test]
    async fn hash_file_matches_direct_hash() {
        let base = tmpdir("hash");
        let p = base.join("h");
        let mut data = vec![0u8; 300 * 1024];
        fill_random(&mut data, 3);
        std::fs::write(&p, &data).unwrap();
        let ctx = IoCtx::new(IoMode::Auto, 128 * 1024);
        let h = hash_file(&ctx, &p, 128 * 1024).await.unwrap();
        assert_eq!(*blake3::hash(&data).as_bytes(), h);
        std::fs::remove_dir_all(base).unwrap();
    }
}
