//! End-to-end integration tests: a real in-process server over loopback TCP
//! driven by the same code paths the CLI uses (no SSH host needed).
//!
//! Scenario matrix:
//! * push a directory then modified copies — bytes must match exactly every
//!   time (first push = whole-file, then delta on a mid-file insertion),
//! * `--no-delta` forces whole-file transfer of changed files,
//! * multi-link (`links: 2`) transfers complete byte-perfect,
//! * `--delete` removes extras from the destination.

use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{collections::BTreeMap, net::SocketAddr};

use rtransfer::cli::{IoChoice, SyncArgs};
use rtransfer::protocol::CopyMode;
use rtransfer::session::Summary;
use rtransfer::{client, serve};

use tokio_util::sync::CancellationToken;

fn args(links: u16, no_delta: bool, delete: bool) -> SyncArgs {
    SyncArgs {
        links,
        workers: None,
        chunk_bytes: 0,
        block_bytes: 0,
        // Fixed, well-understood config keeps the assertions deterministic;
        // tuning behavior itself is covered by unit tests.
        compression: "none".into(),
        no_delta,
        fsync: false,
        delete,
        include: Vec::new(),
        exclude: Vec::new(),
        follow: false,
        io: IoChoice::Auto,
        quiet: false,
        ssh_port: 22,
        ssh_key: None,
        password: None,
        accept_host_key: false,
        serve_binary: None,
        stage_dir: "/tmp".into(),
    }
}

/// Spawn an in-process serve loop on an OS-assigned free port (grabbed and
/// released here, then handed to serve).
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

struct Server {
    port: u16,
    cancel: CancellationToken,
}

async fn spawn_server(root: &Path, port: u16) -> Server {
    let cancel = CancellationToken::new();
    let opts = serve::ServeOptions {
        root: root.to_path_buf(),
        bind: Some(std::net::IpAddr::from([127, 0, 0, 1])),
        port: Some(port),
        stdio: false,
    };
    let srv_cancel = cancel.clone();
    tokio::spawn(async move {
        let _ = serve::serve(&opts, srv_cancel).await;
    });
    // The listener is only up once serve() is polled; give it a beat.
    tokio::time::sleep(Duration::from_millis(300)).await;
    Server { port, cancel }
}

async fn push(src_root: &Path, base_name: &str, port: u16, a: &SyncArgs) -> Summary {
    let srcs = vec![src_root.join(base_name).to_string_lossy().into_owned()];
    client::run_tcp(
        a,
        CopyMode::Push,
        "127.0.0.1",
        port,
        base_name,
        &srcs,
        None,
        &CancellationToken::new(),
    )
    .await
    .expect("push session")
}

fn write_tree(base: &Path, files: &[(&str, &[u8])]) -> PathBuf {
    for (rel, data) in files {
        let p = base.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, data).unwrap();
    }
    base.to_path_buf()
}

fn read_tree(base: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();
    for e in walkdir::WalkDir::new(base).into_iter().flatten() {
        if e.file_type().is_file() {
            let rel = e
                .path()
                .strip_prefix(base)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            out.insert(rel, std::fs::read(e.path()).unwrap());
        }
    }
    out
}

fn assert_trees_eq(a: &Path, b: &Path) {
    let (ta, tb) = (read_tree(a), read_tree(b));
    let diff: Vec<String> = ta
        .iter()
        .filter_map(|(k, v)| match tb.get(k) {
            Some(v2) if v2 == v => None,
            _ => Some(k.clone()),
        })
        .collect();
    let mut keys: Vec<_> = ta.keys().chain(tb.keys()).collect();
    keys.sort();
    keys.dedup();
    for k in keys {
        assert_eq!(ta.get(k), tb.get(k), "tree mismatch at {k} (diff: {diff:?})");
    }
}

fn port_ready(port: u16) -> bool {
    std::net::TcpStream::connect(SocketAddr::from(([127, 0, 0, 1], port))).is_ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_flow_delta_no_delta_links_delete() {
    // Session logs go to stderr when RUST_LOG is set.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .with_target(true)
        .try_init();

    let tmp = std::env::temp_dir().join(format!("rt-int-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let server_root = tmp.join("srv");
    std::fs::create_dir_all(&server_root).unwrap();

    let srv = spawn_server(&server_root, free_port()).await;
    assert!(port_ready(srv.port), "server never came up");

    // ── first push: new destination → whole-file transfer ──────────────
    let src_root = tmp.join("src");
    let src = write_tree(
        &src_root.join("proj"),
        &[
            ("docs/readme.txt", b"hello world 0123456789".as_slice()),
            ("data/blob.bin", &[7u8; 300_000]),
        ],
    );
    let recv = server_root.join("proj");
    let normal = args(1, false, false);

    let s1 = push(&src_root, "proj", srv.port, &normal).await;
    assert_eq!(s1.files_failed, 0);
    assert_trees_eq(&recv, &src);

    // ── second push: middle insertion → delta transfer path ────────────
    let mut blob = vec![7u8; 300_000];
    blob.splice(150_000..150_000, [1u8; 8]);
    std::fs::write(src.join("data/blob.bin"), &blob).unwrap();

    let s2 = push(&src_root, "proj", srv.port, &normal).await;
    assert_eq!(s2.files_failed, 0);
    assert_trees_eq(&recv, &src);

    // ── --no-delta forces whole-file transfer ──────────────────────────
    blob.splice(150_000..150_008, [9u8; 8]);
    std::fs::write(src.join("data/blob.bin"), &blob).unwrap();
    let nodelta = args(1, true, false);
    let s3 = push(&src_root, "proj", srv.port, &nodelta).await;
    assert_eq!(s3.files_failed, 0);
    assert_trees_eq(&recv, &src);

    // ── two parallel links ─────────────────────────────────────────────
    blob[100_000..100_008].copy_from_slice(b"swapme!!");
    std::fs::write(src.join("data/blob.bin"), &blob).unwrap();
    let twolinks = args(2, false, false);
    let s4 = push(&src_root, "proj", srv.port, &twolinks).await;
    assert_eq!(s4.files_failed, 0);
    assert_trees_eq(&recv, &src);

    // ── --delete removes destination extras ────────────────────────────
    std::fs::create_dir_all(recv.join("extra/deep")).unwrap();
    std::fs::write(recv.join("extra/deep/junk"), b"bye").unwrap();
    let delety = args(1, false, true);
    let s5 = push(&src_root, "proj", srv.port, &delety).await;
    assert_eq!(s5.files_failed, 0);
    assert!(!recv.join("extra").exists(), "extras must be deleted");
    assert_trees_eq(&recv, &src);

    srv.cancel.cancel();
    let _ = std::fs::remove_dir_all(&tmp);
}

/// Regression for two control-dispatcher deadlocks: results drained only
/// after the plan loop (>256 finished files ahead of the Verify follow-up for
/// `z/file`), and a bounded plan queue (>1024 plans queued ahead of the
/// results that free the sender's worker slots).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_new_files_then_verify_does_not_deadlock() {
    let tmp = std::env::temp_dir().join(format!("rt-int-dl-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let server_root = tmp.join("srv");
    std::fs::create_dir_all(&server_root).unwrap();
    let srv = spawn_server(&server_root, free_port()).await;

    let src_root = tmp.join("src");
    let src = write_tree(&src_root.join("proj"), &[("z/file", [1u8; 4096].as_slice())]);
    let a = args(2, false, false);
    assert_eq!(push(&src_root, "proj", srv.port, &a).await.files_failed, 0);

    for i in 0..1500 {
        write_tree(&src, &[(&format!("a/{i:04}"), format!("file {i}").as_bytes())]);
    }
    // Same size, different content and mtime → Verify, then a follow-up plan.
    let z = src.join("z/file");
    std::fs::write(&z, [2u8; 4096]).unwrap();
    filetime::set_file_mtime(&z, filetime::FileTime::from_unix_time(1_600_000_000, 0)).unwrap();

    let s = tokio::time::timeout(Duration::from_secs(60), push(&src_root, "proj", srv.port, &a))
        .await
        .expect("second push deadlocked");
    assert_eq!(s.files_failed, 0);
    assert_trees_eq(&server_root.join("proj"), &src);

    srv.cancel.cancel();
    let _ = std::fs::remove_dir_all(&tmp);
}

/// Regression: a source that cannot be opened used to leave the receiver
/// waiting for frames that never came (10-minute hang); now it fails alone.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unreadable_source_fails_one_file_fast() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = std::env::temp_dir().join(format!("rt-int-ur-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let server_root = tmp.join("srv");
    std::fs::create_dir_all(&server_root).unwrap();
    let srv = spawn_server(&server_root, free_port()).await;

    let src_root = tmp.join("src");
    let src = src_root.join("proj");
    for i in 0..20 {
        write_tree(&src, &[(&format!("f{i:02}"), format!("data {i}").as_bytes())]);
    }
    let locked = src.join("f07");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::read(&locked).is_ok() {
        // Running as root: permissions are not enforced, nothing to test.
        srv.cancel.cancel();
        let _ = std::fs::remove_dir_all(&tmp);
        return;
    }

    let s = tokio::time::timeout(
        Duration::from_secs(60),
        push(&src_root, "proj", srv.port, &args(1, false, false)),
    )
    .await
    .expect("push hung on an unreadable source");
    assert_eq!(s.files_failed, 1);
    assert_eq!(s.files_done, 19);
    let got = read_tree(&server_root.join("proj"));
    assert_eq!(got.len(), 19);
    assert!(!got.contains_key("f07"));

    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o644)).unwrap();
    srv.cancel.cancel();
    let _ = std::fs::remove_dir_all(&tmp);
}

/// Regression: Verify used to hold an eval permit while waiting for the
/// sender's hash, and the sender only hashes in manifest order — permits held
/// by later files starved the one the sender waited on (hang, then abort).
/// Equal content must also adopt the source mtime so the next run quick-skips.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_content_new_mtime_verifies_without_deadlock() {
    let tmp = std::env::temp_dir().join(format!("rt-int-vf-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let server_root = tmp.join("srv");
    std::fs::create_dir_all(&server_root).unwrap();
    let srv = spawn_server(&server_root, free_port()).await;

    let src_root = tmp.join("src");
    let src = src_root.join("proj");
    for i in 0..300 {
        write_tree(&src, &[(&format!("f{i:03}"), format!("content {i}").as_bytes())]);
    }
    let a = args(2, false, false);
    assert_eq!(push(&src_root, "proj", srv.port, &a).await.files_failed, 0);

    let recv = server_root.join("proj");
    let old = filetime::FileTime::from_unix_time(1_600_000_000, 0);
    for i in 0..300 {
        filetime::set_file_mtime(recv.join(format!("f{i:03}")), old).unwrap();
    }

    let s = tokio::time::timeout(Duration::from_secs(60), push(&src_root, "proj", srv.port, &a))
        .await
        .expect("verify push deadlocked");
    assert_eq!((s.files_failed, s.files_skipped, s.files_done), (0, 300, 0));
    // Manifest mtimes carry millisecond precision.
    let mtime_ms = |p: PathBuf| {
        let t = filetime::FileTime::from_last_modification_time(&std::fs::metadata(p).unwrap());
        t.unix_seconds() * 1000 + i64::from(t.nanoseconds() / 1_000_000)
    };
    assert_eq!(mtime_ms(recv.join("f042")), mtime_ms(src.join("f042")), "verified file keeps a stale mtime");
    assert_trees_eq(&recv, &src);

    srv.cancel.cancel();
    let _ = std::fs::remove_dir_all(&tmp);
}

/// Regression: a zero-byte file alone in a directory that does not exist on
/// the destination yet failed ("create empty part file: No such file"): no
/// data frame ever ran the mkdir.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_file_in_new_directory_installs() {
    let tmp = std::env::temp_dir().join(format!("rt-int-ef-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let server_root = tmp.join("srv");
    std::fs::create_dir_all(&server_root).unwrap();
    let srv = spawn_server(&server_root, free_port()).await;

    let src_root = tmp.join("src");
    let src = write_tree(
        &src_root.join("proj"),
        &[("a.txt", b"hello".as_slice()), ("only/empty/dir/invoked.timestamp", b"".as_slice())],
    );
    let s = push(&src_root, "proj", srv.port, &args(1, false, false)).await;
    assert_eq!(s.files_failed, 0);
    assert_trees_eq(&server_root.join("proj"), &src);

    srv.cancel.cancel();
    let _ = std::fs::remove_dir_all(&tmp);
}
