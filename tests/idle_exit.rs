use std::time::{Duration, Instant};

#[test]
fn unix_server_exits_after_idle_window() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("idle.sock");
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_bus"))
        .args(["serve", "--unix"])
        .arg(&sock)
        .args(["--idle-exit", "2s"])
        .arg("--data-dir")
        .arg(dir.path().join("data"))
        .spawn()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "server exited with {status}");
            return;
        }
        if Instant::now() > deadline {
            child.kill().ok();
            panic!("server did not exit after idle window");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}
