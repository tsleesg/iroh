use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};

#[test]
fn test_provide() {
    let sendme = env!("CARGO_BIN_EXE_iroh");
    let provider = Command::new(sendme)
        .args(["provide", "README.md"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut provider = KillOnDrop(provider);
    let stderr = provider.0.stderr.take().unwrap();
    let stderr = BufReader::new(stderr);
    for line in stderr.lines() {
        let line = line.unwrap();
        println!("line: {line}");
    }
}

struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        self.0.kill().ok();
        self.0.try_wait().ok();
    }
}
