use std::{
    io::Write,
    process::{Command, Stdio},
};
pub async fn run(command: &str, args: Vec<String>, input: Option<String>) -> String {
    let command = command.to_owned();
    tokio::task::spawn_blocking(move || {
        let container = std::env::var("DORIS_KAFKA_CONTAINER")
            .expect("dedicated DORIS_KAFKA_CONTAINER required for actual-message contracts");
        assert!(
            !container.is_empty()
                && container
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
        );
        assert!(
            std::env::var_os("DOCKER_HOST").is_some(),
            "set the previously audited external Docker daemon explicitly"
        );
        let brokers = std::env::var("DORIS_KAFKA_BROKERS").unwrap();
        let mut child = Command::new("docker");
        child.args([
            "exec",
            "-i",
            "-e",
            "KAFKA_HEAP_OPTS=-Xms32m -Xmx128m",
            &container,
            &format!("/opt/kafka/bin/{command}.sh"),
        ]);
        if command == "kafka-cluster" {
            child.arg("cluster-id");
        }
        child.args(["--bootstrap-server", &brokers]);
        child.args(args);
        let mut child = child
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        if let Some(input) = input {
            child
                .stdin
                .take()
                .unwrap()
                .write_all(input.as_bytes())
                .unwrap();
        } else {
            drop(child.stdin.take());
        }
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "Kafka fixture tool: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    })
    .await
    .unwrap()
}
