use super::render::text;
use serde_json::Value;
pub(crate) fn render(value: &Value) {
    println!(
        "Kafka {}: {} (exit {})",
        value["operation"].as_str().unwrap(),
        value["observation"].as_str().unwrap(),
        value["exit_code"]
    );
    if let Some(path) = value["checkpoint_file"].as_str() {
        println!("checkpoint {}", text(&Value::String(path.into())));
    }
    let current = &value["current"];
    if !current.is_null() {
        println!(
            "cluster {}\ntopic {}\ntopic-id {}\nisolation read_uncommitted",
            text(&current["cluster_id"]),
            text(&current["topic"]),
            text(&current["topic_id"])
        );
        for p in current["partitions"].as_array().unwrap() {
            println!(
                "partition {} epoch {} range [{},{}]",
                p["partition"], p["leader_epoch"], p["earliest"], p["end"]
            );
        }
    }
    for c in value["checks"].as_array().unwrap() {
        println!(
            "partition {} next {} range={} history={}",
            c["partition"],
            c["next_offset"],
            c["range"].as_str().unwrap(),
            c["history"].as_str().unwrap()
        );
    }
    for i in value["issues"].as_array().unwrap() {
        println!(
            "issue {} partition {}",
            i["code"].as_str().unwrap(),
            i["partition"]
        );
    }
    println!("{}", value["message"].as_str().unwrap());
}
