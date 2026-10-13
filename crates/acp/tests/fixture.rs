//! Deterministic ACP peer used only by process integration tests.
use serde_json::{Value, json};
use std::io::{self, BufRead, Write};
fn send(value: Value) {
    let mut out = io::stdout().lock();
    writeln!(out, "{value}").unwrap();
    out.flush().unwrap();
}
fn result(id: &Value, value: Value) {
    send(json!({"jsonrpc":"2.0","id":id,"result":value}));
}
fn update(text: &str) {
    send(
        json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"native-test","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":text}}}}),
    );
}
fn main() {
    let mut pending = Value::Null;
    for line in io::stdin().lock().lines() {
        let request: Value = serde_json::from_str(&line.unwrap()).unwrap();
        let id = &request["id"];
        match request["method"].as_str() {
            Some("initialize") => result(
                id,
                json!({"protocolVersion":1,"agentCapabilities":{"loadSession":true},"agentInfo":{"name":"fixture","version":"1"},"authMethods":[]}),
            ),
            Some("session/new") => result(id, json!({"sessionId":"native-test"})),
            Some("session/load") => result(id, json!({})),
            Some("session/prompt") => {
                let text = request["params"]["prompt"][0]["text"]
                    .as_str()
                    .unwrap_or_default();
                if text == "permission" {
                    pending = id.clone();
                    send(
                        json!({"jsonrpc":"2.0","id":"permission-1","method":"session/request_permission","params":{"sessionId":"native-test","toolCall":{"toolCallId":"write-1","title":"Write file","kind":"edit","status":"pending"},"options":[{"optionId":"allow","name":"Allow once","kind":"allow_once"},{"optionId":"deny","name":"Reject","kind":"reject_once"}]}}),
                    );
                } else if text == "wait" {
                    pending = id.clone();
                    update("waiting");
                } else {
                    update(text);
                    update(" second chunk");
                    result(id, json!({"stopReason":"end_turn"}));
                }
            }
            Some("session/cancel") => {
                result(&pending, json!({"stopReason":"cancelled"}));
                pending = Value::Null;
            }
            Some("session/set_mode") | Some("session/set_config_option") | Some("authenticate") => {
                result(id, json!({"configOptions":[]}))
            }
            None if request["id"] == "permission-1" => {
                update(
                    request["result"]["outcome"]["optionId"]
                        .as_str()
                        .unwrap_or("cancelled"),
                );
                result(&pending, json!({"stopReason":"end_turn"}));
                pending = Value::Null;
            }
            _ => send(
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"Unknown method"}}),
            ),
        }
    }
}
