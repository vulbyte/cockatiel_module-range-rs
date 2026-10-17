//! range — a tiny chat-command module: a random number in an explicit range.
//!
//! Registers `!range` and replies in chat:
//!   `!range`         → a number in 1..=100
//!   `!range 20`      → a number in 1..=20
//!   `!range 5 10`    → a number in 5..=10 (inclusive)
//! A reversed range (`!range 10 5`) is swapped to 5..=10. Standalone: no
//! reliance on any other module.

use futures_util::{SinkExt, StreamExt};
use prost::Message;
use rand::Rng;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tracing::{info, warn};
use tracing_subscriber::FmtSubscriber;

use cockatiel_client::proto::container_for_engine::Payload as EnginePayload;
use cockatiel_client::proto::container_for_module::Payload as ModulePayload;
use cockatiel_client::proto::*;
use cockatiel_client::CockatielClient;

type WsWriteHalf = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    WsMessage,
>;

const COMMAND_NAME: &str = "range";
const DEFAULT_FLAG: &str = "!";
const DEFAULT_RECONNECT_BASE_SECS: u32 = 1;
const DEFAULT_RECONNECT_MAX_SECS: u32 = 30;
const DEFAULT_LO: u32 = 1;
const DEFAULT_HI: u32 = 100;
const MAX_BOUND: u32 = 1_000_000_000;

#[derive(Debug, Clone)]
struct ModuleSettings {
    command_flag: String,
    reconnect_base_secs: u32,
    reconnect_max_secs: u32,
}

fn ensure_defaults() -> ModuleSettings {
    let root: Option<serde_json::Value> = std::fs::read_to_string("config.json")
        .ok()
        .and_then(|data| serde_json::from_str(&data).ok());
    let ms = root
        .as_ref()
        .and_then(|r| r.get("module_specific"))
        .and_then(|ms| ms.as_object())
        .cloned()
        .unwrap_or_default();
    let command_flag = ms
        .get("command_flag")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| DEFAULT_FLAG.to_string());
    let reconnect_base_secs = ms
        .get("reconnect_base_secs")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(DEFAULT_RECONNECT_BASE_SECS);
    let reconnect_max_secs = ms
        .get("reconnect_max_secs")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(DEFAULT_RECONNECT_MAX_SECS);
    if let Some(mut root) = root {
        if let Some(obj) = root.as_object_mut() {
            if let Some(ms) = obj
                .entry("module_specific".to_string())
                .or_insert_with(|| serde_json::json!({}))
                .as_object_mut()
            {
                ms.entry("command_flag".to_string())
                    .or_insert_with(|| serde_json::json!(command_flag));
                ms.entry("reconnect_base_secs".to_string())
                    .or_insert_with(|| serde_json::json!(reconnect_base_secs));
                ms.entry("reconnect_max_secs".to_string())
                    .or_insert_with(|| serde_json::json!(reconnect_max_secs));
            }
            let _ = std::fs::write("config.json", serde_json::to_string_pretty(&root).unwrap());
        }
    }
    ModuleSettings {
        command_flag,
        reconnect_base_secs,
        reconnect_max_secs,
    }
}

// ── pure logic ─────────────────────────────────────────────────────────────

/// Parse `[min] [max]` into an inclusive `(lo, hi)`:
///   0 args → (1, 100); 1 arg `n` → (1, n); 2+ args → (min, max) with a swap if
///   reversed. Rejects a zero/absurd bound or a non-number.
fn parse_range(arg: &str) -> Option<(u32, u32)> {
    let nums: Vec<u32> = arg
        .split_whitespace()
        .filter_map(|t| t.parse::<u32>().ok())
        .collect();
    let nonempty: Vec<&str> = arg.split_whitespace().collect();
    // Any non-numeric token is an error (don't silently ignore "!range a b").
    if nonempty.len() != nums.len() {
        return None;
    }
    let (lo, hi) = match nums.len() {
        0 => (DEFAULT_LO, DEFAULT_HI),
        1 => (DEFAULT_LO, nums[0]),
        _ => (nums[0], nums[1]),
    };
    if lo == 0 || hi == 0 || lo > MAX_BOUND || hi > MAX_BOUND {
        return None;
    }
    Some(if lo <= hi { (lo, hi) } else { (hi, lo) })
}

fn rand_between(lo: u32, hi: u32) -> u32 {
    rand::thread_rng().gen_range(lo..=hi)
}

/// The chat reply, or `None` when the range is unusable.
fn range_reply(handle: &str, arg: &str) -> Option<String> {
    let (lo, hi) = parse_range(arg)?;
    let who = if handle.is_empty() { "you" } else { handle };
    Some(format!("🎲 {who} {lo}–{hi}: {}", rand_between(lo, hi)))
}

fn strip_command(raw: &str, flag: &str, name: &str) -> String {
    let mut rest = raw.trim().to_string();
    if let Some(idx) = rest.find(flag) {
        rest = rest[idx + flag.len()..].trim_start().to_string();
    }
    if rest.starts_with(name) {
        rest = rest[name.len()..].trim_start().to_string();
    }
    rest
}

// ── connection / message loop ──────────────────────────────────────────────

#[derive(Clone)]
struct EngineIdentity {
    auth: String,
    instance: String,
    module: String,
}

async fn send_container(write_shared: &Arc<AsyncMutex<WsWriteHalf>>, container: ContainerForEngine) {
    let mut buf = Vec::new();
    if container.encode(&mut buf).is_ok() {
        let mut w = write_shared.lock().await;
        let _ = w.send(WsMessage::Binary(buf)).await;
    }
}

async fn register_commands(
    write_shared: &Arc<AsyncMutex<WsWriteHalf>>,
    identity: &Arc<AsyncMutex<EngineIdentity>>,
    command_flag: &str,
) {
    let id = identity.lock().await.clone();
    let commands = ContainerForEngine {
        version: 2,
        auth_token: id.auth,
        module_name: id.module,
        module_instance_uuid7: id.instance,
        payload: Some(EnginePayload::Commands(Commands {
            commands: vec![Command {
                command_name: COMMAND_NAME.to_string(),
                command_flag: command_flag.to_string(),
                command_description: "random number in a range — !range <min> <max>".to_string(),
                command_flags: vec![],
            }],
            alert_on_unknown_command: false,
        })),
    };
    send_container(write_shared, commands).await;
    info!("registered !{} command", COMMAND_NAME);
}

async fn send_to_chat(
    write: &Arc<AsyncMutex<WsWriteHalf>>,
    id: &EngineIdentity,
    msg: String,
    chat: &ChatMessage,
) {
    let handle = chat
        .user_data
        .as_ref()
        .map(|u| u.username.clone())
        .unwrap_or_default();
    let container = ContainerForEngine {
        version: 2,
        auth_token: id.auth.clone(),
        module_name: id.module.clone(),
        module_instance_uuid7: id.instance.clone(),
        payload: Some(EnginePayload::SendToPlatforms(SendToPlatforms {
            msg,
            level: 0,
            module_uuid7: id.instance.clone(),
            pid: String::new(),
            platform: chat.platform.clone(),
            actor_platform: chat.platform.clone(),
            actor_handle: handle,
            actor_uuid7: chat.user_uuid7.clone(),
            channel_id: chat.channel_id.clone(),
        })),
    };
    send_container(write, container).await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let client = CockatielClient::connect("config.json").await?;
    let (write, read) = client.stream.split();
    let write_shared: Arc<AsyncMutex<WsWriteHalf>> = Arc::new(AsyncMutex::new(write));
    let module_name = client.config.module_name.clone();
    info!("range module connected as '{}'", module_name);
    let identity: Arc<AsyncMutex<EngineIdentity>> = Arc::new(AsyncMutex::new(EngineIdentity {
        auth: client.auth_token.clone(),
        instance: client.instance_uuid7.clone(),
        module: module_name,
    }));

    let settings = ensure_defaults();
    let command_flag = settings.command_flag.clone();
    let reconnect_base_secs = settings.reconnect_base_secs;
    let reconnect_max_secs = settings.reconnect_max_secs;
    let write_for_task = Arc::clone(&write_shared);
    let identity_for_task = Arc::clone(&identity);

    tokio::spawn(async move {
        let mut read = read;
        loop {
            register_commands(&write_for_task, &identity_for_task, &command_flag).await;
            loop {
                let Some(msg) = read.next().await else { break };
                let data = match msg {
                    Ok(WsMessage::Binary(d)) => d,
                    Ok(WsMessage::Close(_)) => {
                        info!("Engine closed connection");
                        break;
                    }
                    Ok(_) => continue,
                    Err(e) => {
                        warn!("Engine WebSocket error: {}", e);
                        break;
                    }
                };
                let Ok(container) = ContainerForModule::decode(data.as_ref()) else { continue };
                let id = identity_for_task.lock().await.clone();
                match container.payload {
                    Some(ModulePayload::AuthVerify(_)) => {
                        let reply = ContainerForEngine {
                            version: 2,
                            auth_token: id.auth.clone(),
                            module_name: id.module.clone(),
                            module_instance_uuid7: id.instance.clone(),
                            payload: Some(EnginePayload::AuthVerify(AuthVerify {
                                cur_auth: id.auth.clone(),
                            })),
                        };
                        send_container(&write_for_task, reply).await;
                    }
                    Some(ModulePayload::MessagePreProcess(pre)) => {
                        let MessagePreProcess {
                            message_uuid7: uuid,
                            raw_message,
                            audio,
                            audio_type,
                        } = pre;
                        if !uuid.is_empty() {
                            let receipt = ContainerForEngine {
                                version: 2,
                                auth_token: id.auth.clone(),
                                module_name: id.module.clone(),
                                module_instance_uuid7: id.instance.clone(),
                                payload: Some(EnginePayload::MessageAck(MessageAck {
                                    message_uuid7: uuid.clone(),
                                })),
                            };
                            send_container(&write_for_task, receipt).await;
                        }
                        if let Some(chat) = &raw_message {
                            if let Some(cmd) = &chat.command {
                                if cmd.command_name == COMMAND_NAME {
                                    let handle = chat
                                        .user_data
                                        .as_ref()
                                        .map(|u| u.username.clone())
                                        .unwrap_or_default();
                                    let arg = strip_command(
                                        &chat.raw_message,
                                        &cmd.command_flag,
                                        &cmd.command_name,
                                    );
                                    let reply = range_reply(&handle, &arg)
                                        .unwrap_or_else(|| "usage: !range [min] [max]".to_string());
                                    send_to_chat(&write_for_task, &id, reply, chat).await;
                                }
                            }
                        }
                        let ack = ContainerForEngine {
                            version: 2,
                            auth_token: id.auth.clone(),
                            module_name: id.module.clone(),
                            module_instance_uuid7: id.instance.clone(),
                            payload: Some(EnginePayload::MessagePreProcess(MessagePreProcess {
                                message_uuid7: uuid,
                                raw_message,
                                audio,
                                audio_type,
                            })),
                        };
                        send_container(&write_for_task, ack).await;
                    }
                    Some(ModulePayload::MessageInProcess(process)) => {
                        if !process.message_uuid7.is_empty() {
                            let receipt = ContainerForEngine {
                                version: 2,
                                auth_token: id.auth.clone(),
                                module_name: id.module.clone(),
                                module_instance_uuid7: id.instance.clone(),
                                payload: Some(EnginePayload::MessageAck(MessageAck {
                                    message_uuid7: process.message_uuid7.clone(),
                                })),
                            };
                            send_container(&write_for_task, receipt).await;
                        }
                        let ack = ContainerForEngine {
                            version: 2,
                            auth_token: id.auth.clone(),
                            module_name: id.module.clone(),
                            module_instance_uuid7: id.instance.clone(),
                            payload: Some(EnginePayload::MessageInProcess(MessageInProcess {
                                message_uuid7: process.message_uuid7,
                                raw_message: process.raw_message,
                                processed_message: process.processed_message,
                                abandon_message: process.abandon_message,
                                audio: process.audio,
                                audio_type: process.audio_type,
                            })),
                        };
                        send_container(&write_for_task, ack).await;
                    }
                    Some(ModulePayload::Shutdown(_)) => {
                        info!("Engine requested shutdown — exiting");
                        std::process::exit(0);
                    }
                    _ => {}
                }
            }

            info!("Engine disconnected — reconnecting...");
            let mut backoff = reconnect_base_secs;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(backoff as u64)).await;
                match CockatielClient::connect("config.json").await {
                    Ok(conn) => {
                        info!("Reconnected to engine");
                        let (w, r) = conn.stream.split();
                        *write_for_task.lock().await = w;
                        *identity_for_task.lock().await = EngineIdentity {
                            auth: conn.auth_token,
                            instance: conn.instance_uuid7,
                            module: conn.config.module_name,
                        };
                        read = r;
                        break;
                    }
                    Err(e) => {
                        warn!("Engine reconnect failed: {} — retrying in {}s", e, backoff);
                        backoff = (backoff * 2).min(reconnect_max_secs);
                    }
                }
            }
        }
    });

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_shapes() {
        assert_eq!(parse_range(""), Some((1, 100)));
        assert_eq!(parse_range("20"), Some((1, 20)));
        assert_eq!(parse_range("5 10"), Some((5, 10)));
        assert_eq!(parse_range("1 1"), Some((1, 1)));
    }

    #[test]
    fn swaps_a_reversed_range() {
        assert_eq!(parse_range("10 5"), Some((5, 10)));
        assert_eq!(parse_range("100 1"), Some((1, 100)));
    }

    #[test]
    fn rejects_bad_input() {
        for bad in ["0", "0 5", "5 0", "abc", "5 abc", "1000000001", "1.5"] {
            assert!(parse_range(bad).is_none(), "{bad:?} should not parse");
        }
    }

    #[test]
    fn stays_in_range() {
        for _ in 0..1000 {
            let n = rand_between(3, 7);
            assert!((3..=7).contains(&n));
        }
    }

    #[test]
    fn reply_formats_and_bounds() {
        let r = range_reply("bob", "5 10").unwrap();
        assert!(r.starts_with("🎲 bob 5–10: "), "{r}");
        let n: u32 = r.rsplit(": ").next().unwrap().parse().unwrap();
        assert!((5..=10).contains(&n));
    }

    #[test]
    fn reply_none_on_bad_arg() {
        assert!(range_reply("bob", "banana").is_none());
        assert!(range_reply("bob", "0 5").is_none());
    }
}
