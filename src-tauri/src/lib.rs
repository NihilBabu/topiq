//! Tauri + rust-rdkafka spike. Proves the two hard risks of the Electron -> Tauri
//! migration: that librdkafka builds/links across macOS/Windows/Linux (this whole
//! crate compiling is the proof), and that the three core Kafka commands behave
//! like their kafkajs counterparts in `electron/services/kafka.service.ts`.

mod kafka;
mod util;

use std::collections::HashMap;
use std::sync::Mutex;

use kafka::AppState;

pub fn run() {
    tauri::Builder::default()
        .manage(AppState {
            clients: Mutex::new(HashMap::new()),
        })
        .invoke_handler(tauri::generate_handler![
            kafka::connect,
            kafka::get_topics,
            kafka::get_messages,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
