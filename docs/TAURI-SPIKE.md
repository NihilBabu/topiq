# Tauri + rust-rdkafka spike

A **throwaway** proof-of-concept that de-risks the Electron → Tauri migration
*before* committing the ~5–7 week port. It is not production code and is not wired
into the React app. See the approved plan in
`.claude/plans/what-is-the-work-tender-meerkat.md`.

## What it proves (and what it can't)

The migration's two real risks (from the plan):

1. **librdkafka builds/links across all 3 OSes.** rust-rdkafka wraps a C library;
   the `bundled` build vendors librdkafka **and** OpenSSL from source via cmake.
   The `.github/workflows/tauri-spike.yml` matrix compiles it on macOS, Windows and
   Linux. **Green matrix = risk retired.** The known weak spot is the Windows
   vendored-OpenSSL build (needs `nasm` + `perl`); if it fails, fall back to a
   system librdkafka/OpenSSL.
2. **Linux WebKitGTK renders the real UI acceptably.** This spike's HTML harness is
   trivial, so it does *not* settle risk #2 — that still needs the real React UI
   loaded on a Linux/WebKitGTK box. Out of scope here; called out so it isn't
   forgotten.

What this box **cannot** produce (needs CI or other OSes or a running app):
installer size, idle-RAM number, the 3-OS green matrix, and the Linux render
verdict. The PR's CI run is the vehicle for the first and third.

## Scope (deliberately tiny)

Only three commands, faithful to `electron/services/kafka.service.ts`:
`connect`, `get_topics`, `get_messages`. **No** persistence/encrypted store, **no**
updater, **no** consumer-groups/produce/admin. The connection is passed inline from
the UI (the spike has no store yet).

| File | Role |
| --- | --- |
| `src-tauri/src/util.rs` | Pure helpers (truncate, error-sanitize, security-protocol, offset math) **+ unit tests** |
| `src-tauri/src/kafka.rs` | The 3 commands on rust-rdkafka |
| `src-tauri/src/lib.rs` / `main.rs` | Tauri builder + entry |
| `spike-ui/index.html` | Manual test harness (calls `invoke`) |
| `.github/workflows/tauri-spike.yml` | 3-OS compile + unit-test matrix |

## Run it locally (macOS)

Fast path uses the system librdkafka (`brew install librdkafka cmake pkg-config`):

```bash
cd src-tauri
# Unit tests — pure logic, no broker needed:
cargo test --no-default-features --features system --lib
# Compile-check the whole crate against system librdkafka:
cargo check --no-default-features --features system
```

Run the desktop harness against a broker. The repo already ships a Kafka compose
under `docker/`; start it (Docker or `podman machine start` first), then:

```bash
cargo install tauri-cli --version "^2"          # one-time
cd src-tauri && cargo tauri dev --no-default-features --features system
```

In the window: set brokers (e.g. `localhost:9092`), **Connect**, **List topics**,
pick a topic, **Fetch messages**.

> CI and any shippable build use the default `bundled` feature (vendored
> librdkafka+OpenSSL) — a self-contained binary. `bundled` also builds locally as
> long as `cmake` + `nasm` are installed; it's just slower (compiles OpenSSL).

## How the React app would consume these commands (full migration)

The full migration replaces `electron/preload.ts`'s `window.api` with a thin shim
so components keep calling the same shape:

```ts
// src/lib/api.ts  (illustrative — NOT added here to avoid touching the Electron build)
import { invoke } from '@tauri-apps/api/core'
import type { KafkaConnection, MessageOptions, MessageFetchResult } from '../../shared/types'

export const kafka = {
  // NOTE divergence: Electron's connect(connectionId) looks the connection up in the
  // encrypted store; the Tauri command takes the full object (store is a later phase).
  connect: (connection: KafkaConnection) => invoke<void>('connect', { connection }),
  getTopics: (connectionId: string) => invoke<string[]>('get_topics', { connectionId }),
  getMessages: (connectionId: string, topic: string, options?: MessageOptions) =>
    invoke<MessageFetchResult>('get_messages', { connectionId, topic, options }),
}
```

> **Parity note for the full migration:** Electron's IPC layer (`validateMessageOptions`)
> *rejected* a bad `limit`, non-integer/negative `partition`, or non-numeric `fromOffset`
> with an error. The Rust port clamps `limit` and rejects a non-digit `fromOffset` but is
> otherwise lenient; restore explicit `Err` returns in the real commands to keep the
> validation contract the React components rely on.

## Decision gate

Pass the CI matrix + a local broker smoke test → commit to the full port. If
Windows can't be made to build librdkafka reasonably, that's a real finding —
better learned now, in one PR, than mid-migration.
