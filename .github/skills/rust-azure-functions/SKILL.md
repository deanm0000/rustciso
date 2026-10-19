---
name: rust-azure-functions
description: "Scaffold and wire Rust Azure Functions custom handlers with axum, including trigger route mapping, Azure payload envelope structs, per-trigger function.json patterns, output contracts, and common gotchas like nested/double-encoded JSON."
---

# Rust Azure Functions (Custom Handler + axum)

Use this skill when creating a **new Rust Azure Functions app** and you need the real custom-handler mechanics, not business logic.

This skill focuses on:

- axum route layout that matches Azure Function folders
- input/output envelope structs Azure sends/expects
- trigger-specific binding patterns
- continuation/out binding patterns
- serialization gotchas (especially double-encoded JSON)

## 1) Blank-slate scaffold

```bash
func init . --worker-runtime custom
cargo init --bin --name <app_name> .
```

Create function folders at repo root:

```bash
mkdir -p queueUrl updateDoc
```

Add/confirm host config in `host.json`:

```json
{
  "version": "2.0",
  "customHandler": {
    "description": {
      "defaultExecutablePath": "target/x86_64-unknown-linux-musl/release/<app_name>",
      "workingDirectory": "",
      "arguments": []
    },
    "enableForwardingHttpRequest": false
  }
}
```

Create local settings:

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "custom"
  }
}
```

## 2) Route construction rule (critical)

For custom handlers, Azure forwards invocations to path:

`/<FunctionFolderName>`

So each folder with `function.json` must have a matching axum route:

```rust
use axum::{routing::post, Router};
use std::{env, sync::Arc};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let state = Arc::new(AppState::new().await?);
    let app = Router::new()
        .route("/queueUrl", post(queue_url_handler))
        .route("/updateDoc", post(update_doc_handler))
        .with_state(state);

    let port = env::var("FUNCTIONS_CUSTOMHANDLER_PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse::<u16>()
        .unwrap_or(3000);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
```

Do not route to `/api/...` inside the Rust app.  
`/api/*` is the Functions host surface; forwarded custom-handler paths are function-folder names.

## 3) Azure invocation envelope boilerplate

Use explicit Azure envelope structs (PascalCase fields, non-snake-case allowance):

```rust
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[allow(non_snake_case)]
#[derive(Debug, Deserialize, Serialize)]
pub struct Sys {
    pub MethodName: String,
    pub UtcNow: String,
    pub RandGuid: String,
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize, Serialize)]
pub struct MetaData {
    pub DequeueCount: Option<String>,
    pub ExpirationTime: Option<String>,
    pub Id: Option<String>,
    pub InsertionTime: Option<String>,
    pub NextVisibleTime: Option<String>,
    pub PopReceipt: Option<String>,
    pub sys: Sys,
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
pub struct FuncRequest {
    pub Data: HashMap<String, Value>,
    pub Metadata: MetaData,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize)]
pub struct OutResponse {
    pub Outputs: Option<Value>,
    pub Logs: Option<Vec<String>>,
    pub ReturnValue: Option<String>,
}
```

Handler signature pattern:

```rust
use axum::{extract::{State, rejection::JsonRejection}, Json, response::IntoResponse};
use std::sync::Arc;

pub async fn queue_url_handler(
    State(state): State<Arc<AppState>>,
    result: Result<Json<FuncRequest>, JsonRejection>,
) -> Result<impl IntoResponse, IngestError> {
    let func_req = result.map_err(|e| IngestError::bad_request(format!("{e:?}"), state.get_logs()))?;
    // parse payload, execute, and return OutResponse
    # todo!()
}
```

## 4) Trigger folder + function.json patterns

### Queue trigger with queue output continuation

`queueUrl/function.json`:

```json
{
  "bindings": [
    {
      "name": "embedurl",
      "type": "queueTrigger",
      "direction": "in",
      "queueName": "embedurl",
      "connection": "AzureWebJobsStorage"
    },
    {
      "name": "continuation",
      "type": "queue",
      "direction": "out",
      "queueName": "updatedoc",
      "connection": "AzureWebJobsStorage"
    }
  ]
}
```

### Queue trigger for continuation processing

`updateDoc/function.json`:

```json
{
  "bindings": [
    {
      "name": "updatedoc",
      "type": "queueTrigger",
      "direction": "in",
      "queueName": "updatedoc",
      "connection": "AzureWebJobsStorage"
    },
    {
      "name": "continuation",
      "type": "queue",
      "direction": "out",
      "queueName": "updatedoc",
      "connection": "AzureWebJobsStorage"
    }
  ]
}
```

## 5) Payload parsing patterns and gotchas

### Gotcha: nested/double-encoded JSON

Some callers enqueue JSON as a string, and sometimes stringified multiple times.  
Normalize before field extraction:

```rust
fn normalize_payload(value: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let mut normalized = value;
    for _ in 0..5 {
        match normalized {
            serde_json::Value::String(s) => {
                normalized = serde_json::from_str::<serde_json::Value>(&s)?;
            }
            _ => break,
        }
    }
    Ok(normalized)
}
```

### Gotcha: Data map key is not stable

Do not hardcode `Data["req"]` / `Data["embedurl"]`.  
Take first payload entry and parse:

```rust
fn first_payload(mut value: FuncRequest) -> anyhow::Result<serde_json::Value> {
    let mut data = std::mem::take(&mut value.Data);
    let (_key, payload) = data.iter_mut().next()
        .ok_or_else(|| anyhow::anyhow!("No data found in request body"))?;
    normalize_payload(std::mem::take(payload))
}
```

### Gotcha: queue output may need JSON string payload

For queue output bindings, using a stringified object is often safest across consumers:

```rust
let outputs = Some(serde_json::json!({
  "continuation": serde_json::json!({ "doc_id": doc_id, "page_range": "3-5,7" }).to_string()
}));
```

Downstream can deserialize with `serde_json::from_str`.

## 6) Suggested trigger-specific input structs

```rust
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct IngestInput {
    pub target_url: String,
    pub task_id: i64,
    pub browser_link_id: i64,
    pub discovered_context: DiscoveredContext,
    #[serde(default)]
    pub force: bool,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct UpdateDocInput {
    pub doc_id: i64,
    pub page_range: Option<String>,
}
```

Implement `TryFrom<FuncRequest>` per trigger to centralize validation and clear bad-request errors.

## 7) Error and response contract

Return structured errors as valid Azure custom-handler responses:

- HTTP status should reflect parse/validation/server failure
- body should always be `OutResponse`
- include invocation logs in `Logs` when available

Pattern:

```rust
(
  StatusCode::BAD_REQUEST,
  Json(OutResponse { Outputs: None, Logs: logs, ReturnValue: None })
)
```

## 8) Local run + verification

Build and run:

```bash
cargo build --release --target x86_64-unknown-linux-musl
func start
```

Verify each function endpoint:

- for HTTP triggers, call the function URL under the Functions host (for example `http://localhost:7071/api/<functionName>`)
- for queue/timer/blob triggers, invoke the trigger through its real source (enqueue message, timer fire, blob create), then confirm handler logs/output bindings

For queue triggers, verify:

1. trigger message is consumed
2. expected `Outputs` queue message is emitted
3. continuation message deserializes correctly (if stringified JSON strategy is used)

## 9) Checklist before handing off

- `host.json` has `customHandler.description.defaultExecutablePath`
- every `<function>/function.json` has a matching `Router::route("/<function>", ...)`
- envelope structs compile and deserialize with non-snake-case Azure fields
- payload normalization handles nested JSON string payloads
- output `OutResponse` shape is consistent across success/failure
- local `func start` passes and at least one end-to-end trigger path is validated

## 10) Optional debug technique: payload discovery fallback

When adding a new trigger type, temporarily attach a fallback route to inspect unmatched requests and discover Azure’s actual forwarded payload.

Use this only in local/dev debugging and remove or disable it afterward.

```rust
use axum::{
    body::Bytes,
    extract::OriginalUri,
    http::StatusCode,
    response::IntoResponse,
};

pub async fn debug_not_found(
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> impl IntoResponse {
    if !cfg!(debug_assertions) {
        return (StatusCode::NOT_FOUND, "Not Found".to_string());
    }

    eprintln!("404 - Route not found: {}", uri);

    let raw = String::from_utf8_lossy(&body);
    let preview: String = raw.chars().take(2000).collect();
    eprintln!("Body preview (truncated): {}", preview);

    match serde_json::from_slice::<serde_json::Value>(&body) {
        Ok(value) => eprintln!("Parsed JSON payload: {}", value),
        Err(err) => eprintln!("Body is not valid JSON: {}", err),
    }

    (StatusCode::NOT_FOUND, format!("404 Not Found: {}", uri))
}
```

Router wiring example:

```rust
let app = Router::new()
    .route("/queueUrl", post(queue_url_handler))
    .route("/updateDoc", post(update_doc_handler))
    .fallback(debug_not_found)
    .with_state(state);
```

Notes:

- Parse as `serde_json::Value` first; then design strict structs from observed shape.
- Prefer truncating logged body to avoid noisy/huge logs.
- Never rely on fallback for normal execution; handlers should return standard `OutResponse`.
- Avoid logging sensitive payload fields outside local development.
