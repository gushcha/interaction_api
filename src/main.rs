use axum::{
    extract::{
        ws::{Message as WsMessage, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use uuid::Uuid;

const TOPIC_REQUESTS: &str = "weather.requests";
const TOPIC_RESULTS: &str = "weather.results";

#[derive(Serialize, Deserialize, Clone)]
struct Envelope {
    action: String,
    client_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<serde_json::Value>,
}

#[derive(Clone)]
struct AppState {
    producer: Arc<FutureProducer>,
    result_tx: broadcast::Sender<Envelope>,
}

async fn health() -> &'static str {
    "ok"
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    let client_id = Uuid::new_v4().to_string();
    let mut result_rx = state.result_tx.subscribe();

    if socket
        .send(WsMessage::Text(r#"{"isReady":true}"#.into()))
        .await
        .is_err()
    {
        return;
    }

    let mut heartbeat = tokio::time::interval(Duration::from_secs(60));
    heartbeat.tick().await;

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if socket.send(WsMessage::Ping(vec![].into())).await.is_err() {
                    return;
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(WsMessage::Text(text))) => {
                        let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) else {
                            continue;
                        };
                        if val.get("action").and_then(|a| a.as_str()) == Some("get_weather") {
                            let envelope = Envelope {
                                action: "get_weather".into(),
                                client_id: client_id.clone(),
                                payload: None,
                            };
                            if let Ok(json) = serde_json::to_string(&envelope) {
                                let _ = state.producer
                                    .send(
                                        FutureRecord::to(TOPIC_REQUESTS)
                                            .payload(&json)
                                            .key(&client_id),
                                        Duration::from_secs(5),
                                    )
                                    .await;
                            }
                        }
                    }
                    Some(Ok(_)) => {}
                    _ => return,
                }
            }
            Ok(envelope) = result_rx.recv() => {
                println!("[ws] broadcast received for client={}, my id={client_id}", envelope.client_id);
                if envelope.client_id == client_id {
                    if let Ok(json) = serde_json::to_string(&envelope) {
                        if socket.send(WsMessage::Text(json.into())).await.is_err() {
                            return;
                        }
                    }
                }
            }
        }
    }
}

fn start_result_consumer(state: AppState, brokers: String) {
    tokio::spawn(async move {
        println!("[kafka] creating consumer, brokers={brokers}");
        let consumer: StreamConsumer = match ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", "interaction-api")
            .set("auto.offset.reset", "latest")
            .create()
        {
            Ok(c) => c,
            Err(e) => { println!("[kafka] consumer create failed: {e}"); return; }
        };

        println!("[kafka] consumer created, subscribing to {TOPIC_RESULTS}");
        if let Err(e) = consumer.subscribe(&[TOPIC_RESULTS]) {
            println!("[kafka] subscribe failed: {e}");
            return;
        }

        println!("[kafka] subscribed to {TOPIC_RESULTS}, waiting for messages");
        let mut stream = consumer.stream();
        while let Some(result) = stream.next().await {
            match result {
                Err(e) => println!("[kafka] consumer error: {e}"),
                Ok(msg) => {
                    let Some(Ok(raw)) = msg.payload_view::<str>() else {
                        continue;
                    };
                    println!("[kafka] received: {raw}");
                    match serde_json::from_str::<Envelope>(raw) {
                        Err(e) => println!("[kafka] deserialize error: {e}"),
                        Ok(envelope) => {
                            let receivers = state.result_tx.receiver_count();
                            println!("[kafka] broadcasting to {receivers} receivers");
                            let _ = state.result_tx.send(envelope);
                        }
                    }
                }
            }
        }
    });
}

#[tokio::main]
async fn main() {
    let brokers = std::env::var("KAFKA_BROKERS")
        .unwrap_or_else(|_| "localhost:9092".into());

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("failed to create kafka producer");

    let (result_tx, _) = broadcast::channel(256);

    let state = AppState {
        producer: Arc::new(producer),
        result_tx,
    };

    start_result_consumer(state.clone(), brokers);

    let app = Router::new()
        .route("/", get(ws_handler))
        .route("/health", get(health))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Listening on http://0.0.0.0:3000");
    axum::serve(listener, app).await.unwrap();
}
