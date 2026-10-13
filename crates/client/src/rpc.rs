use crate::SavedSession;
use anyhow::{Context, Result, ensure};
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
enum Request {
    Call {
        id: String,
        tag: String,
        payload: Value,
        response: oneshot::Sender<Result<Value>>,
    },
    Subscribe {
        id: String,
        tag: String,
        payload: Value,
        values: mpsc::Sender<Result<Vec<Value>>>,
    },
    Ack(String),
    Interrupt(String),
}
enum Pending {
    Call(oneshot::Sender<Result<Value>>),
    Subscribe(mpsc::Sender<Result<Vec<Value>>>),
}
struct Connection {
    requests: mpsc::Sender<Request>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// One authenticated connection. Mutations are never replayed automatically.
#[derive(Clone)]
pub struct Client {
    connection: Arc<Connection>,
}
impl Client {
    pub async fn connect(session: &SavedSession) -> Result<Self> {
        let url = session.websocket_url().await?;
        let (socket, _) = tokio::time::timeout(
            Duration::from_secs(15),
            tokio_tungstenite::connect_async(url.as_str()),
        )
        .await
        .context("Environment connection timed out")?
        .context("Could not connect to the environment")?;
        let (requests, receiver) = mpsc::channel(64);
        let task = tokio::spawn(async move {
            run(socket, receiver).await;
        });
        Ok(Self {
            connection: Arc::new(Connection { requests, task }),
        })
    }
    pub async fn call(&self, tag: &str, payload: Value) -> Result<Value> {
        let id = uuid::Uuid::new_v4().to_string();
        let (response, receiver) = oneshot::channel();
        self.connection
            .requests
            .send(Request::Call {
                id: id.clone(),
                tag: tag.into(),
                payload,
                response,
            })
            .await
            .context("Environment disconnected")?;
        match tokio::time::timeout(Duration::from_secs(45), receiver).await {
            Ok(response) => response.context("Environment disconnected")?,
            Err(_) => {
                let _ = self.connection.requests.send(Request::Interrupt(id)).await;
                anyhow::bail!("Environment request timed out; its outcome may still be committed")
            }
        }
    }
    pub async fn subscribe(&self, tag: &str, payload: Value) -> Result<Subscription> {
        let id = uuid::Uuid::new_v4().to_string();
        let (values, receiver) = mpsc::channel(4);
        self.connection
            .requests
            .send(Request::Subscribe {
                id: id.clone(),
                tag: tag.into(),
                payload,
                values,
            })
            .await
            .context("Environment disconnected")?;
        Ok(Subscription {
            client: self.clone(),
            id,
            receiver,
            acknowledge: false,
        })
    }
}

/// Acknowledges a chunk only after the caller consumes it, bounding server backpressure.
pub struct Subscription {
    client: Client,
    id: String,
    receiver: mpsc::Receiver<Result<Vec<Value>>>,
    acknowledge: bool,
}
impl Subscription {
    pub async fn next(&mut self) -> Result<Vec<Value>> {
        if self.acknowledge {
            self.client
                .connection
                .requests
                .send(Request::Ack(self.id.clone()))
                .await
                .context("Environment disconnected")?;
            self.acknowledge = false;
        }
        let values = self
            .receiver
            .recv()
            .await
            .context("Environment subscription disconnected")??;
        self.acknowledge = true;
        Ok(values)
    }
}
impl Drop for Subscription {
    fn drop(&mut self) {
        let _ = self
            .client
            .connection
            .requests
            .try_send(Request::Interrupt(self.id.clone()));
    }
}

async fn run(socket: Socket, mut requests: mpsc::Receiver<Request>) {
    let (mut sender, mut receiver) = socket.split();
    let mut pending = HashMap::<String, Pending>::new();
    let mut heartbeat = tokio::time::interval(Duration::from_secs(20));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut last_response = tokio::time::Instant::now();
    let result: Result<()> = async {
        loop {
            tokio::select! {
                request=requests.recv()=>{
                    let Some(request)=request else { break; };
                    let wire=match request {
                        Request::Call{id,tag,payload,response}=> { pending.insert(id.clone(),Pending::Call(response));json!({"_tag":"Request","id":id,"tag":tag,"payload":payload,"headers":[]}) },
                        Request::Subscribe{id,tag,payload,values}=> { pending.insert(id.clone(),Pending::Subscribe(values));json!({"_tag":"Request","id":id,"tag":tag,"payload":payload,"headers":[]}) },
                        Request::Ack(id)=>json!({"_tag":"Ack","requestId":id}),
                        Request::Interrupt(id)=>{pending.remove(&id);json!({"_tag":"Interrupt","requestId":id})},
                    };
                    tokio::time::timeout(Duration::from_secs(10),sender.send(Message::Text(wire.to_string().into()))).await??;
                }
                response=receiver.next()=>{
                    let frame=response.context("Environment disconnected")??;
                    last_response=tokio::time::Instant::now();
                    let text=match frame { Message::Text(text)=>text, Message::Ping(_) | Message::Pong(_)=>continue, _=>break };
                    let response:Value=serde_json::from_str(&text)?;
                    let frames=match response { Value::Array(frames)=>frames, value=>vec![value] };
                    for response in frames {
                        if response["_tag"]=="Pong" { continue; }
                        let Some(id)=response["requestId"].as_str() else { continue; };
                        match response["_tag"].as_str() {
                            Some("Chunk")=>{
                                let values=response["values"].as_array().context("Invalid RPC chunk")?.clone();
                                if let Some(Pending::Subscribe(subscription))=pending.get(id) {
                                    ensure!(subscription.try_send(Ok(values)).is_ok(),"Environment subscription stopped consuming updates");
                                }
                            },
                            Some("Exit")=>{
                                let result=if response["exit"]["_tag"]=="Success" { Ok(response["exit"]["value"].clone()) } else { Err(anyhow::anyhow!(failure_message(&response["exit"]["cause"]))) };
                                match pending.remove(id) {
                                    Some(Pending::Call(response))=>{let _=response.send(result);},
                                    Some(Pending::Subscribe(subscription))=>{let _=subscription.try_send(Err(result.err().unwrap_or_else(||anyhow::anyhow!("Environment subscription ended"))));},
                                    None=>{},
                                }
                            },
                            _=>{},
                        }
                    }
                }
                _=heartbeat.tick()=>{
                    ensure!(last_response.elapsed()<Duration::from_secs(50),"Environment stopped responding");
                    tokio::time::timeout(Duration::from_secs(10),sender.send(Message::Text(json!({"_tag":"Ping"}).to_string().into()))).await??;
                }
            }
        }
        Ok(())
    }.await;
    let message = result
        .err()
        .map(|error| error.to_string())
        .unwrap_or_else(|| "Environment disconnected".into());
    for (_, pending) in pending {
        match pending {
            Pending::Call(response) => {
                let _ = response.send(Err(anyhow::anyhow!(message.clone())));
            }
            Pending::Subscribe(subscription) => {
                let _ = subscription.try_send(Err(anyhow::anyhow!(message.clone())));
            }
        }
    }
}
fn failure_message(cause: &Value) -> String {
    cause
        .as_array()
        .and_then(|causes| {
            causes.iter().find_map(|cause| {
                cause["error"]["message"]
                    .as_str()
                    .or_else(|| cause["defect"]["message"].as_str())
            })
        })
        .unwrap_or("Environment request failed")
        .to_owned()
}
