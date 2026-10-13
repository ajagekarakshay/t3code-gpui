use anyhow::{Context, Result, ensure};
use async_channel::{Receiver, Sender};
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};
use t3_client::{Client, PairingTarget, SavedSession, Subscription};
use t3_server::Server;

pub(crate) enum Request {
    Select(String),
    Dispatch {
        command: Value,
        select: Option<String>,
        clear_draft: bool,
    },
    Cancel(String),
    Configure {
        thread: String,
        id: String,
        value: Value,
    },
    Authenticate {
        thread: String,
        method: String,
    },
    Permission {
        thread: String,
        id: String,
        option: Option<String>,
    },
    Pair(String),
    Restore(Vec<SavedSession>),
    Environment(Option<String>),
    Forget(String),
    PairingLink(String),
}

pub(crate) enum Update {
    Shell(Value),
    Thread {
        id: String,
        snapshot: Value,
        reset: bool,
    },
    Agent(Value),
    Completed {
        submitted: Option<(String, String)>,
    },
    Failed(String),
    Environment {
        id: Option<String>,
        label: String,
        connected: bool,
        scopes: Vec<String>,
    },
    Saved(Vec<SavedSession>),
    PairingLink(String),
}

struct Remote {
    session: SavedSession,
    client: Option<Client>,
    events: Option<Subscription>,
    retry_at: tokio::time::Instant,
    retry_seconds: u64,
}

impl Remote {
    fn new(session: SavedSession) -> Self {
        Self {
            session,
            client: None,
            events: None,
            retry_at: tokio::time::Instant::now(),
            retry_seconds: 1,
        }
    }

    async fn reconnect(&mut self, selected: Option<&str>, updates: &Sender<Update>) -> Result<()> {
        let client = Client::connect(&self.session).await?;
        let descriptor = client.call("server.probe", json!({})).await?;
        ensure!(
            descriptor["rustMigration"] == true,
            "This server uses the original protocol. Native client compatibility is still being ported."
        );
        ensure!(
            descriptor["environmentId"] == self.session.environment_id(),
            "The environment at this address has changed. Pair again to reconnect."
        );
        let shell = self.session.get("/api/orchestration/shell").await?;
        let events = client
            .subscribe(
                "rust.subscribeEvents",
                json!({"afterSequence":shell["snapshotSequence"]}),
            )
            .await?;
        let _ = updates.send(Update::Shell(shell)).await;
        if let Some(id) = selected {
            let snapshot = client
                .call("rust.getThread", json!({"threadId":id}))
                .await?;
            let _ = updates
                .send(Update::Thread {
                    id: id.into(),
                    snapshot,
                    reset: false,
                })
                .await;
        }
        self.client = Some(client);
        self.events = Some(events);
        self.retry_seconds = 1;
        Ok(())
    }

    fn disconnected(&mut self) {
        self.client = None;
        self.events = None;
        self.retry_at = tokio::time::Instant::now() + Duration::from_secs(self.retry_seconds);
        self.retry_seconds = (self.retry_seconds * 2).min(30);
    }
}

async fn environment_update(remote: Option<&Remote>, updates: &Sender<Update>) {
    let update = match remote {
        Some(remote) => Update::Environment {
            id: Some(remote.session.environment_id().into()),
            label: remote.session.label().into(),
            connected: remote.client.is_some(),
            scopes: remote.session.scopes().to_vec(),
        },
        None => Update::Environment {
            id: None,
            label: "Local environment".into(),
            connected: true,
            scopes: Vec::new(),
        },
    };
    let _ = updates.send(update).await;
}

async fn snapshots(
    server: &Server,
    remote: Option<&Remote>,
    selected: Option<&str>,
    reset: bool,
    updates: &Sender<Update>,
) -> Result<()> {
    let shell = match remote {
        Some(remote) => remote.session.get("/api/orchestration/shell").await?,
        None => server.shell()?,
    };
    let _ = updates.send(Update::Shell(shell)).await;
    if let Some(id) = selected {
        let snapshot = match remote {
            Some(remote) => {
                remote
                    .client
                    .as_ref()
                    .context("Environment is disconnected")?
                    .call("rust.getThread", json!({"threadId":id}))
                    .await?
            }
            None => server.thread(id)?,
        };
        let _ = updates
            .send(Update::Thread {
                id: id.into(),
                snapshot,
                reset,
            })
            .await;
    }
    Ok(())
}

/// Owns local and remote connections on Tokio, delivering owned updates to GPUI.
pub(crate) async fn run(server: Server, requests: Receiver<Request>, updates: Sender<Update>) {
    let mut selected = None::<String>;
    let mut local_events = server.subscribe();
    let mut saved = BTreeMap::<String, SavedSession>::new();
    let mut remote = None::<Remote>;
    environment_update(None, &updates).await;
    let _ = snapshots(&server, None, None, false, &updates).await;

    loop {
        let retry = remote
            .as_ref()
            .filter(|remote| remote.client.is_none())
            .map(|remote| remote.retry_at);
        tokio::select! {
            request=requests.recv()=>{
                let Ok(request)=request else { break; };
                let result:Result<()>=async {
                    match request {
                        Request::Restore(sessions)=>{
                            for session in sessions {
                                if !session.expired() {
                                    saved.insert(session.environment_id().into(),session);
                                }
                            }
                            let _=updates.send(Update::Saved(saved.values().cloned().collect())).await;
                        }
                        Request::Pair(link)=>{
                            let session=PairingTarget::from_url(&link)?.exchange().await?;
                            let mut next=Remote::new(session.clone());
                            next.reconnect(None,&updates).await?;
                            saved.insert(session.environment_id().into(),session);
                            remote=Some(next);
                            selected=None;
                            environment_update(remote.as_ref(),&updates).await;
                            let _=updates.send(Update::Saved(saved.values().cloned().collect())).await;
                        }
                        Request::Environment(id)=>{
                            remote=match id {
                                Some(id)=>Some(Remote::new(saved.get(&id).context("Unknown environment")?.clone())),
                                None=>None,
                            };
                            selected=None;
                            environment_update(remote.as_ref(),&updates).await;
                            if remote.is_none() {
                                snapshots(&server,None,None,false,&updates).await?;
                            }
                        }
                        Request::Forget(id)=>{
                            saved.remove(&id);
                            if remote.as_ref().is_some_and(|remote|remote.session.environment_id()==id) {
                                remote=None;
                                selected=None;
                                environment_update(None,&updates).await;
                                snapshots(&server,None,None,false,&updates).await?;
                            }
                            let _=updates.send(Update::Saved(saved.values().cloned().collect())).await;
                        }
                        Request::PairingLink(origin)=>{
                            let _=updates.send(Update::PairingLink(server.pairing_url(&origin)?)).await;
                        }
                        Request::Select(id)=>{
                            let snapshot=if let Some(remote)=&remote {
                                remote.client.as_ref().context("Environment is disconnected")?.call("rust.getThread",json!({"threadId":id})).await?
                            } else {
                                server.thread(&id)?
                            };
                            selected=Some(id.clone());
                            let _=updates.send(Update::Thread{id,snapshot,reset:true}).await;
                        }
                        Request::Dispatch{command,select,clear_draft}=>{
                            let submitted=if clear_draft {
                                command["threadId"].as_str().zip(command["text"].as_str()).map(|(id,text)|(id.to_owned(),text.to_owned()))
                            } else {None};
                            if let Some(remote)=&remote {
                                remote.client.as_ref().context("Environment is disconnected")?.call("orchestration.dispatchCommand",command).await?;
                            } else {
                                server.dispatch(command).await?;
                            }
                            if let Some(id)=select {
                                selected=Some(id);
                                snapshots(&server,remote.as_ref(),selected.as_deref(),true,&updates).await?;
                            }
                            let _=updates.send(Update::Completed{submitted}).await;
                        }
                        Request::Cancel(id)=>{
                            if let Some(remote)=&remote {
                                remote.client.as_ref().context("Environment is disconnected")?.call("rust.cancel",json!({"threadId":id})).await?;
                            } else { server.cancel(&id).await?; }
                        }
                        Request::Configure{thread,id,value}=>{
                            if let Some(remote)=&remote {
                                remote.client.as_ref().context("Environment is disconnected")?.call("rust.configureAgent",json!({"threadId":thread,"id":id,"value":value})).await?;
                            } else { server.configure_agent(&thread,id,value).await?; }
                        }
                        Request::Authenticate{thread,method}=>{
                            if let Some(remote)=&remote {
                                remote.client.as_ref().context("Environment is disconnected")?.call("rust.authenticateAgent",json!({"threadId":thread,"method":method})).await?;
                            } else { server.authenticate_agent(&thread,method).await?; }
                        }
                        Request::Permission{thread,id,option}=>{
                            if let Some(remote)=&remote {
                                remote.client.as_ref().context("Environment is disconnected")?.call("rust.answerPermission",json!({"threadId":thread,"id":id,"optionId":option})).await?;
                            } else { server.answer_permission(&thread,&id,option).await?; }
                        }
                    }
                    Ok(())
                }.await;
                if let Err(error)=result {
                    let _=updates.send(Update::Failed(error.to_string())).await;
                }
            }
            event=local_events.recv(),if remote.is_none()=>{
                let event=match event {
                    Ok(event)=>event,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_))=>json!({}),
                    Err(_)=>break,
                };
                if !event["agentEvent"].is_null() {
                    if updates.send(Update::Agent(event)).await.is_err(){break;}
                } else if let Err(error)=snapshots(&server,None,selected.as_deref(),false,&updates).await {
                    let _=updates.send(Update::Failed(error.to_string())).await;
                }
            }
            page=async { remote.as_mut().expect("guarded").events.as_mut().expect("guarded").next().await },if remote.as_ref().is_some_and(|remote|remote.events.is_some())=>{
                match page {
                    Ok(pages)=>{
                        let mut refresh=false;
                        for page in pages {
                            if let Some(changes)=page["changes"].as_array() {
                                for change in changes {
                                    if change["agentEvent"].is_null(){refresh=true;}else{let _=updates.send(Update::Agent(change.clone())).await;}
                                }
                            }
                        }
                        if refresh && let Err(error)=snapshots(&server,remote.as_ref(),selected.as_deref(),false,&updates).await {
                            let _=updates.send(Update::Failed(error.to_string())).await;
                        }
                    }
                    Err(error)=>{
                        remote.as_mut().expect("guarded").disconnected();
                        environment_update(remote.as_ref(),&updates).await;
                        let _=updates.send(Update::Failed(format!("{error}. Reconnecting…"))).await;
                    }
                }
            }
            _=async {tokio::time::sleep_until(retry.expect("guarded")).await},if retry.is_some()=>{
                let result=remote.as_mut().expect("guarded").reconnect(selected.as_deref(),&updates).await;
                if let Err(error)=result {
                    remote.as_mut().expect("guarded").disconnected();
                    let _=updates.send(Update::Failed(format!("{error}. Reconnecting…"))).await;
                }
                environment_update(remote.as_ref(),&updates).await;
            }
        }
    }
}
