use async_channel::{Receiver, Sender};
use serde_json::Value;
use t3_server::Server;

pub(crate) enum Request {
    Select(String),
    Dispatch {
        command: Value,
        select: Option<String>,
        clear_draft: bool,
    },
    Cancel(String),
    Permission {
        thread: String,
        id: String,
        option: Option<String>,
    },
}
pub(crate) enum Update {
    Shell(Value),
    Thread {
        id: String,
        snapshot: Value,
        reset: bool,
    },
    Permission {
        thread: String,
        event: Value,
    },
    Completed {
        clear_draft: bool,
    },
    Failed(String),
}

/// Runs all database and agent work on Tokio, delivering owned results to the UI thread.
pub(crate) async fn run(server: Server, requests: Receiver<Request>, updates: Sender<Update>) {
    let mut selected = None::<String>;
    let mut events = server.subscribe();
    if let Ok(shell) = server.shell() {
        let _ = updates.send(Update::Shell(shell)).await;
    }
    loop {
        tokio::select! {
            request=requests.recv()=>{
                let Ok(request)=request else{break};
                let result=match request{
                    Request::Select(id)=>{selected=Some(id.clone());match server.thread(&id){Ok(snapshot)=>{let _=updates.send(Update::Thread{id,snapshot,reset:true}).await;Ok(())},Err(error)=>Err(error)}},
                    Request::Dispatch{command,select,clear_draft}=>match server.dispatch(command).await{
                        Ok(_)=>{
                            if let Some(id)=select{selected=Some(id.clone());if let Ok(snapshot)=server.thread(&id){let _=updates.send(Update::Thread{id,snapshot,reset:true}).await;}}
                            let _=updates.send(Update::Completed{clear_draft}).await;Ok(())
                        },Err(error)=>Err(error)
                    },
                    Request::Cancel(id)=>server.cancel(&id).await,
                    Request::Permission{thread,id,option}=>server.answer_permission(&thread,&id,option).await,
                };
                if let Err(error)=result{let _=updates.send(Update::Failed(error.to_string())).await;}
            }
            event=events.recv()=>{
                let event=match event{Ok(event)=>event,Err(tokio::sync::broadcast::error::RecvError::Lagged(_))=>serde_json::json!({}),Err(_)=>break};
                if let Ok(shell)=server.shell(){if updates.send(Update::Shell(shell)).await.is_err(){break;}}
                if let Some(id)=&selected{
                    if let Ok(snapshot)=server.thread(id){let _=updates.send(Update::Thread{id:id.clone(),snapshot,reset:false}).await;}
                }
                if event["agentEvent"]["type"]=="acp.permission"{
                    if let Some(thread)=event["threadId"].as_str(){let _=updates.send(Update::Permission{thread:thread.into(),event:event["agentEvent"].clone()}).await;}
                }
            }
        }
    }
}
