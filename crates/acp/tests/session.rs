use t3_acp::{AgentCommand, AgentEvent, Session, SessionCommand};
use tokio::sync::mpsc;

fn start(resume: Option<String>) -> (Session, mpsc::Receiver<AgentEvent>) {
    Session::start(
        AgentCommand::new(env!("CARGO_BIN_EXE_t3-acp-fixture")),
        std::env::current_dir().unwrap(),
        resume,
    )
}
async fn next(events: &mut mpsc::Receiver<AgentEvent>) -> AgentEvent {
    tokio::time::timeout(std::time::Duration::from_secs(10), events.recv())
        .await
        .expect("ACP event deadline")
        .expect("ACP event stream closed")
}

#[tokio::test]
async fn retains_native_session_and_streams_multiple_turns() {
    let (session, mut events) = start(None);
    assert!(matches!(next(&mut events).await, AgentEvent::Ready(_)));
    for prompt in ["first turn", "next turn"] {
        session.prompt(prompt.into()).await.unwrap();
        let AgentEvent::Update(first) = next(&mut events).await else {
            panic!("Missing first chunk")
        };
        assert_eq!(first["update"]["content"]["text"], prompt);
        assert!(matches!(next(&mut events).await, AgentEvent::Update(_)));
        assert!(matches!(
            next(&mut events).await,
            AgentEvent::TurnFinished(_)
        ));
    }
    session.send(SessionCommand::Shutdown).await.unwrap();
}

#[tokio::test]
async fn cancellation_is_processed_while_prompt_is_waiting() {
    let (session, mut events) = start(None);
    let _ = next(&mut events).await;
    session.prompt("wait".into()).await.unwrap();
    assert!(matches!(next(&mut events).await, AgentEvent::Update(_)));
    session.send(SessionCommand::Cancel).await.unwrap();
    let AgentEvent::TurnFinished(result) = next(&mut events).await else {
        panic!("Cancellation did not complete the turn")
    };
    assert_eq!(result["stopReason"], "cancelled");
    session.send(SessionCommand::Shutdown).await.unwrap();
}

#[tokio::test]
async fn permissions_wait_for_the_selected_native_option() {
    let (session, mut events) = start(None);
    let _ = next(&mut events).await;
    session.prompt("permission".into()).await.unwrap();
    let AgentEvent::Permission { id, request } = next(&mut events).await else {
        panic!("Missing permission request")
    };
    assert_eq!(request["options"][0]["optionId"], "allow");
    session
        .answer_permission(&id, Some("deny".into()))
        .await
        .unwrap();
    let AgentEvent::Update(update) = next(&mut events).await else {
        panic!("Missing chosen option")
    };
    assert_eq!(update["update"]["content"]["text"], "deny");
    assert!(matches!(
        next(&mut events).await,
        AgentEvent::TurnFinished(_)
    ));
    assert!(
        session
            .answer_permission(&id, Some("allow".into()))
            .await
            .is_err()
    );
    session.send(SessionCommand::Shutdown).await.unwrap();
}

#[tokio::test]
async fn loads_a_native_session_before_prompting() {
    let (session, mut events) = start(Some("native-test".into()));
    let AgentEvent::Ready(ready) = next(&mut events).await else {
        panic!("Missing loaded session")
    };
    assert_eq!(ready["sessionId"], "native-test");
    session.send(SessionCommand::Shutdown).await.unwrap();
}
