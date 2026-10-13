use crate::backend::{self, Request, Update};
use gpui_kit::component::{
    ActiveTheme, Disableable, Selectable, Sizable, WindowExt,
    button::{Button, ButtonVariants},
    clipboard::Clipboard,
    command::{Command, CommandItem, CommandState},
    input::{Input, InputEvent, InputState, Textarea, TextareaState},
    message_scroller::{MessageScroller, MessageScrollerState},
    select::{SearchableVec, Select, SelectEvent, SelectItem, SelectState},
    sidebar::{SidebarFooter, SidebarHeader},
    text::TextView,
};
use gpui_kit::prelude::FluentBuilder as _;
use gpui_kit::*;
use serde_json::{Value, json};
use std::{collections::HashMap, net::SocketAddr, rc::Rc, sync::Arc};
use t3_client::SavedSession;
use t3_server::Server;

const CREDENTIAL_KEY: &str = "t3code://native/environments";

actions!(
    t3,
    [
        NewThread,
        AddProject,
        SendPrompt,
        ShowCommands,
        ShowSettings,
        StopTurn
    ]
);
pub(crate) fn bind_keys(cx: &mut App) {
    let modifier = if cfg!(target_os = "macos") {
        "cmd"
    } else {
        "ctrl"
    };
    cx.bind_keys([
        KeyBinding::new(&format!("{modifier}-n"), NewThread, Some("T3Workspace")),
        KeyBinding::new(&format!("{modifier}-o"), AddProject, Some("T3Workspace")),
        KeyBinding::new(
            &format!("{modifier}-enter"),
            SendPrompt,
            Some("T3Workspace"),
        ),
        KeyBinding::new(&format!("{modifier}-k"), ShowCommands, Some("T3Workspace")),
        KeyBinding::new(&format!("{modifier}-,"), ShowSettings, Some("T3Workspace")),
        KeyBinding::new(&format!("{modifier}-."), StopTurn, Some("T3Workspace")),
    ]);
}

struct Row {
    id: SharedString,
    title: SharedString,
    project: bool,
}
#[derive(Clone)]
struct ChatMessage {
    id: SharedString,
    role: SharedString,
    text: SharedString,
    tool: Option<Value>,
    tool_text: SharedString,
}
#[derive(Clone)]
struct AgentChoice {
    value: Value,
    label: SharedString,
}
impl SelectItem for AgentChoice {
    type Value = Value;
    fn title(&self) -> SharedString {
        self.label.clone()
    }
    fn value(&self) -> &Value {
        &self.value
    }
}
struct AgentControl {
    label: SharedString,
    state: Entity<SelectState<SearchableVec<AgentChoice>>>,
    _subscription: Subscription,
}
enum Page {
    Conversation,
    Project,
    Settings,
}

pub(crate) struct Workspace {
    requests: async_channel::Sender<Request>,
    _backend: tokio::task::JoinHandle<()>,
    _runtime: Arc<tokio::runtime::Runtime>,
    _updates: Task<()>,
    _subscriptions: Vec<Subscription>,
    focus: FocusHandle,
    composer: Entity<TextareaState>,
    path: Entity<InputState>,
    pairing: Entity<InputState>,
    palette: Entity<CommandState>,
    scroller: Entity<MessageScrollerState>,
    shell: Value,
    detail: Value,
    rows: Rc<Vec<Row>>,
    messages: Rc<Vec<ChatMessage>>,
    selected_project: Option<String>,
    selected_thread: Option<String>,
    provider: String,
    drafts: HashMap<String, String>,
    permissions: HashMap<String, Value>,
    agent_controls: Vec<AgentControl>,
    page: Page,
    busy: bool,
    error: Option<SharedString>,
    address: SocketAddr,
    environment_id: Option<String>,
    environment_label: SharedString,
    environment_connected: bool,
    environment_scopes: Vec<String>,
    saved_environments: Vec<SavedSession>,
    pairing_link: Option<SharedString>,
    _credentials: Task<()>,
}

impl Workspace {
    pub(crate) fn new(
        server: Server,
        runtime: Arc<tokio::runtime::Runtime>,
        address: SocketAddr,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let (requests, request_receiver) = async_channel::bounded(32);
        let (updates, update_receiver) = async_channel::bounded(256);
        let backend = runtime.spawn(backend::run(server, request_receiver, updates));
        let composer = cx.new(|cx| {
            TextareaState::new(window, cx)
                .placeholder("Ask your agent to work on this project…")
                .rows(4)
        });
        let path =
            cx.new(|cx| InputState::new(window, cx).placeholder("Absolute project directory"));
        let pairing =
            cx.new(|cx| InputState::new(window, cx).placeholder("Paste a T3 pairing link"));
        let palette = cx.new(|cx| CommandState::new(window, cx));
        let scroller = cx.new(|cx| MessageScrollerState::new(0, cx));
        let subscriptions = vec![
            cx.observe(&scroller, |_, _, cx| cx.notify()),
            cx.subscribe(&composer, |_, _, event, cx| {
                if matches!(event, InputEvent::Change) {
                    cx.notify();
                }
            }),
        ];
        let task = cx.spawn_in(window, async move |this, cx| {
            while let Ok(update) = update_receiver.recv().await {
                if this
                    .update_in(cx, |this, window, cx| this.receive(update, window, cx))
                    .is_err()
                {
                    break;
                }
            }
        });
        let focus = cx.focus_handle();
        focus.focus(window, cx);
        let credential_request = requests.clone();
        let credentials = cx.read_credentials(CREDENTIAL_KEY);
        let credential_task = cx.spawn(async move |this, cx| match credentials.await {
            Ok(Some((_, bytes))) => match serde_json::from_slice::<Vec<SavedSession>>(&bytes) {
                Ok(sessions) => {
                    let _ = credential_request.send(Request::Restore(sessions)).await;
                }
                Err(error) => {
                    let _ = this.update(cx, |this, cx| {
                        this.error =
                            Some(format!("Could not read saved environments: {error}").into());
                        cx.notify();
                    });
                }
            },
            Ok(None) => {}
            Err(error) => {
                let _ = this.update(cx, |this, cx| {
                    this.error =
                        Some(format!("Could not open the credential store: {error}").into());
                    cx.notify();
                });
            }
        });
        Self {
            requests,
            _backend: backend,
            _runtime: runtime,
            _updates: task,
            _subscriptions: subscriptions,
            focus,
            composer,
            path,
            pairing,
            palette,
            scroller,
            shell: json!({}),
            detail: json!({}),
            rows: Rc::default(),
            messages: Rc::default(),
            selected_project: None,
            selected_thread: None,
            provider: "claude-code".into(),
            drafts: HashMap::new(),
            permissions: HashMap::new(),
            agent_controls: Vec::new(),
            page: Page::Conversation,
            busy: false,
            error: None,
            address,
            environment_id: None,
            environment_label: "Local environment".into(),
            environment_connected: true,
            environment_scopes: Vec::new(),
            saved_environments: Vec::new(),
            pairing_link: None,
            _credentials: credential_task,
        }
    }

    fn receive(&mut self, update: Update, window: &mut Window, cx: &mut Context<Self>) {
        match update {
            Update::Shell(shell) => {
                let mut rows = Vec::new();
                if let Some(projects) = shell["projects"].as_array() {
                    for project in projects {
                        let id = project["id"].as_str().unwrap_or_default();
                        rows.push(Row {
                            id: id.to_owned().into(),
                            title: project["title"]
                                .as_str()
                                .unwrap_or("Project")
                                .to_owned()
                                .into(),
                            project: true,
                        });
                        if let Some(threads) = shell["threads"].as_array() {
                            for thread in threads.iter().filter(|thread| thread["projectId"] == id)
                            {
                                rows.push(Row {
                                    id: thread["id"].as_str().unwrap_or_default().to_owned().into(),
                                    title: thread["title"]
                                        .as_str()
                                        .unwrap_or("Thread")
                                        .to_owned()
                                        .into(),
                                    project: false,
                                });
                            }
                        }
                    }
                }
                self.rows = Rc::new(rows);
                self.shell = shell;
            }
            Update::Thread {
                id,
                snapshot,
                reset,
            } => {
                if self
                    .selected_thread
                    .as_ref()
                    .is_some_and(|selected| *selected != id)
                    && !reset
                {
                    return;
                }
                if reset {
                    if let Some(previous) = &self.selected_thread {
                        self.drafts
                            .insert(previous.clone(), self.composer.read(cx).value().to_string());
                    }
                    let draft = self.drafts.get(&id).cloned().unwrap_or_default();
                    self.composer
                        .update(cx, |state, cx| state.set_value(draft, window, cx));
                    self.selected_thread = Some(id.clone());
                    self.selected_project =
                        snapshot["thread"]["projectId"].as_str().map(str::to_owned);
                    self.page = Page::Conversation;
                }
                if let Some(permission) = snapshot["thread"]
                    .get("pendingAcpPermission")
                    .filter(|value| !value.is_null())
                {
                    self.permissions.insert(id.clone(), permission.clone());
                }
                let messages = snapshot["messages"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|message| ChatMessage {
                        id: message["id"].as_str().unwrap_or_default().to_owned().into(),
                        role: message["role"]
                            .as_str()
                            .unwrap_or("assistant")
                            .to_owned()
                            .into(),
                        text: message["text"]
                            .as_str()
                            .unwrap_or_default()
                            .to_owned()
                            .into(),
                        tool: message.get("toolCall").cloned(),
                        tool_text: message.get("toolCall").map(tool_output).unwrap_or_default(),
                    })
                    .collect::<Vec<_>>();
                let old_count = self.messages.len();
                let count = messages.len();
                self.messages = Rc::new(messages);
                self.error = snapshot["thread"]["lastError"]
                    .as_str()
                    .map(|message| SharedString::from(message.to_owned()));
                self.detail = snapshot;
                self.refresh_agent_controls(window, cx);
                self.scroller.update(cx, |state, cx| {
                    if reset || count < old_count {
                        state.reset(count, cx);
                    } else if count > old_count {
                        state.append(count - old_count, cx);
                    } else if count > 0 {
                        state.remeasure_items(count - 1..count, cx);
                    }
                });
                if matches!(
                    self.detail["thread"]["status"].as_str(),
                    Some("idle" | "error")
                ) && let Some(id) = &self.selected_thread
                {
                    self.permissions.remove(id);
                }
            }
            Update::Agent(change) => self.receive_agent(change, window, cx),
            Update::Completed { submitted } => {
                self.busy = false;
                self.error = None;
                if let Some((id, text)) = submitted {
                    if self.selected_thread.as_deref() == Some(&id)
                        && self.composer.read(cx).value().as_ref() == text
                    {
                        self.composer
                            .update(cx, |state, cx| state.set_value("", window, cx));
                    }
                    if self.drafts.get(&id).is_some_and(|draft| *draft == text) {
                        self.drafts.remove(&id);
                    }
                }
                if matches!(self.page, Page::Project) {
                    self.page = Page::Conversation;
                }
            }
            Update::Failed(error) => {
                self.busy = false;
                self.error = Some(error.into());
            }
            Update::Environment {
                id,
                label,
                connected,
                scopes,
            } => {
                self.busy = false;
                if connected {
                    self.error = None;
                }
                self.environment_id = id;
                self.environment_label = label.into();
                self.environment_connected = connected;
                self.environment_scopes = scopes;
                self.selected_project = None;
                self.selected_thread = None;
                self.detail = json!({});
                self.messages = Rc::default();
                self.permissions.clear();
            }
            Update::Saved(sessions) => {
                self.saved_environments = sessions;
                match serde_json::to_vec(&self.saved_environments) {
                    Ok(bytes) => cx
                        .write_credentials(CREDENTIAL_KEY, "environments", &bytes)
                        .detach(),
                    Err(error) => {
                        self.error = Some(format!("Could not save environments: {error}").into());
                    }
                }
            }
            Update::PairingLink(link) => {
                self.pairing_link = Some(link.into());
            }
        }
        cx.notify();
    }

    fn receive_agent(&mut self, change: Value, window: &mut Window, cx: &mut Context<Self>) {
        let Some(thread_id) = change["threadId"].as_str() else {
            return;
        };
        if change["sequence"].as_u64() > self.shell["snapshotSequence"].as_u64()
            && let Some(threads) = self.shell["threads"].as_array_mut()
            && let Some(thread) = threads.iter_mut().find(|thread| thread["id"] == thread_id)
        {
            *thread = change["thread"].clone();
        }
        if self.selected_thread.as_deref() != Some(thread_id) {
            return;
        }
        if change["sequence"].as_u64() <= self.detail["snapshotSequence"].as_u64() {
            return;
        }
        self.detail["thread"] = change["thread"].clone();
        self.detail["snapshotSequence"] = change["sequence"].clone();
        let event = &change["agentEvent"];
        if event["type"] == "acp.failure" {
            self.error = event["message"]
                .as_str()
                .map(|message| SharedString::from(message.to_owned()));
        }
        if event["type"] == "acp.permission" {
            self.permissions.insert(thread_id.to_owned(), event.clone());
        }
        if matches!(
            event["type"].as_str(),
            Some("acp.turn.finished" | "acp.failure" | "acp.permission.answered")
        ) {
            self.permissions.remove(thread_id);
        }
        let update = &event["data"]["update"];
        let run = change["thread"]["latestRunId"].as_str().unwrap_or_default();
        let old_count = self.messages.len();
        let messages = Rc::make_mut(&mut self.messages);
        match update["sessionUpdate"].as_str() {
            Some("agent_message_chunk") => {
                let id = format!("assistant:{run}");
                let text = update["content"]["text"].as_str().unwrap_or_default();
                if let Some(message) = messages
                    .iter_mut()
                    .find(|message| message.id.as_ref() == id)
                {
                    message.text = format!("{}{text}", message.text).into();
                } else {
                    messages.push(ChatMessage {
                        id: id.into(),
                        role: "assistant".into(),
                        text: text.to_owned().into(),
                        tool: None,
                        tool_text: SharedString::default(),
                    });
                }
            }
            Some("tool_call" | "tool_call_update") => {
                let id = format!(
                    "tool:{run}:{}",
                    update["toolCallId"].as_str().unwrap_or_default()
                );
                let index = messages
                    .iter()
                    .position(|message| message.id.as_ref() == id)
                    .unwrap_or_else(|| {
                        messages.push(ChatMessage {
                            id: id.into(),
                            role: "tool".into(),
                            text: "".into(),
                            tool: Some(json!({})),
                            tool_text: SharedString::default(),
                        });
                        messages.len() - 1
                    });
                let message = &mut messages[index];
                if let Some(fields) = update.as_object() {
                    for (key, value) in fields {
                        message.tool.as_mut().unwrap()[key] = value.clone();
                    }
                }
                message.tool_text = tool_output(message.tool.as_ref().unwrap());
                message.text = message.tool.as_ref().unwrap()["title"]
                    .as_str()
                    .unwrap_or("Tool call")
                    .to_owned()
                    .into();
            }
            _ => {}
        }
        let count = messages.len();
        self.scroller.update(cx, |state, cx| {
            if count > old_count {
                state.append(count - old_count, cx);
            } else if count > 0 {
                state.remeasure_items(count - 1..count, cx);
            }
        });
        if matches!(
            event["type"].as_str(),
            Some("acp.ready" | "acp.configuration")
        ) || matches!(
            update["sessionUpdate"].as_str(),
            Some("config_option_update" | "current_mode_update")
        ) {
            self.refresh_agent_controls(window, cx);
        }
    }
    fn refresh_agent_controls(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.agent_controls.clear();
        let Some(thread) = self.selected_thread.clone() else {
            return;
        };
        if let Some(options) =
            self.detail["thread"]["agentConfiguration"]["session"]["configOptions"].as_array()
        {
            for option in options {
                let Some(id) = option["id"].as_str() else {
                    continue;
                };
                let mut choices = Vec::new();
                if option["type"] == "boolean" {
                    choices.push(AgentChoice {
                        value: json!(true),
                        label: "On".into(),
                    });
                    choices.push(AgentChoice {
                        value: json!(false),
                        label: "Off".into(),
                    });
                } else if let Some(values) = option["options"].as_array() {
                    for value in values {
                        let entries = value["options"]
                            .as_array()
                            .cloned()
                            .unwrap_or_else(|| vec![value.clone()]);
                        for entry in entries {
                            if entry["value"].is_string() {
                                choices.push(AgentChoice {
                                    value: entry["value"].clone(),
                                    label: entry["name"]
                                        .as_str()
                                        .unwrap_or("Option")
                                        .to_owned()
                                        .into(),
                                });
                            }
                        }
                    }
                }
                let selected = choices
                    .iter()
                    .position(|choice| choice.value == option["currentValue"])
                    .map(|row| gpui_kit::component::IndexPath::default().row(row));
                let state = cx.new(|cx| {
                    SelectState::new(SearchableVec::from(choices), selected, window, cx)
                        .searchable(true)
                });
                let id = id.to_owned();
                let thread = thread.clone();
                let subscription = cx.subscribe(&state, move |this, _, event, cx| {
                    if let SelectEvent::Confirm(Some(value)) = event {
                        this.request(
                            Request::Configure {
                                thread: thread.clone(),
                                id: id.clone(),
                                value: value.clone(),
                            },
                            cx,
                        );
                    }
                });
                self.agent_controls.push(AgentControl {
                    label: option["name"]
                        .as_str()
                        .unwrap_or("Agent setting")
                        .to_owned()
                        .into(),
                    state,
                    _subscription: subscription,
                });
            }
        }
    }

    fn request(&mut self, request: Request, cx: &mut Context<Self>) {
        if let Err(error) = self.requests.try_send(request) {
            self.error = Some(format!("Could not send request: {error}").into());
            self.busy = false;
        }
        cx.notify();
    }
    fn dispatch(
        &mut self,
        mut command: Value,
        select: Option<String>,
        clear_draft: bool,
        cx: &mut Context<Self>,
    ) {
        if self.busy {
            return;
        }
        command["commandId"] = json!(uuid::Uuid::new_v4().to_string());
        self.busy = true;
        self.error = None;
        self.request(
            Request::Dispatch {
                command,
                select,
                clear_draft,
            },
            cx,
        );
    }
    fn new_thread(&mut self, _: &NewThread, _: &mut Window, cx: &mut Context<Self>) {
        let project = self
            .selected_project
            .clone()
            .or_else(|| self.shell["projects"][0]["id"].as_str().map(str::to_owned));
        if let Some(project) = project {
            let id = uuid::Uuid::new_v4().to_string();
            self.dispatch(json!({"type":"thread.create","threadId":id,"projectId":project,"title":"New thread","modelSelection":{"instanceId":self.provider,"model":"default"},"runtimeMode":"supervised","interactionMode":"default","createdBy":"user","creationSource":"server","branch":null,"worktreePath":null}),Some(id),false,cx);
        } else {
            self.page = Page::Project;
            cx.notify();
        }
    }
    fn add_project(&mut self, _: &AddProject, window: &mut Window, cx: &mut Context<Self>) {
        self.page = Page::Project;
        self.path.focus_handle(cx).focus(window, cx);
        cx.notify();
    }
    fn save_project(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let path = self.path.read(cx).value().to_string();
        let title = std::path::Path::new(&path)
            .file_name()
            .and_then(|v| v.to_str())
            .unwrap_or("Project")
            .to_owned();
        let id = uuid::Uuid::new_v4().to_string();
        self.selected_project = Some(id.clone());
        self.dispatch(
            json!({"type":"project.create","projectId":id,"workspaceRoot":path,"title":title}),
            None,
            false,
            cx,
        );
        let _ = window;
    }
    fn send_prompt(&mut self, _: &SendPrompt, _: &mut Window, cx: &mut Context<Self>) {
        if self.busy || self.is_running() {
            return;
        }
        if let Some(thread) = self.selected_thread.clone() {
            let text = self.composer.read(cx).value().to_string();
            if text.trim().is_empty() {
                return;
            }
            self.dispatch(json!({"type":"message.dispatch","threadId":thread,"messageId":uuid::Uuid::new_v4().to_string(),"text":text,"attachments":[],"dispatchMode":{"type":"start_immediately"}}),None,true,cx);
        }
    }
    fn stop_turn(&mut self, _: &StopTurn, _: &mut Window, cx: &mut Context<Self>) {
        if let Some(id) = self.selected_thread.clone() {
            self.request(Request::Cancel(id), cx);
        }
    }
    fn settings(&mut self, _: &ShowSettings, _: &mut Window, cx: &mut Context<Self>) {
        self.page = Page::Settings;
        cx.notify();
    }
    fn pair_environment(&mut self, _: &mut Window, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        let link = self.pairing.read(cx).value().to_string();
        if link.trim().is_empty() {
            return;
        }
        self.busy = true;
        self.error = None;
        self.request(Request::Pair(link), cx);
    }
    fn select_environment(&mut self, id: Option<String>, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        self.error = None;
        self.request(Request::Environment(id), cx);
    }
    fn forget_environment(&mut self, id: String, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        self.request(Request::Forget(id), cx);
    }
    fn create_pairing_link(&mut self, cx: &mut Context<Self>) {
        self.pairing_link = None;
        self.request(Request::PairingLink(format!("http://{}", self.address)), cx);
    }
    fn commands(&mut self, _: &ShowCommands, window: &mut Window, cx: &mut Context<Self>) {
        let palette = self.palette.clone();
        let workspace = cx.entity().downgrade();
        window.open_dialog(cx, move |dialog, _, _| {
            let workspace = workspace.clone();
            dialog.title("Commands").child(
                Command::new(&palette)
                    .items([
                        CommandItem::new().label("New thread"),
                        CommandItem::new().label("Add project…"),
                        CommandItem::new().label("Settings…"),
                        CommandItem::new().label("Stop turn"),
                    ])
                    .on_confirm(move |index, window, cx| {
                        window.close_dialog(cx);
                        let _ = workspace.update(cx, |this, cx| match index.row {
                            0 => this.new_thread(&NewThread, window, cx),
                            1 => this.add_project(&AddProject, window, cx),
                            2 => this.settings(&ShowSettings, window, cx),
                            3 => this.stop_turn(&StopTurn, window, cx),
                            _ => {}
                        });
                    }),
            )
        });
        cx.defer_in(window, |this, window, cx| {
            this.palette.update(cx, |state, cx| state.focus(window, cx))
        });
    }

    fn is_running(&self) -> bool {
        matches!(
            self.detail["thread"]["status"].as_str(),
            Some("starting" | "running" | "waiting")
        )
    }

    fn navigation(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let rows = self.rows.clone();
        let selected = self.selected_thread.clone();
        div()
            .id("navigation")
            .flex()
            .flex_col()
            .bg(cx.theme().sidebar)
            .w_64()
            .child(
                SidebarHeader::new().child(
                    div()
                        .flex()
                        .justify_between()
                        .items_center()
                        .child("T3 Code")
                        .child(
                            Button::new("commands")
                                .label("Commands…")
                                .small()
                                .ghost()
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.commands(&ShowCommands, window, cx)
                                })),
                        ),
                ),
            )
            .child(
                uniform_list(
                    "projects-and-threads",
                    rows.len(),
                    cx.processor(move |_this, range: std::ops::Range<usize>, _, cx| {
                        range
                            .map(|ix| {
                                let row = &rows[ix];
                                let id = row.id.to_string();
                                let project = row.project;
                                div()
                                    .id((ElementId::from("navigation-row"), row.id.clone()))
                                    .px_2()
                                    .py_1()
                                    .child(
                                        Button::new((ElementId::from("navigate"), row.id.clone()))
                                            .label(row.title.clone())
                                            .ghost()
                                            .selected(selected.as_ref() == Some(&id))
                                            .on_click(cx.listener(move |this, _, _, cx| {
                                                if project {
                                                    this.selected_project = Some(id.clone());
                                                    cx.notify();
                                                } else {
                                                    this.request(Request::Select(id.clone()), cx);
                                                }
                                            })),
                                    )
                                    .into_any_element()
                            })
                            .collect()
                    }),
                )
                .flex_1()
                .min_h_0()
                .into_any_element(),
            )
            .child(
                SidebarFooter::new()
                    .child(Button::new("add-project").label("Add project…").on_click(
                        cx.listener(|this, _, window, cx| {
                            this.add_project(&AddProject, window, cx)
                        }),
                    ))
                    .child(Button::new("settings").label("Settings…").ghost().on_click(
                        cx.listener(|this, _, window, cx| this.settings(&ShowSettings, window, cx)),
                    )),
            )
    }

    fn conversation(&self, cx: &mut Context<Self>) -> AnyElement {
        if self.selected_thread.is_none() {
            return div()
                .flex()
                .flex_col()
                .flex_1()
                .items_center()
                .justify_center()
                .gap_3()
                .child("Start a thread to work with your agent")
                .child(Button::new("start-thread").label("New thread").on_click(
                    cx.listener(|this, _, window, cx| this.new_thread(&NewThread, window, cx)),
                ))
                .into_any_element();
        }
        let messages = self.messages.clone();
        let running = self.is_running();
        let mut content = div().flex().flex_col().flex_1().min_h_0().min_w_0().child(
            MessageScroller::new("conversation", self.scroller.clone(), move |ix, _, cx| {
                let message = &messages[ix];
                div()
                    .id((ElementId::from("message"), message.id.clone()))
                    .p_4()
                    .gap_2()
                    .flex()
                    .flex_col()
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .child(message.role.clone()),
                    )
                    .when_some(message.tool.as_ref(), |this, tool| {
                        this.child(
                            div()
                                .text_sm()
                                .child(tool["status"].as_str().unwrap_or("pending").to_owned()),
                        )
                        .child(TextView::markdown(
                            (ElementId::from("tool-output"), message.id.clone()),
                            message.tool_text.clone(),
                        ))
                    })
                    .child(TextView::markdown(
                        (ElementId::from("message-content"), message.id.clone()),
                        message.text.clone(),
                    ))
                    .into_any_element()
            })
            .flex_1()
            .min_h_0(),
        );
        if let Some(methods) = self.detail["thread"]["authenticationMethods"].as_array() {
            let mut panel = div()
                .p_4()
                .gap_2()
                .flex()
                .flex_col()
                .child("Sign in to continue");
            for method in methods.iter().filter(|method| method["type"] != "terminal") {
                let Some(thread) = self.selected_thread.clone() else {
                    continue;
                };
                let id = method["id"].as_str().unwrap_or_default().to_owned();
                panel = panel.child(
                    Button::new((
                        ElementId::from("agent-auth"),
                        SharedString::from(id.clone()),
                    ))
                    .label(method["name"].as_str().unwrap_or("Sign in").to_owned())
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.request(
                            Request::Authenticate {
                                thread: thread.clone(),
                                method: id.clone(),
                            },
                            cx,
                        )
                    })),
                );
            }
            content = content.child(panel);
        }
        if let Some(thread) = &self.selected_thread
            && let Some(permission) = self.permissions.get(thread)
        {
            let request_id = permission["id"].as_str().unwrap_or_default().to_owned();
            let mut panel = div()
                .p_4()
                .border_t_1()
                .border_color(cx.theme().border)
                .child(
                    permission["request"]["toolCall"]["title"]
                        .as_str()
                        .unwrap_or("Permission requested")
                        .to_owned(),
                );
            if let Some(options) = permission["request"]["options"].as_array() {
                for option in options {
                    let thread = thread.clone();
                    let id = request_id.clone();
                    let option_id = option["optionId"].as_str().unwrap_or_default().to_owned();
                    panel = panel.child(
                        Button::new((
                            ElementId::from("permission"),
                            SharedString::from(option_id.clone()),
                        ))
                        .label(option["name"].as_str().unwrap_or("Choose").to_owned())
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.permissions.remove(&thread);
                            this.request(
                                Request::Permission {
                                    thread: thread.clone(),
                                    id: id.clone(),
                                    option: Some(option_id.clone()),
                                },
                                cx,
                            );
                        })),
                    )
                }
            }
            content = content.child(panel);
        }
        content
            .child(
                div()
                    .p_4()
                    .gap_3()
                    .flex()
                    .flex_col()
                    .border_t_1()
                    .border_color(cx.theme().border)
                    .child(
                        Textarea::new(&self.composer)
                            .accessibility_id("composer")
                            .aria_label("Message"),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(cx.theme().muted_foreground)
                                    .child(if running { "Agent is working" } else { "Ready" }),
                            )
                            .child(if running {
                                Button::new("stop").label("Stop").on_click(cx.listener(
                                    |this, _, window, cx| this.stop_turn(&StopTurn, window, cx),
                                ))
                            } else {
                                Button::new("send")
                                    .label("Send")
                                    .primary()
                                    .disabled(
                                        self.busy
                                            || self.composer.read(cx).value().trim().is_empty(),
                                    )
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.send_prompt(&SendPrompt, window, cx)
                                    }))
                            }),
                    ),
            )
            .into_any_element()
    }
}

fn tool_output(tool: &Value) -> SharedString {
    let mut parts = Vec::new();
    if let Some(content) = tool["content"].as_array() {
        for item in content {
            if let Some(text) = item["content"]["text"].as_str() {
                parts.push(text.to_owned());
            }
            if item["type"] == "diff" {
                parts.push(format!(
                    "{}\nBefore:\n```\n{}\n```\nAfter:\n```\n{}\n```",
                    item["path"].as_str().unwrap_or("File change"),
                    item["oldText"].as_str().unwrap_or_default(),
                    item["newText"].as_str().unwrap_or_default()
                ));
            }
        }
    }
    if parts.is_empty()
        && let Some(output) = tool.get("rawOutput")
    {
        parts.push(
            output
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| output.to_string()),
        );
    }
    parts
        .join("\n\n")
        .chars()
        .take(24000)
        .collect::<String>()
        .into()
}

impl Render for Workspace {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let page = match self.page {
            Page::Conversation => self.conversation(cx),
            Page::Project => div()
                .p_6()
                .gap_4()
                .flex()
                .flex_col()
                .child("Add project")
                .child("Directory on this environment")
                .child(Input::new(&self.path))
                .child(
                    Button::new("save-project")
                        .label("Add project")
                        .primary()
                        .disabled(self.busy)
                        .on_click(cx.listener(|this, _, window, cx| this.save_project(window, cx))),
                )
                .into_any_element(),
            Page::Settings => div()
                .p_6()
                .gap_4()
                .flex()
                .flex_col()
                .child("Settings")
                .child("Environment")
                .child(
                    div()
                        .flex()
                        .gap_2()
                        .child(
                            Button::new("environment-local")
                                .label("Local")
                                .selected(self.environment_id.is_none())
                                .on_click(
                                    cx.listener(|this, _, _, cx| this.select_environment(None, cx)),
                                ),
                        )
                        .children(self.saved_environments.iter().map(|environment| {
                            let id = environment.environment_id().to_owned();
                            let selected = self.environment_id.as_deref() == Some(&id);
                            Button::new(SharedString::from(format!("environment-{id}")))
                                .label(environment.label().to_owned())
                                .selected(selected)
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.select_environment(Some(id.clone()), cx)
                                }))
                        })),
                )
                .child(format!(
                    "{} · {}",
                    self.environment_label,
                    if self.environment_connected {
                        "Connected"
                    } else {
                        "Reconnecting"
                    }
                ))
                .when(!self.environment_scopes.is_empty(), |settings| {
                    settings.child(format!(
                        "Permissions: {}",
                        self.environment_scopes.join(", ")
                    ))
                })
                .when_some(self.environment_id.clone(), |settings, id| {
                    settings.child(
                        Button::new(SharedString::from(format!("forget-environment-{id}")))
                            .label("Forget environment")
                            .danger()
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.forget_environment(id.clone(), cx)
                            })),
                    )
                })
                .child(Input::new(&self.pairing))
                .child(
                    Button::new("pair-environment")
                        .label("Pair environment")
                        .primary()
                        .disabled(self.busy)
                        .on_click(
                            cx.listener(|this, _, window, cx| this.pair_environment(window, cx)),
                        ),
                )
                .child(
                    Button::new("create-pairing-link")
                        .label("Create local pairing link")
                        .on_click(cx.listener(|this, _, _, cx| this.create_pairing_link(cx))),
                )
                .when_some(self.pairing_link.clone(), |settings, link| {
                    settings.child("One-use link, valid for two minutes").child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .min_w_0()
                            .child(div().flex_1().min_w_0().child(link.clone()))
                            .child(
                                Clipboard::new("copy-local-pairing-link")
                                    .value(link)
                                    .tooltip("Copy pairing link"),
                            ),
                    )
                })
                .child("Agent for new threads")
                .child(
                    div().flex().gap_2().children(
                        [("claude-code", "Claude Code"), ("codex", "Codex")]
                            .into_iter()
                            .map(|(provider, label)| {
                                Button::new(provider)
                                    .label(label)
                                    .selected(self.provider == provider)
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        this.provider = provider.into();
                                        cx.notify();
                                    }))
                            }),
                    ),
                )
                .children(self.agent_controls.iter().map(|control| {
                    div()
                        .flex()
                        .flex_col()
                        .gap_2()
                        .child(control.label.clone())
                        .child(Select::new(&control.state).disabled(self.is_running()))
                }))
                .child(format!("Local server: http://{}", self.address))
                .child("Agents use their own installed credentials and configuration.")
                .into_any_element(),
        };
        div()
            .key_context("T3Workspace")
            .track_focus(&self.focus)
            .on_action(cx.listener(Self::new_thread))
            .on_action(cx.listener(Self::add_project))
            .on_action(cx.listener(Self::send_prompt))
            .on_action(cx.listener(Self::stop_turn))
            .on_action(cx.listener(Self::commands))
            .on_action(cx.listener(Self::settings))
            .size_full()
            .flex()
            .bg(cx.theme().background)
            .text_color(cx.theme().foreground)
            .child(self.navigation(cx))
            .child(
                div()
                    .flex()
                    .flex_col()
                    .flex_1()
                    .min_w_0()
                    .min_h_0()
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .p_3()
                            .border_b_1()
                            .border_color(cx.theme().border)
                            .child(
                                self.detail["thread"]["title"]
                                    .as_str()
                                    .unwrap_or("Workspace")
                                    .to_owned(),
                            )
                            .child(Button::new("new-thread").label("New thread").on_click(
                                cx.listener(|this, _, window, cx| {
                                    this.new_thread(&NewThread, window, cx)
                                }),
                            )),
                    )
                    .when_some(self.error.clone(), |this, error| {
                        this.child(
                            div()
                                .px_4()
                                .py_2()
                                .text_color(cx.theme().danger)
                                .child(error),
                        )
                    })
                    .child(page),
            )
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        self.requests.close();
        self._backend.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::{Page, Workspace};
    use gpui_kit::test::TestWindowExt;
    use gpui_kit::{AppContext, Bounds, TestAppContext, WindowBounds, WindowOptions, px, size};
    use std::sync::Arc;
    use t3_server::{Server, ServerOptions};

    #[gpui_kit::test]
    fn command_palette_routes_to_settings_and_changes_the_agent(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        cx.update(super::bind_keys);
        let directory = tempfile::tempdir().unwrap();
        let server = Server::open(ServerOptions::new(directory.path())).unwrap();
        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let shutdown = server.clone();
        let runtime_for_window = runtime.clone();
        let (handle, workspace) = cx.update(|cx| {
            gpui_kit::open_window(
                WindowOptions {
                    window_bounds: Some(WindowBounds::Windowed(Bounds::new(
                        Default::default(),
                        size(px(1200.), px(800.)),
                    ))),
                    ..Default::default()
                },
                cx,
                move |window, cx| {
                    cx.new(|cx| {
                        Workspace::new(
                            server,
                            runtime_for_window,
                            "127.0.0.1:3773".parse().unwrap(),
                            window,
                            cx,
                        )
                    })
                },
            )
            .unwrap()
        });
        cx.update_window(handle, |_, window, cx| {
            window.render_frame(cx);
            window.click("commands", cx);
        })
        .unwrap();
        cx.run_until_parked();
        cx.update_window(handle, |_, window, cx| {
            window.press("down", cx);
            window.press("down", cx);
            window.press("enter", cx);
        })
        .unwrap();
        cx.run_until_parked();
        cx.update(|cx| assert!(matches!(workspace.read(cx).page, Page::Settings)));
        cx.update_window(handle, |_, window, cx| {
            window.render_frame(cx);
            window.click("codex", cx);
        })
        .unwrap();
        cx.update(|cx| assert_eq!(workspace.read(cx).provider, "codex"));
        cx.update_window(handle, |_, window, cx| {
            window.press(
                if cfg!(target_os = "macos") {
                    "cmd-n"
                } else {
                    "ctrl-n"
                },
                cx,
            );
        })
        .unwrap();
        cx.run_until_parked();
        cx.update(|cx| assert!(matches!(workspace.read(cx).page, Page::Project)));
        // Server replies may arrive after the user edits or changes threads.
        let snapshot = |id: &str| serde_json::json!({"snapshotSequence":0,"thread":{"id":id,"projectId":"project","status":"idle"},"messages":[]});
        cx.update_window(handle, |_, window, cx| {
            workspace.update(cx, |this, cx| {
                this.receive(
                    crate::backend::Update::Thread {
                        id: "first".into(),
                        snapshot: snapshot("first"),
                        reset: true,
                    },
                    window,
                    cx,
                )
            });
            window.render_frame(cx);
            let composer: gpui_kit::ElementId =
                ("input", workspace.read(cx).composer.entity_id()).into();
            window.click(composer.clone(), cx);
            window.input("new draft", cx);
            workspace.update(cx, |this, cx| {
                this.receive(
                    crate::backend::Update::Completed {
                        submitted: Some(("first".into(), "older prompt".into())),
                    },
                    window,
                    cx,
                )
            });
            window.render_frame(cx);
            assert_eq!(window.find(composer.clone()).value(), Some("new draft"));
            workspace.update(cx, |this, cx| {
                this.receive(
                    crate::backend::Update::Thread {
                        id: "second".into(),
                        snapshot: snapshot("second"),
                        reset: true,
                    },
                    window,
                    cx,
                )
            });
            window.render_frame(cx);
            let composer: gpui_kit::ElementId =
                ("input", workspace.read(cx).composer.entity_id()).into();
            window.click(composer.clone(), cx);
            window.input("second draft", cx);
            workspace.update(cx, |this, cx| {
                this.receive(
                    crate::backend::Update::Completed {
                        submitted: Some(("first".into(), "new draft".into())),
                    },
                    window,
                    cx,
                )
            });
            window.render_frame(cx);
            assert_eq!(window.find(composer.clone()).value(), Some("second draft"));
        })
        .unwrap();
        runtime.block_on(shutdown.shutdown());
    }
}
