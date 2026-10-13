use crate::backend::{self, Request, Update};
use gpui_kit::component::{
    ActiveTheme, Disableable, Selectable, Sizable, WindowExt,
    button::{Button, ButtonVariants},
    command::{Command, CommandItem, CommandState},
    input::{Input, InputEvent, InputState, Textarea, TextareaState},
    message_scroller::{MessageScroller, MessageScrollerState},
    sidebar::{SidebarFooter, SidebarHeader},
    text::TextView,
};
use gpui_kit::prelude::FluentBuilder as _;
use gpui_kit::*;
use serde_json::{Value, json};
use std::{collections::HashMap, net::SocketAddr, rc::Rc, sync::Arc};
use t3_server::Server;

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
struct ChatMessage {
    id: SharedString,
    role: SharedString,
    text: SharedString,
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
    page: Page,
    busy: bool,
    error: Option<SharedString>,
    address: SocketAddr,
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
        Self {
            requests,
            _backend: backend,
            _runtime: runtime,
            _updates: task,
            _subscriptions: subscriptions,
            focus: cx.focus_handle(),
            composer,
            path,
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
            page: Page::Conversation,
            busy: false,
            error: None,
            address,
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
                    self.selected_thread = Some(id);
                    self.selected_project =
                        snapshot["thread"]["projectId"].as_str().map(str::to_owned);
                    self.page = Page::Conversation;
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
                    })
                    .collect::<Vec<_>>();
                let old_count = self.messages.len();
                let count = messages.len();
                self.messages = Rc::new(messages);
                self.detail = snapshot;
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
                ) {
                    if let Some(id) = &self.selected_thread {
                        self.permissions.remove(id);
                    }
                }
            }
            Update::Permission { thread, event } => {
                self.permissions.insert(thread, event);
            }
            Update::Completed { clear_draft } => {
                self.busy = false;
                self.error = None;
                if clear_draft {
                    self.composer
                        .update(cx, |state, cx| state.set_value("", window, cx));
                    if let Some(id) = &self.selected_thread {
                        self.drafts.remove(id);
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
        }
        cx.notify();
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
    fn commands(&mut self, _: &ShowCommands, window: &mut Window, cx: &mut Context<Self>) {
        let palette = self.palette.clone();
        window.open_dialog(cx, move |dialog, _, _| {
            dialog.title("Commands").child(
                Command::new(&palette).items([
                    CommandItem::new()
                        .label("New thread")
                        .action(Box::new(NewThread)),
                    CommandItem::new()
                        .label("Add project…")
                        .action(Box::new(AddProject)),
                    CommandItem::new()
                        .label("Settings…")
                        .action(Box::new(ShowSettings)),
                    CommandItem::new()
                        .label("Stop turn")
                        .action(Box::new(StopTurn)),
                ]),
            )
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
                    .child(TextView::markdown(
                        (ElementId::from("message-content"), message.id.clone()),
                        message.text.clone(),
                    ))
                    .into_any_element()
            })
            .flex_1()
            .min_h_0(),
        );
        if let Some(thread) = &self.selected_thread {
            if let Some(permission) = self.permissions.get(thread) {
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
                            .on_click(cx.listener(
                                move |this, _, _, cx| {
                                    this.permissions.remove(&thread);
                                    this.request(
                                        Request::Permission {
                                            thread: thread.clone(),
                                            id: id.clone(),
                                            option: Some(option_id.clone()),
                                        },
                                        cx,
                                    );
                                },
                            )),
                        )
                    }
                }
                content = content.child(panel);
            }
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
                    .child(Textarea::new(&self.composer))
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
                .child(format!("Local environment: http://{}", self.address))
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
