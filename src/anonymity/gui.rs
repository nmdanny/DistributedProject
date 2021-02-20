use std::{cell::RefCell, collections::{HashMap, HashSet}, hash::{Hash, Hasher}, ops::Sub, pin::Pin, sync::Arc};

use dist_lib::{anonymity::{anonymous_client::{AnonymousClient, CommitResult, combined_subscriber}, logic::{AnonymityMessage, Config, NewRound, ReconstructionResults}}, consensus::{client::{ClientTransport, EventStream}, types::RaftError}, grpc::transport::{GRPCConfig, GRPCTransport}};
use futures::{Stream, StreamExt, stream::BoxStream};
use iced::{Application, Background, Button, Color, Column, Command, Container, Element, Font, Length, Scrollable, Subscription, Text, TextInput, button, executor, keyboard::Event, scrollable};
use iced::text_input;

use derivative;

use crate::ClientConfig;


#[derive(Debug, Clone)]
/// Status of a chat message
enum ChatMessageState {
    /// Sent but not yet recovered
    Sent,
    /// Sent and recovered by servers
    Delivered(CommitResult),
    /// Tried sending but got an error
    Errored(String),
    /// A message from another party(which was recovered by the servers)
    Received(CommitResult)
}

impl iced::container::StyleSheet for ChatMessageState {
    fn style(&self) -> iced::container::Style {

        let my_message_color : Color = Color::from_rgb8(239, 255, 250);
        let other_message_color : Color = Color::WHITE;
        let error_message_color : Color = Color::from_rgb8(250, 212, 216);
        let background_color = match &self {
                ChatMessageState::Sent | ChatMessageState::Delivered(_) => { my_message_color }
                ChatMessageState::Errored(_) => { error_message_color}
                ChatMessageState::Received(_) => { other_message_color }
        };
        iced::container::Style {
            background: background_color.into(),
            text_color: Color::BLACK.into(),
            border_radius: 0.0,
            border_width: 1.0,
            border_color: Color::BLACK.into(),
            .. iced::container::Style::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChatMessage {
    contents: String,
    state: ChatMessageState
}

impl ChatMessage {
    fn view(&mut self) -> Element<Message> {
        let contents = format!("[{:?}] {}", self.state, self.contents);

        let contents = match &self.state {
            ChatMessageState::Sent => { contents }
            ChatMessageState::Delivered(commit_result) => { format!("{} √\n{:?}", contents, commit_result)}
            ChatMessageState::Errored(err) => { format!("{}\nError: {}", contents, err)}
            ChatMessageState::Received(commit_result) => { format!("{}\n{:?}", contents, commit_result) }
        };
        let text = Text::new(contents);
        let container = Container::new(text).width(Length::Fill).padding(1).style(self.state.clone());
        container.into()
    }
}

struct ChatState {
    messages: Vec<ChatMessage>,
    send: button::State,
    input_msg: String,
    input: text_input::State,
    scrollable_state: scrollable::State,
    client: AnonymousClient<String>,
    round: usize,
    event_stream: RefCell<Option<EventStream<NewRound<String>>>>

}


#[derive(Debug, Clone)]
pub enum Message {
    SendChatMessage,
    SendChatMessageResult(usize, Result<CommitResult, String>),
    InputMessageChanged(String),
    NewRoundEvent(NewRound<String>)
}

const MSG_FONT_SIZE: u16 = 14;

impl ChatState {
    pub fn new(client: AnonymousClient<String>) -> Self {
        let event_stream = client.event_stream().expect("AnonymousClient should have event_stream at first call");
        let event_stream = RefCell::new(Some(event_stream));

        ChatState {
            messages: vec![],
            send: Default::default(),
            input_msg: String::new(),
            input: Default::default(),
            scrollable_state: Default::default(),
            client,
            round: 0,
            event_stream
        }
    }

    pub fn update(&mut self, message: Message) -> Command<Message> {
        match message {
            Message::SendChatMessage => {
                let mut message = String::new();
                std::mem::swap(&mut self.input_msg, &mut message);

                self.messages.push(ChatMessage {
                    contents: message.clone(),
                    state: ChatMessageState::Sent
                });
                let message_ix = self.messages.len() - 1;
                let fut = self.client.send_anonymously(message);
                let cmd = Command::from(async move {
                    let res = fut.await
                        .map_err(|e| format!("{:?}", e));
                    Message::SendChatMessageResult(message_ix, res)
                });
                return cmd;
            }
            Message::InputMessageChanged(new_input) => {
                self.input_msg = new_input
            }
            Message::NewRoundEvent(new_round) => {
                self.round = new_round.round;

                if let ReconstructionResults::Some { chan_to_val } = new_round.last_reconstruct_results {
                    let round = self.round - 1;

                    for (channel, res) in chan_to_val.into_iter() {
                        let commit_res = CommitResult {
                            channel, round
                        };
                        let contents = res.expect("SM event channel shouldn't transmit collision entries");
                        self.messages.push(ChatMessage {
                            contents, state: ChatMessageState::Received(commit_res)
                        });
                    }

                }
            }
            Message::SendChatMessageResult(message_ix, res) => {
                self.messages[message_ix].state = match res {
                    Ok(commit_res) => {
                        ChatMessageState::Delivered(commit_res)
                    }
                    Err(err_msg) => ChatMessageState::Errored(err_msg)
                }
            }
        }
        Command::none()
    }

    pub fn view(&mut self) -> Element<Message> {
        let messages = self.messages.iter_mut().map(ChatMessage::view);
        let scrollable_msgs = messages.fold(Scrollable::new(&mut self.scrollable_state),
            |scrollable, message| scrollable.push(message)
        );

        let container = Container::new(scrollable_msgs).height(Length::Fill).width(Length::Fill);

        Column::new()
            .push(Text::new(format!("Round {}", self.round)))
            .push(container)
            .push(TextInput::new(&mut self.input, "", &self.input_msg, |s| Message::InputMessageChanged(s) ))
            .push(
                Button::new(&mut self.send, Text::new("Send"))
                .on_press(Message::SendChatMessage)
            )
            .into()
    }

    fn subscription(&self) -> Subscription<Message> {
        let stream = self.event_stream.borrow_mut().take()
            .map(|stream| stream.map(Message::NewRoundEvent));
        Subscription::from_recipe(StreamRecipe {
            stream
        })
    }
}



struct StreamRecipe<T, S: Send + Stream<Item = T>> {
    stream: Option<S>
}

impl <H: Hasher, I, T: 'static, S: 'static + Send + Stream<Item = T>> iced_futures::subscription::Recipe<H, I> for StreamRecipe<T, S> {
   type Output = T;

   fn hash(&self, state: &mut H) {
        use std::hash::Hash;
        struct Marker;
        std::any::TypeId::of::<Marker>().hash(state);
    }

   fn stream(
        self: Box<Self>,
        _input: iced_futures::BoxStream<I>,
    ) -> iced_futures::BoxStream<Self::Output> {
        Box::pin(self.stream.expect("StreamRecipe::stream should only be called once"))
    }
}


pub struct AppFlags {
    pub config: Config,
    pub client_id: usize
}


#[derive(Debug)]
pub enum AppMessage {
    Message(Message),
    InitComplete(AnonymousClient<String>)
}

enum AppState {
    Initializing,
    Initialized {
        chat_state: ChatState
    }
}

pub struct App {
    state: AppState,
    title: String,
}

impl Application for App {
    type Executor = executor::Default;
    type Message = AppMessage;
    type Flags = AppFlags;


    fn new(flags: Self::Flags) -> (Self, Command<Self::Message>) {
        let title = format!("Client {:?}", flags.client_id);
        let cmd = Command::from(async move {
            info!("cmd starting");
            let grpc_config = GRPCConfig::default_for_nodes(flags.config.num_nodes);
            let shared_cfg = Arc::new(flags.config.clone());
            let transport: GRPCTransport<AnonymityMessage<String>> = GRPCTransport::new(None, grpc_config).await.unwrap();
            let sm_events = futures::future::join_all((0 .. shared_cfg.num_nodes).map(|node_id| {
                let stream = transport.get_sm_event_stream::<NewRound<String>>(node_id);
                async move {
                    stream.await.unwrap()
                }
                
            })).await;

            let recv = combined_subscriber(sm_events.into_iter());
            let anonym = AnonymousClient::new(transport, shared_cfg, flags.client_id, recv);
            info!("Anonym client created");
            AppMessage::InitComplete(anonym)
        });
        (App {
            state: AppState::Initializing,
            title
        }, cmd)
    }

    fn title(&self) -> String {
        self.title.clone()
    }

    fn update(&mut self, message: Self::Message) -> Command<Self::Message> {
        match message {
            AppMessage::Message(message) => { 
                if let AppState::Initialized { chat_state} = &mut self.state {
                    chat_state.update(message).map(AppMessage::Message) 
                } else {
                    panic!("Got message in wrong stage");
                }
            },
            AppMessage::InitComplete(client) => { 
                self.state = AppState::Initialized { chat_state: ChatState::new(client) };
                Command::none()
            }
        }
    }

    fn view(&mut self) -> Element<Self::Message> {
        match &mut self.state {
            AppState::Initializing => { Text::new("Initializing").into() }
            AppState::Initialized { chat_state } => { chat_state.view().map(AppMessage::Message)}
        }
    }

    fn subscription(&self) -> Subscription<Self::Message> {
        match &self.state {
            AppState::Initializing => { Subscription::none() }
            AppState::Initialized { chat_state } => { chat_state.subscription().map(AppMessage::Message)}
        }
    }
}