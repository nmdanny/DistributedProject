use std::{cell::RefCell, hash::{Hash, Hasher}, ops::Sub, pin::Pin, sync::Arc};

use dist_lib::{anonymity::{anonymous_client::{AnonymousClient, CommitResult, combined_subscriber}, logic::{AnonymityMessage, Config, NewRound}}, consensus::{client::{ClientTransport, EventStream}, types::RaftError}, grpc::transport::{GRPCConfig, GRPCTransport}};
use futures::{Stream, StreamExt, stream::BoxStream};
use iced::{Application, Button, Column, Command, Element, Subscription, Text, TextInput, button, executor};
use iced::text_input;

use derivative;

use crate::ClientConfig;

struct ChatState {
    messages: Vec<String>,
    send: button::State,
    input_msg: String,
    input: text_input::State,
    client: AnonymousClient<String>,

    sub: StreamRecipe<Message>

}


#[derive(Debug, Clone)]
pub enum Message {
    SendChatMessage,
    SendChatMessageResult(Result<CommitResult, String>),
    ReceiveChatMessage(String),
    InputMessageChanged(String),
    NewRoundEvent(NewRound<String>)
}

const MSG_FONT_SIZE: u16 = 14;

impl ChatState {
    pub fn new(client: AnonymousClient<String>) -> Self {
        let event_stream = client.event_stream().expect("AnonymousClient should have event_stream at first call");
        let event_stream = Box::pin(event_stream.map(|ev| {
            info!("Got NewRound {:?}", ev);
            Message::NewRoundEvent(ev)
        }));
        let sub = StreamRecipe {
            stream: event_stream
        };

        ChatState {
            messages: vec![],
            send: Default::default(),
            input_msg: String::new(),
            input: Default::default(),
            client,
            sub
        }
    }

    pub fn update(&mut self, message: Message) -> Command<Message> {
        match message {
            Message::SendChatMessage => {
                let mut message = String::new();
                std::mem::swap(&mut self.input_msg, &mut message);
                self.messages.push(message.clone());
                let fut = self.client.send_anonymously(message);
                let cmd = Command::from(async move {
                    let res = fut.await
                        .map_err(|e| format!("{:?}", e));
                    Message::SendChatMessageResult(res)
                });
                return cmd;
            }
            Message::ReceiveChatMessage(new_message) => {
                self.messages.push(new_message)
            }
            Message::InputMessageChanged(new_input) => {
                self.input_msg = new_input
            }
            Message::NewRoundEvent(new_round) => {
                self.messages.push(format!("{:?}\n", new_round));
            }
            Message::SendChatMessageResult(res) => {
                self.messages.push(format!("{:?}\n", res));
            }
        }
        Command::none()
    }

    pub fn view(&mut self) -> Element<Message> {
        let messages = self.messages.join("\n");
        Column::new()
            .push(Text::new(messages))
            .push(
                TextInput::new(&mut self.input, "", &self.input_msg,
                |s| Message::InputMessageChanged(s) )
            )
            .push(
                Button::new(&mut self.send, Text::new("Send"))
                .on_press(Message::SendChatMessage)
            )
            .into()
    }

    fn subscription(&self) -> Subscription<Message> {
        // Subscription::from_recipe(self.sub)
        Subscription::none()
    }
}



struct StreamRecipe<S> {
    stream: Pin<Box<dyn Send + Stream<Item = S>>>
}

impl <H: Hasher, I, S: 'static> iced_futures::subscription::Recipe<H, I> for StreamRecipe<S> {
   type Output = S;

   fn hash(&self, state: &mut H) {
        use std::hash::Hash;

        std::any::TypeId::of::<Self>().hash(state);

        // self.client.client_name().hash(state);
    }

   fn stream(
        self: Box<Self>,
        _input: iced_futures::BoxStream<I>,
    ) -> iced_futures::BoxStream<Self::Output> {
        self.stream
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
            let anonym = AnonymousClient::new(transport, shared_cfg, format!("Client {}", flags.client_id), recv);
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