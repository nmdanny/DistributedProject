use iced::{Application, Button, Column, Command, Element, Text, TextInput, button, executor};
use iced::text_input;

#[derive(Debug, Default, Clone)]
struct ChatState {
    messages: Vec<String>,
    send: button::State,
    input_msg: String,
    input: text_input::State,
}


#[derive(Debug, Clone)]
pub enum Message {
    SendMessage,
    ChatMessage(String),
    InputMessageChanged(String)
}

const MSG_FONT_SIZE: u16 = 14;

impl ChatState {
    pub fn new() -> Self {
        ChatState::default()
    }

    pub fn update(&mut self, message: Message) -> Command<Message> {
        match message {
            Message::SendMessage => {
                let mut message = String::new();
                std::mem::swap(&mut self.input_msg, &mut message);
                self.messages.push(message);
            }
            Message::ChatMessage(new_message) => {
                self.messages.push(new_message)
            }
            Message::InputMessageChanged(new_input) => {
                self.input_msg = new_input
            }
        }
        Command::none()
    }

    pub fn view(&mut self) -> Column<Message> {
        let messages = self.messages.join("\n");
        Column::new()
            .push(Text::new(messages))
            .push(
                TextInput::new(&mut self.input, "", &self.input_msg,
                |s| Message::InputMessageChanged(s) )
            )
            .push(
                Button::new(&mut self.send, Text::new("Send"))
                .on_press(Message::SendMessage)
            )
    }
}

#[derive(Debug, Default)]
pub struct App {
    chat_state: ChatState
}

impl Application for App {
    type Executor = executor::Default;
    type Message = Message;
    type Flags = ();


    fn new(_flags: ()) -> (Self, Command<Self::Message>) {
        (Self::default(), Command::none())
    }

    fn title(&self) -> String {
        String::from("Anonymous Chat")
    }

    fn update(&mut self, message: Self::Message) -> Command<Self::Message> {
        self.chat_state.update(message)
    }

    fn view(&mut self) -> Element<Self::Message> {
        self.chat_state.view().into()
    }
}