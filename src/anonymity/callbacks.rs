use lazy_static::lazy_static;
use std::{hash::Hash, sync::{Arc}};
use crate::consensus::client::Client;
use crate::consensus::types::*;
use crate::anonymity::anonymous_client::AnonymousClient;
use parking_lot::RwLock;
use std::cell::RefCell;


struct CallbackManager {

    pub client_callbacks: Vec<Box<dyn FnMut(&str, usize, Option<Id>)>>
}

impl CallbackManager {
    fn new() -> Self {
        CallbackManager {
            client_callbacks: Vec::new()
        }
    }

    fn on_anonym_client_send(&mut self, client_name: &str, round: usize, node_id: Option<Id>) {
        for cb in self.client_callbacks.iter_mut() {
            cb(client_name, round, node_id);
        }
    }

    fn register_client_send_callback(&mut self, callback: Box<dyn FnMut(&str, usize, Option<Id>)>) {
        self.client_callbacks.push(callback);
    }
}

type VType = u64;

std::thread_local! {
    static CALLBACKS: RefCell<CallbackManager> = RefCell::new(CallbackManager::new());
}

pub fn register_client_send_callback(callback: Box<dyn FnMut(&str, usize, Option<Id>)>) {
    CALLBACKS.with(|cb| {
        cb.borrow_mut().register_client_send_callback(callback);
    })
}

/// Invoked with the node_id when sending a share, or None when submitting liveness
pub fn on_anonym_client_send(client_name: &str, round: usize, node_id: Option<Id>) {
    CALLBACKS.with(|cb| {
        cb.borrow_mut().on_anonym_client_send(client_name, round, node_id);
    })
}