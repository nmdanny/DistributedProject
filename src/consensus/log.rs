use crate::consensus::types::*;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct InMemoryStorage<V: Value>
{
    pub log: BTreeMap<usize, LogEntry<V>>
}

impl <V: Value> Default for InMemoryStorage<V> {
    fn default() -> Self {
        InMemoryStorage {
            log: Default::default()
        }
    }
}


impl <V: Value> InMemoryStorage<V> {
    /// Gets the log entry at given index
    pub fn get(&self, index: &usize) -> Option<&LogEntry<V>> {
        assert!(*index > 0, "storage get() - index must be bigger than 0");
        self.log.get(index)
    }

    /// Inserts a log entry into given index, returns the previous log entry(if exists)
    pub fn insert(&mut self, index: usize, entry: impl Into<LogEntry<V>>) -> Option<LogEntry<V>> {
        assert!(*index > 0, "storage insert() - index must be bigger than 0");
        self.log.insert(*&index, entry.into())
    }

    pub fn remove(&mut self, index: &usize) -> Option<LogEntry<V>> {
        self.log.remove(index)
    }

    /// Index of last entry in the log
    pub fn last_log_index(&self) -> usize {
        assert!(!self.log.contains_key(&0));
        *self.log.keys().last().unwrap_or(&0)
    }

    /// Term of the last entry in the log
    pub fn last_log_term(&self) -> usize {
        self.log.get(&self.last_log_index()).map(|e| e.term).unwrap_or(0)
    }
}
