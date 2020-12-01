use crate::consensus::types::*;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct InMemoryStorage<V: Value>
{
    log: Vec<LogEntry<V>>
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
        self.log.get(*index)
    }

    /// Inserts a log entry into given index, returns the previous log entry(if exists)
    pub fn insert(&mut self, index: usize, entry: impl Into<LogEntry<V>>) -> Option<LogEntry<V>> {
        let prev = self.log.remove(index);
        self.log.insert(index, entry.into());
        unimplemented!()
    }

    pub fn remove(&mut self, index: &usize) -> Option<LogEntry<V>> {
        unimplemented!()
    }


    /// Returns the last log index and term of the last entry in the log, if it exists
    pub fn last_log_index_term(&self) -> IndexTerm {
        if self.log.is_empty() {
            return IndexTerm::no_entry();
        }
        let index = self.log.len() - 1;
        IndexTerm::new(index, self.log[index].term)
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_index_term() {
        let none = IndexTerm::no_entry();
    }
}