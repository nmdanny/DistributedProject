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
    pub fn get(&self, index: usize) -> Option<&LogEntry<V>> {
        self.log.get(index)
    }

    /// Gets all log entries starting with given index
    pub fn get_from(&self, from: usize) -> &[LogEntry<V>] {
        &self.log[from ..]
    }


    pub fn get_from_to(&self, from: usize, to_exclusive: usize) -> &[LogEntry<V>] {
        return &self.log[from .. to_exclusive]
    }

    pub fn len(&self) -> usize {
        return self.log.len();
    }

    /// Inserts a log entry to the end of the log
    pub fn push(&mut self, entry: LogEntry<V>) {
        self.log.push(entry);
    }

    /// Returns the last log index and term of the last entry in the log, if it exists
    pub fn last_log_index_term(&self) -> IndexTerm {
        if self.log.is_empty() {
            return IndexTerm::no_entry();
        }
        let index = self.log.len() - 1;
        IndexTerm::new(index, self.log[index].term)
    }

    /// Finds the index in our log(if it exists) of the first entry whose term
    /// conflicts with one from `entries`, where the first index in `entries`
    /// corresponds to `starting_from` in our log.
    pub fn find_index_of_conflicting_entry(&self,
                                            entries: &Vec<LogEntry<V>>,
                                            starting_from: usize) -> Option<usize> {
        // observe that `entries_indices` are in bounds, but `log_indices` might go out of bounds
        let log_indices = starting_from .. starting_from + entries.len();
        let entries_indices = 0 .. entries.len();

        for (log_ix, entry_ix) in log_indices.zip(entries_indices) {
            match (self.log.get(log_ix), &entries[entry_ix]) {
                (Some(log_entry), entry) if log_entry.term != entry.term =>
                    return Some(log_ix),
                (Some(_), _) => {},
                (None, _) => break
            }
        }
        None
    }

    /// Deletes all log entries after the given index(including)
    pub fn delete_entries(&mut self, starting_from: usize) {
        self.log.drain(starting_from .. );
    }

    /// Appends the given entries to the end of the log
    pub fn append_entries(&mut self, entries: &[LogEntry<V>]) {
        self.log.extend_from_slice(entries);

        assert!(
            entries.iter()
                .zip(entries.iter().skip(1))
                .all(|(e1, e2)| e1.term <= e2.term),
            "log entries must be ordered such that their terms are monotonically increasing"
        );
    }

    /// Given entries and an insertion index, such that `log[insertion_index ..]` might overlap with
    /// `entries`, inserts all elements from `entries` after the overlap.
    ///
    /// **This should only be called after deleting conflicting entries**
    pub fn append_entries_not_in_log(&mut self, entries: &[LogEntry<V>], insertion_index: usize) {
        assert!(insertion_index <= self.log.len(),
                "bad insertion_index(append_entries_not_in_log) - cannot be more than 1 after the end");

        let mut index_with_uniques = 0;

        // TODO use index math to calculate this instead of loop
        for ix in insertion_index .. insertion_index + entries.len() {
            if let Some(val) = self.log.get(ix) {
                // entries with mis-matching terms should've been deleted by now, and so any entries
                // that intersect with the log, should have the same term, thus, by the log matching
                // property, should also have the same value (so in total, same LogEntry)
                assert_eq!(val, &entries[ix - insertion_index], "log matching property");
                index_with_uniques += 1;
            } else {
                break;
            }
        }

        self.append_entries(&entries[index_with_uniques ..]);

    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_conflicting_entry() {

        // note that values in this test don't matter,
        // but in Raft, entries with the same index and term must have
        // the same value (by the log matching property)

        let my_entries = vec![
            LogEntry::new(1337, 0),
            LogEntry::new(1338, 0),
            LogEntry::new(420, 1),
            LogEntry::new(42, 4),
            LogEntry::new(69, 4),
            LogEntry::new(0xCAFE, 8)
        ];
        let storage = InMemoryStorage {
            log: my_entries
        };


        assert!(storage.find_index_of_conflicting_entry(&Vec::new(), 0).is_none());

        let succ_1 = vec![
            LogEntry::new(0xBABE, 0)
        ];

        assert!(storage.find_index_of_conflicting_entry(&succ_1, 0).is_none());
        assert!(storage.find_index_of_conflicting_entry(&succ_1, 1).is_none());

        let succ_2 = vec![
            LogEntry::new(420, 1),
            LogEntry::new(9999, 4)
        ];


        assert!(storage.find_index_of_conflicting_entry(&succ_2, 2).is_none());

        let succ_3  = vec![
            LogEntry::new(2020, 8),
            LogEntry::new(2020, 10)
        ];

        assert!(storage.find_index_of_conflicting_entry(&succ_3, 5).is_none());

        let fail_1 = vec![
            LogEntry::new(0xBABE, 0),
            LogEntry::new(1338, 1),
            LogEntry::new(420, 1),
        ];

        assert_eq!(storage.find_index_of_conflicting_entry(&fail_1, 0), Some(1));

        let fail_2 = vec![
            LogEntry::new(420, 1),
            LogEntry::new(9999, 5)
        ];

        assert_eq!(storage.find_index_of_conflicting_entry(&fail_2, 2), Some(3));


        let fail_3 = vec![
            LogEntry::new(2020, 5),
            LogEntry::new(2020, 10)
        ];

        assert_eq!(storage.find_index_of_conflicting_entry(&fail_3, 5), Some(5));
    }

    #[test]
    fn test_append_entries() {
        let mut storage = InMemoryStorage::<i32>::default();
        storage.append_entries(&vec![
            LogEntry::new(1, 0),
        ]);

        storage.append_entries(&vec![
            LogEntry::new(5, 1),
            LogEntry::new(5, 1),
        ]);

        assert_eq!(storage.log, vec![
            LogEntry::new(1, 0),
            LogEntry::new(5, 1),
            LogEntry::new(5, 1),
        ]);
    }

    #[test]
    #[should_panic]
    fn test_append_entries_panic() {
        let mut storage = InMemoryStorage::<i32>::default();
        storage.append_entries(&vec![
            LogEntry::new(1, 0),
            LogEntry::new(1, 1),
            LogEntry::new(1, 0),
        ]);
    }

    #[test]
    fn test_append_entries_not_in_log() {
        let mut storage = InMemoryStorage::default();
        storage.append_entries_not_in_log(&[
            LogEntry::new(0, 0),
            LogEntry::new(1, 0),
            LogEntry::new(2, 1),
        ], 0);

        // no-op append
        storage.append_entries_not_in_log(&[
            LogEntry::new(1, 0),
            LogEntry::new(2, 1),
        ], 1);

        assert_eq!(storage.log,vec![
            LogEntry::new(0, 0),
            LogEntry::new(1, 0),
            LogEntry::new(2, 1),
        ]);

        // increase size by 1
        storage.append_entries_not_in_log(&[
            LogEntry::new(0, 0),
            LogEntry::new(1, 0),
            LogEntry::new(2, 1),
            LogEntry::new(3, 1),
        ], 0);

        assert_eq!(storage.log,vec![
            LogEntry::new(0, 0),
            LogEntry::new(1, 0),
            LogEntry::new(2, 1),
            LogEntry::new(3, 1),
        ]);

        // no-op append in middle
        storage.append_entries_not_in_log(&[
            LogEntry::new(1, 0),
            LogEntry::new(2, 1),
        ], 1);

        assert_eq!(storage.log,vec![
            LogEntry::new(0, 0),
            LogEntry::new(1, 0),
            LogEntry::new(2, 1),
            LogEntry::new(3, 1),
        ]);

        // append unique elements
        storage.append_entries_not_in_log(&[
            LogEntry::new(3, 1),
            LogEntry::new(4, 2),
        ], 3);

        storage.append_entries_not_in_log(&[
            LogEntry::new(5, 2),
        ], 5);

        // no-op
        storage.append_entries_not_in_log(&[
            LogEntry::new(3, 1),
            LogEntry::new(4, 2),
            LogEntry::new(5, 2),
        ], 3);

        // no-op
        storage.append_entries_not_in_log(&[
            LogEntry::new(3, 1),
            LogEntry::new(4, 2),
        ], 3);

        assert_eq!(storage.log,vec![
            LogEntry::new(0, 0),
            LogEntry::new(1, 0),
            LogEntry::new(2, 1),
            LogEntry::new(3, 1),
            LogEntry::new(4, 2),
            LogEntry::new(5, 2),
        ]);
    }
}