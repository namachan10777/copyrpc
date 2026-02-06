/// Key-range sharded KV store.
///
/// Each daemon thread owns a `ShardedStore` for its portion of the key space.
/// Shard assignment: `key % num_daemons == daemon_id`.
/// Local index: `key / num_daemons`.
/// No Arc, no atomics — each shard is exclusively accessed by its owning thread.
pub struct ShardedStore {
    data: Vec<u64>,
    num_daemons: u64,
    daemon_id: u64,
}

impl ShardedStore {
    pub fn new(key_range: u64, num_daemons: u64, daemon_id: u64) -> Self {
        // Number of keys this shard owns
        let shard_size = key_range.div_ceil(num_daemons);
        Self {
            data: vec![0u64; shard_size as usize],
            num_daemons,
            daemon_id,
        }
    }

    /// Returns the daemon that owns the given key.
    #[inline]
    pub fn owner_of(key: u64, num_daemons: u64) -> u64 {
        key % num_daemons
    }

    #[inline]
    fn local_index(&self, key: u64) -> usize {
        debug_assert_eq!(key % self.num_daemons, self.daemon_id);
        (key / self.num_daemons) as usize
    }

    #[inline]
    pub fn get(&self, key: u64) -> Option<u64> {
        let idx = self.local_index(key);
        self.data.get(idx).copied()
    }

    #[inline]
    pub fn put(&mut self, key: u64, value: u64) {
        let idx = self.local_index(key);
        if let Some(slot) = self.data.get_mut(idx) {
            *slot = value;
        }
    }
}
