//! O(1) map using Vec with offset-based indexing.
//!
//! Optimized for keys that are small, monotonically increasing integers
//! such as QPN (Queue Pair Number) in RDMA applications.

/// O(1) map using Vec with offset-based indexing.
///
/// This structure is optimized for keys that tend to be small, contiguous
/// integers. It provides O(1) access by using the key directly as an index
/// (offset by a base value).
///
/// # Performance
///
/// - O(1) get, insert, remove (amortized for insert when resizing)
/// - No hash computation overhead
/// - Cache-friendly for sequential key access
///
/// # Memory
///
/// Memory usage is proportional to the key range (max_key - min_key), not
/// the number of elements. This is efficient when keys are clustered, but
/// wasteful for sparse keys.
pub struct FastMap<V> {
    entries: Vec<Option<V>>,
    base: u32,
    len: usize,
}

impl<V> Default for FastMap<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> FastMap<V> {
    /// Create a new empty FastMap.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            base: 0,
            len: 0,
        }
    }

    /// Insert a key-value pair, returning the old value if present.
    pub fn insert(&mut self, key: u32, value: V) -> Option<V> {
        if self.entries.is_empty() {
            // First insertion: set base to this key
            self.base = key;
            self.entries.push(Some(value));
            self.len = 1;
            return None;
        }

        if key < self.base {
            // Key is before current base: expand front
            let shift = (self.base - key) as usize;
            let new_len = self.entries.len() + shift;
            let mut new_entries = Vec::with_capacity(new_len);

            // Fill with None for the new slots
            new_entries.resize_with(shift, || None);
            // Move existing entries
            new_entries.append(&mut self.entries);

            self.entries = new_entries;
            self.base = key;
            self.entries[0] = Some(value);
            self.len += 1;
            return None;
        }

        let offset = (key - self.base) as usize;

        if offset >= self.entries.len() {
            // Key is beyond current capacity: expand back
            self.entries.resize_with(offset + 1, || None);
        }

        let old = self.entries[offset].take();
        self.entries[offset] = Some(value);

        if old.is_none() {
            self.len += 1;
        }

        old
    }

    /// Get a reference to the value for a key.
    #[inline]
    pub fn get(&self, key: u32) -> Option<&V> {
        if self.entries.is_empty() || key < self.base {
            return None;
        }
        let offset = (key - self.base) as usize;
        self.entries.get(offset).and_then(|v| v.as_ref())
    }

    /// Get a mutable reference to the value for a key.
    #[inline]
    pub fn get_mut(&mut self, key: u32) -> Option<&mut V> {
        if self.entries.is_empty() || key < self.base {
            return None;
        }
        let offset = (key - self.base) as usize;
        self.entries.get_mut(offset).and_then(|v| v.as_mut())
    }

    /// Remove a key-value pair, returning the value if present.
    pub fn remove(&mut self, key: u32) -> Option<V> {
        if self.entries.is_empty() || key < self.base {
            return None;
        }
        let offset = (key - self.base) as usize;
        if offset >= self.entries.len() {
            return None;
        }
        let old = self.entries[offset].take();
        if old.is_some() {
            self.len -= 1;
        }
        old
    }

    /// Check if a key is present.
    #[inline]
    pub fn contains_key(&self, key: u32) -> bool {
        self.get(key).is_some()
    }

    /// Get the number of key-value pairs.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the map is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Iterate over key-value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (u32, &V)> {
        let base = self.base;
        self.entries
            .iter()
            .enumerate()
            .filter_map(move |(i, v)| v.as_ref().map(|v| (base + i as u32, v)))
    }

    /// Iterate over key-value pairs mutably.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (u32, &mut V)> {
        let base = self.base;
        self.entries
            .iter_mut()
            .enumerate()
            .filter_map(move |(i, v)| v.as_mut().map(|v| (base + i as u32, v)))
    }

    /// Iterate over values.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.entries.iter().filter_map(|v| v.as_ref())
    }

    /// Iterate over values mutably.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.entries.iter_mut().filter_map(|v| v.as_mut())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let map: FastMap<i32> = FastMap::new();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
        assert_eq!(map.get(0), None);
        assert_eq!(map.get(100), None);
    }

    #[test]
    fn test_insert_and_get() {
        let mut map = FastMap::new();
        assert_eq!(map.insert(100, "a"), None);
        assert_eq!(map.insert(101, "b"), None);
        assert_eq!(map.insert(102, "c"), None);

        assert_eq!(map.get(100), Some(&"a"));
        assert_eq!(map.get(101), Some(&"b"));
        assert_eq!(map.get(102), Some(&"c"));
        assert_eq!(map.get(99), None);
        assert_eq!(map.get(103), None);

        assert_eq!(map.len(), 3);
    }

    #[test]
    fn test_insert_replace() {
        let mut map = FastMap::new();
        assert_eq!(map.insert(100, "a"), None);
        assert_eq!(map.insert(100, "b"), Some("a"));
        assert_eq!(map.get(100), Some(&"b"));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_insert_before_base() {
        let mut map = FastMap::new();
        assert_eq!(map.insert(100, "a"), None);
        assert_eq!(map.insert(98, "b"), None);
        assert_eq!(map.insert(99, "c"), None);

        assert_eq!(map.get(98), Some(&"b"));
        assert_eq!(map.get(99), Some(&"c"));
        assert_eq!(map.get(100), Some(&"a"));
        assert_eq!(map.len(), 3);
    }

    #[test]
    fn test_remove() {
        let mut map = FastMap::new();
        map.insert(100, "a");
        map.insert(101, "b");

        assert_eq!(map.remove(100), Some("a"));
        assert_eq!(map.get(100), None);
        assert_eq!(map.len(), 1);

        assert_eq!(map.remove(100), None);
        assert_eq!(map.len(), 1);

        assert_eq!(map.remove(999), None);
    }

    #[test]
    fn test_iter() {
        let mut map = FastMap::new();
        map.insert(100, "a");
        map.insert(102, "c");
        map.insert(101, "b");

        let items: Vec<_> = map.iter().collect();
        assert_eq!(items, vec![(100, &"a"), (101, &"b"), (102, &"c")]);
    }

    #[test]
    fn test_values() {
        let mut map = FastMap::new();
        map.insert(100, 1);
        map.insert(102, 3);
        map.insert(101, 2);

        let values: Vec<_> = map.values().collect();
        assert_eq!(values, vec![&1, &2, &3]);
    }

    #[test]
    fn test_sparse_keys() {
        let mut map = FastMap::new();
        map.insert(1000, "a");
        map.insert(1010, "b");

        assert_eq!(map.get(1000), Some(&"a"));
        assert_eq!(map.get(1005), None);
        assert_eq!(map.get(1010), Some(&"b"));
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_get_mut() {
        let mut map = FastMap::new();
        map.insert(100, 1);

        if let Some(v) = map.get_mut(100) {
            *v = 2;
        }

        assert_eq!(map.get(100), Some(&2));
    }

    #[test]
    fn test_contains_key() {
        let mut map = FastMap::new();
        map.insert(100, "a");

        assert!(map.contains_key(100));
        assert!(!map.contains_key(99));
        assert!(!map.contains_key(101));
    }
}
