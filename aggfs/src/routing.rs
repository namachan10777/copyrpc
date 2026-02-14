//! Routing and hashing for aggfs.
//!
//! Uses FINCHFS's number-aware hashing (koyama hash) to ensure
//! sequential filenames are distributed evenly across daemons.

/// Number-aware hash (FINCHFS koyama hash).
///
/// Numeric substrings are added as their integer value rather than
/// as individual characters. This ensures that e.g. "file001" through
/// "file100" distribute evenly instead of clustering.
pub fn koyama_hash(s: &[u8]) -> u64 {
    let mut hash: u64 = 0;
    let mut i = 0;
    while i < s.len() {
        if s[i].is_ascii_digit() {
            // Parse the numeric substring
            let mut num: u64 = 0;
            while i < s.len() && s[i].is_ascii_digit() {
                num = num.wrapping_mul(10).wrapping_add((s[i] - b'0') as u64);
                i += 1;
            }
            hash = hash.wrapping_add(num);
        } else {
            hash = hash.wrapping_mul(31).wrapping_add(s[i] as u64);
            i += 1;
        }
    }
    hash
}

/// Determine which daemon (global ID) owns the given chunk.
pub fn route(path_hash: u64, chunk_index: u32, total_daemons: usize) -> usize {
    ((path_hash as usize).wrapping_add(chunk_index as usize)) % total_daemons
}

/// Convert a global daemon ID to (node_rank, daemon_index_on_node).
pub fn global_to_local(global_id: usize, daemons_per_node: usize) -> (u32, usize) {
    let node = (global_id / daemons_per_node) as u32;
    let local = global_id % daemons_per_node;
    (node, local)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_koyama_hash_sequential_files() {
        // Sequential files should have different hashes
        let h1 = koyama_hash(b"file001");
        let h2 = koyama_hash(b"file002");
        let h3 = koyama_hash(b"file003");
        assert_ne!(h1, h2);
        assert_ne!(h2, h3);
    }

    #[test]
    fn test_koyama_hash_number_aware() {
        // Numbers are parsed as integers
        let h10 = koyama_hash(b"file10");
        let h9 = koyama_hash(b"file9");
        // file10 and file9 should differ by 1 in the numeric component
        assert_ne!(h10, h9);
    }

    #[test]
    fn test_koyama_hash_distribution() {
        // Check that sequential filenames distribute across 4 daemons
        let mut counts = [0u32; 4];
        for i in 0..100 {
            let name = format!("data_{:04}", i);
            let h = koyama_hash(name.as_bytes());
            let daemon = route(h, 0, 4);
            counts[daemon] += 1;
        }
        // Each daemon should get at least some files
        for count in &counts {
            assert!(*count > 10, "Poor distribution: {:?}", counts);
        }
    }

    #[test]
    fn test_route() {
        assert_eq!(route(10, 0, 4), 2);
        assert_eq!(route(10, 1, 4), 3);
        assert_eq!(route(10, 2, 4), 0);
        assert_eq!(route(10, 3, 4), 1);
    }

    #[test]
    fn test_global_to_local() {
        assert_eq!(global_to_local(0, 2), (0, 0));
        assert_eq!(global_to_local(1, 2), (0, 1));
        assert_eq!(global_to_local(2, 2), (1, 0));
        assert_eq!(global_to_local(5, 3), (1, 2));
    }
}
