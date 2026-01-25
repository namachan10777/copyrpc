//! Virtualized Mapping for shared message pools.
//!
//! This module implements the virtualized mapping table that binds
//! connections to message slots. The mapping allows multiple connections
//! to share a common message pool while maintaining isolation.

use std::collections::HashMap;

use crate::error::{Error, Result};

/// Entry in the virtual mapping table.
#[derive(Debug, Clone)]
pub struct MappingEntry {
    /// Connection ID.
    pub conn_id: usize,
    /// Assigned slot indices for this connection.
    pub slots: Vec<usize>,
    /// Remote slot address (server-side).
    pub remote_slot_addr: u64,
    /// Remote slot rkey.
    pub remote_slot_rkey: u32,
}

impl MappingEntry {
    /// Create a new mapping entry.
    pub fn new(conn_id: usize) -> Self {
        Self {
            conn_id,
            slots: Vec::new(),
            remote_slot_addr: 0,
            remote_slot_rkey: 0,
        }
    }

    /// Set remote slot information.
    pub fn set_remote(&mut self, addr: u64, rkey: u32) {
        self.remote_slot_addr = addr;
        self.remote_slot_rkey = rkey;
    }

    /// Add a slot to this connection's mapping.
    pub fn add_slot(&mut self, slot_index: usize) {
        self.slots.push(slot_index);
    }

    /// Remove a slot from this connection's mapping.
    pub fn remove_slot(&mut self, slot_index: usize) -> bool {
        if let Some(pos) = self.slots.iter().position(|&s| s == slot_index) {
            self.slots.remove(pos);
            true
        } else {
            false
        }
    }
}

/// Virtual mapping table for connections and slots.
///
/// The mapping table maintains the relationship between:
/// - Connection IDs and their assigned slots
/// - Slot indices and their owning connections
pub struct VirtualMapping {
    /// Connection ID to mapping entry.
    conn_to_entry: HashMap<usize, MappingEntry>,
    /// Slot index to owning connection ID.
    slot_to_conn: HashMap<usize, usize>,
}

impl VirtualMapping {
    /// Create a new empty virtual mapping.
    pub fn new() -> Self {
        Self {
            conn_to_entry: HashMap::new(),
            slot_to_conn: HashMap::new(),
        }
    }

    /// Register a new connection.
    pub fn register_connection(&mut self, conn_id: usize) {
        self.conn_to_entry.insert(conn_id, MappingEntry::new(conn_id));
    }

    /// Unregister a connection and release all its slots.
    pub fn unregister_connection(&mut self, conn_id: usize) -> Option<MappingEntry> {
        if let Some(entry) = self.conn_to_entry.remove(&conn_id) {
            // Remove all slot mappings for this connection
            for slot in &entry.slots {
                self.slot_to_conn.remove(slot);
            }
            Some(entry)
        } else {
            None
        }
    }

    /// Get a connection's mapping entry.
    pub fn get_connection(&self, conn_id: usize) -> Option<&MappingEntry> {
        self.conn_to_entry.get(&conn_id)
    }

    /// Get a mutable reference to a connection's mapping entry.
    pub fn get_connection_mut(&mut self, conn_id: usize) -> Option<&mut MappingEntry> {
        self.conn_to_entry.get_mut(&conn_id)
    }

    /// Bind a slot to a connection.
    pub fn bind_slot(&mut self, conn_id: usize, slot_index: usize) -> Result<()> {
        // Check if slot is already bound
        if self.slot_to_conn.contains_key(&slot_index) {
            return Err(Error::InvalidSlotIndex(slot_index));
        }

        // Get the connection entry
        let entry = self
            .conn_to_entry
            .get_mut(&conn_id)
            .ok_or(Error::ConnectionNotFound(conn_id))?;

        // Add the binding
        entry.add_slot(slot_index);
        self.slot_to_conn.insert(slot_index, conn_id);

        Ok(())
    }

    /// Unbind a slot from its connection.
    pub fn unbind_slot(&mut self, slot_index: usize) -> Option<usize> {
        if let Some(conn_id) = self.slot_to_conn.remove(&slot_index) {
            if let Some(entry) = self.conn_to_entry.get_mut(&conn_id) {
                entry.remove_slot(slot_index);
            }
            Some(conn_id)
        } else {
            None
        }
    }

    /// Get the connection ID that owns a slot.
    pub fn get_slot_owner(&self, slot_index: usize) -> Option<usize> {
        self.slot_to_conn.get(&slot_index).copied()
    }

    /// Set remote slot information for a connection.
    pub fn set_remote_slot(
        &mut self,
        conn_id: usize,
        addr: u64,
        rkey: u32,
    ) -> Result<()> {
        let entry = self
            .conn_to_entry
            .get_mut(&conn_id)
            .ok_or(Error::ConnectionNotFound(conn_id))?;
        entry.set_remote(addr, rkey);
        Ok(())
    }

    /// Get the number of registered connections.
    pub fn connection_count(&self) -> usize {
        self.conn_to_entry.len()
    }

    /// Get the number of bound slots.
    pub fn bound_slot_count(&self) -> usize {
        self.slot_to_conn.len()
    }

    /// Iterate over all connections.
    pub fn connections(&self) -> impl Iterator<Item = &MappingEntry> {
        self.conn_to_entry.values()
    }
}

impl Default for VirtualMapping {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mapping_basic() {
        let mut mapping = VirtualMapping::new();

        // Register connections
        mapping.register_connection(1);
        mapping.register_connection(2);
        assert_eq!(mapping.connection_count(), 2);

        // Bind slots
        mapping.bind_slot(1, 0).unwrap();
        mapping.bind_slot(1, 1).unwrap();
        mapping.bind_slot(2, 2).unwrap();
        assert_eq!(mapping.bound_slot_count(), 3);

        // Check ownership
        assert_eq!(mapping.get_slot_owner(0), Some(1));
        assert_eq!(mapping.get_slot_owner(1), Some(1));
        assert_eq!(mapping.get_slot_owner(2), Some(2));
        assert_eq!(mapping.get_slot_owner(3), None);

        // Check connection entries
        let entry1 = mapping.get_connection(1).unwrap();
        assert_eq!(entry1.slots, vec![0, 1]);
    }

    #[test]
    fn test_mapping_unbind() {
        let mut mapping = VirtualMapping::new();
        mapping.register_connection(1);
        mapping.bind_slot(1, 0).unwrap();
        mapping.bind_slot(1, 1).unwrap();

        // Unbind slot
        let owner = mapping.unbind_slot(0);
        assert_eq!(owner, Some(1));
        assert_eq!(mapping.get_slot_owner(0), None);

        // Check connection entry updated
        let entry = mapping.get_connection(1).unwrap();
        assert_eq!(entry.slots, vec![1]);
    }

    #[test]
    fn test_mapping_unregister() {
        let mut mapping = VirtualMapping::new();
        mapping.register_connection(1);
        mapping.bind_slot(1, 0).unwrap();
        mapping.bind_slot(1, 1).unwrap();

        // Unregister connection
        let entry = mapping.unregister_connection(1);
        assert!(entry.is_some());
        assert_eq!(mapping.connection_count(), 0);
        assert_eq!(mapping.bound_slot_count(), 0);
    }

    #[test]
    fn test_mapping_double_bind_fails() {
        let mut mapping = VirtualMapping::new();
        mapping.register_connection(1);
        mapping.register_connection(2);

        mapping.bind_slot(1, 0).unwrap();

        // Should fail - slot already bound
        let result = mapping.bind_slot(2, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_remote_slot() {
        let mut mapping = VirtualMapping::new();
        mapping.register_connection(1);
        mapping.set_remote_slot(1, 0x1000, 0xABCD).unwrap();

        let entry = mapping.get_connection(1).unwrap();
        assert_eq!(entry.remote_slot_addr, 0x1000);
        assert_eq!(entry.remote_slot_rkey, 0xABCD);
    }
}
