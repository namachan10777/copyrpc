//! Connection Grouping for NIC cache optimization.
//!
//! ScaleRPC uses connection grouping to limit the number of simultaneously
//! active connections, preventing NIC cache thrashing. Connections are
//! organized into groups, and only a limited number of connections can
//! be active within each group at any time.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::config::GroupConfig;
use crate::error::{Error, Result};

/// A single connection within a group.
#[derive(Debug)]
pub struct GroupConnection {
    /// Connection identifier.
    pub conn_id: usize,
    /// Whether this connection is currently active.
    is_active: std::sync::atomic::AtomicBool,
}

impl GroupConnection {
    /// Create a new connection entry.
    pub fn new(conn_id: usize) -> Self {
        Self {
            conn_id,
            is_active: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Check if this connection is active.
    pub fn is_active(&self) -> bool {
        self.is_active.load(Ordering::Acquire)
    }

    /// Try to activate this connection.
    fn try_activate(&self) -> bool {
        self.is_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Deactivate this connection.
    fn deactivate(&self) {
        self.is_active.store(false, Ordering::Release);
    }
}

/// A group of connections with limited active count.
pub struct ConnectionGroup {
    /// Group identifier.
    group_id: usize,
    /// Connections in this group.
    connections: Vec<GroupConnection>,
    /// Maximum number of active connections.
    max_active: usize,
    /// Current number of active connections.
    active_count: AtomicUsize,
}

impl ConnectionGroup {
    /// Create a new connection group.
    pub fn new(group_id: usize, max_active: usize) -> Self {
        Self {
            group_id,
            connections: Vec::new(),
            max_active,
            active_count: AtomicUsize::new(0),
        }
    }

    /// Get the group ID.
    pub fn group_id(&self) -> usize {
        self.group_id
    }

    /// Get the number of connections in this group.
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    /// Get the current active connection count.
    pub fn active_count(&self) -> usize {
        self.active_count.load(Ordering::Acquire)
    }

    /// Add a connection to this group.
    pub fn add_connection(&mut self, conn_id: usize) {
        self.connections.push(GroupConnection::new(conn_id));
    }

    /// Try to acquire an active slot for a connection.
    ///
    /// Returns `Ok(ConnectionGuard)` if successful, `Err(MaxActiveReached)` otherwise.
    pub fn try_activate(&self, conn_index: usize) -> Result<ConnectionGuard<'_>> {
        if conn_index >= self.connections.len() {
            return Err(Error::ConnectionNotFound(conn_index));
        }

        // Check if we can add another active connection
        loop {
            let current = self.active_count.load(Ordering::Acquire);
            if current >= self.max_active {
                return Err(Error::MaxActiveReached);
            }

            // Try to increment active count
            if self
                .active_count
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
            // Retry if CAS failed
        }

        // Mark the connection as active
        let conn = &self.connections[conn_index];
        if !conn.try_activate() {
            // Connection was already active, decrement count
            self.active_count.fetch_sub(1, Ordering::Release);
            return Err(Error::NoAvailableConnection);
        }

        Ok(ConnectionGuard {
            group: self,
            conn_index,
        })
    }

    /// Get a connection by index.
    pub fn get_connection(&self, index: usize) -> Option<&GroupConnection> {
        self.connections.get(index)
    }

    /// Deactivate a connection (called by ConnectionGuard on drop).
    fn deactivate(&self, conn_index: usize) {
        if let Some(conn) = self.connections.get(conn_index) {
            conn.deactivate();
            self.active_count.fetch_sub(1, Ordering::Release);
        }
    }
}

/// RAII guard for an active connection.
///
/// When dropped, the connection is automatically deactivated.
pub struct ConnectionGuard<'a> {
    group: &'a ConnectionGroup,
    conn_index: usize,
}

impl<'a> ConnectionGuard<'a> {
    /// Get the connection index.
    pub fn conn_index(&self) -> usize {
        self.conn_index
    }

    /// Get the connection ID.
    pub fn conn_id(&self) -> usize {
        self.group.connections[self.conn_index].conn_id
    }

    /// Get the group ID.
    pub fn group_id(&self) -> usize {
        self.group.group_id
    }
}

impl Drop for ConnectionGuard<'_> {
    fn drop(&mut self) {
        self.group.deactivate(self.conn_index);
    }
}

/// Manager for multiple connection groups.
pub struct GroupManager {
    /// Connection groups.
    groups: Vec<ConnectionGroup>,
    /// Configuration.
    config: GroupConfig,
}

impl GroupManager {
    /// Create a new group manager.
    pub fn new(config: &GroupConfig) -> Self {
        let groups = (0..config.num_groups)
            .map(|id| ConnectionGroup::new(id, config.max_active_per_group))
            .collect();

        Self {
            groups,
            config: config.clone(),
        }
    }

    /// Get the number of groups.
    pub fn num_groups(&self) -> usize {
        self.groups.len()
    }

    /// Get a group by ID.
    pub fn get_group(&self, group_id: usize) -> Option<&ConnectionGroup> {
        self.groups.get(group_id)
    }

    /// Get a mutable reference to a group by ID.
    pub fn get_group_mut(&mut self, group_id: usize) -> Option<&mut ConnectionGroup> {
        self.groups.get_mut(group_id)
    }

    /// Add a connection to a group.
    ///
    /// The group is selected based on connection ID using round-robin.
    pub fn add_connection(&mut self, conn_id: usize) -> usize {
        let group_id = conn_id % self.groups.len();
        self.groups[group_id].add_connection(conn_id);
        group_id
    }

    /// Add a connection to a specific group.
    pub fn add_connection_to_group(&mut self, conn_id: usize, group_id: usize) -> Result<()> {
        if group_id >= self.groups.len() {
            return Err(Error::GroupNotFound(group_id));
        }
        self.groups[group_id].add_connection(conn_id);
        Ok(())
    }

    /// Try to activate a connection.
    ///
    /// # Arguments
    /// * `group_id` - The group containing the connection
    /// * `conn_index` - The connection index within the group
    pub fn try_activate(&self, group_id: usize, conn_index: usize) -> Result<ConnectionGuard<'_>> {
        let group = self.groups.get(group_id).ok_or(Error::GroupNotFound(group_id))?;
        group.try_activate(conn_index)
    }

    /// Find a group with available capacity and activate a connection.
    ///
    /// Returns the first available connection across all groups.
    pub fn activate_any(&self) -> Result<ConnectionGuard<'_>> {
        for group in &self.groups {
            for conn_index in 0..group.connections.len() {
                if let Ok(guard) = group.try_activate(conn_index) {
                    return Ok(guard);
                }
            }
        }
        Err(Error::NoAvailableConnection)
    }

    /// Get configuration.
    pub fn config(&self) -> &GroupConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_activation() {
        let mut group = ConnectionGroup::new(0, 2);
        group.add_connection(100);
        group.add_connection(101);
        group.add_connection(102);

        // Activate first two connections
        let guard1 = group.try_activate(0).expect("should activate");
        assert_eq!(guard1.conn_id(), 100);
        assert_eq!(group.active_count(), 1);

        let guard2 = group.try_activate(1).expect("should activate");
        assert_eq!(guard2.conn_id(), 101);
        assert_eq!(group.active_count(), 2);

        // Third should fail (max_active = 2)
        let result = group.try_activate(2);
        assert!(matches!(result, Err(Error::MaxActiveReached)));

        // Drop one guard, then third should succeed
        drop(guard1);
        assert_eq!(group.active_count(), 1);

        let guard3 = group.try_activate(2).expect("should activate after drop");
        assert_eq!(guard3.conn_id(), 102);
        assert_eq!(group.active_count(), 2);
    }

    #[test]
    fn test_group_manager() {
        let config = GroupConfig {
            max_active_per_group: 2,
            num_groups: 4,
            time_slice_us: 100,
            max_endpoint_entries: 256,
        };
        let mut manager = GroupManager::new(&config);

        // Add connections - they should be distributed round-robin
        let g0 = manager.add_connection(0); // group 0
        let g1 = manager.add_connection(1); // group 1
        let g2 = manager.add_connection(2); // group 2
        let g3 = manager.add_connection(3); // group 3
        let g4 = manager.add_connection(4); // group 0

        assert_eq!(g0, 0);
        assert_eq!(g1, 1);
        assert_eq!(g2, 2);
        assert_eq!(g3, 3);
        assert_eq!(g4, 0);

        // Check group 0 has 2 connections
        assert_eq!(manager.get_group(0).unwrap().connection_count(), 2);
    }

    #[test]
    fn test_activate_any() {
        let config = GroupConfig {
            max_active_per_group: 1,
            num_groups: 2,
            time_slice_us: 100,
            max_endpoint_entries: 256,
        };
        let mut manager = GroupManager::new(&config);

        manager.add_connection(0);
        manager.add_connection(1);

        // Activate first available
        let guard1 = manager.activate_any().expect("should find connection");
        let _guard2 = manager.activate_any().expect("should find connection");

        // Now all groups are at max capacity
        let result = manager.activate_any();
        assert!(matches!(result, Err(Error::NoAvailableConnection)));

        // Drop one, should work again
        drop(guard1);
        let _guard3 = manager.activate_any().expect("should work after drop");
    }
}
