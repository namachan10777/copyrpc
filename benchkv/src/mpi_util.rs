use mpi::collective::CommunicatorCollectives;
use mpi::point_to_point::{Destination, Source};
use mpi::topology::Communicator;

/// Exchange bytes between two ranks via MPI point-to-point.
pub fn exchange_bytes(
    world: &mpi::topology::SimpleCommunicator,
    rank: i32,
    peer: i32,
    local: &[u8],
) -> Vec<u8> {
    let mut remote = vec![0u8; local.len()];
    if rank < peer {
        world.process_at_rank(peer).send(local);
        world.process_at_rank(peer).receive_into(&mut remote);
    } else {
        world.process_at_rank(peer).receive_into(&mut remote);
        world.process_at_rank(peer).send(local);
    }
    remote
}

/// Returns (ranks_on_this_node, rank_index_on_this_node) by exchanging hostnames.
pub fn node_local_rank(world: &mpi::topology::SimpleCommunicator) -> (u32, u32) {
    let size = world.size() as usize;
    let rank = world.rank() as usize;

    let mut hostname_buf = [0u8; 64];
    let hostname = gethostname::gethostname();
    let hostname_bytes = hostname.as_encoded_bytes();
    let len = hostname_bytes.len().min(64);
    hostname_buf[..len].copy_from_slice(&hostname_bytes[..len]);

    let mut all_hostnames = vec![0u8; 64 * size];
    world.all_gather_into(&hostname_buf[..], &mut all_hostnames[..]);

    let my_host = &all_hostnames[rank * 64..(rank + 1) * 64];
    let mut ranks_on_node = 0u32;
    let mut rank_on_node = 0u32;
    for r in 0..size {
        let other = &all_hostnames[r * 64..(r + 1) * 64];
        if other == my_host {
            if r < rank {
                rank_on_node += 1;
            }
            ranks_on_node += 1;
        }
    }

    (ranks_on_node, rank_on_node)
}
