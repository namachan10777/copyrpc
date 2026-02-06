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

/// Exchange variable-length bytes. Sends length first, then data.
#[allow(dead_code)]
pub fn exchange_bytes_variable(
    world: &mpi::topology::SimpleCommunicator,
    rank: i32,
    peer: i32,
    local: &[u8],
) -> Vec<u8> {
    let local_len = local.len() as u64;
    let mut remote_len = 0u64;

    if rank < peer {
        world
            .process_at_rank(peer)
            .send(std::slice::from_ref(&local_len));
        world
            .process_at_rank(peer)
            .receive_into(std::slice::from_mut(&mut remote_len));
    } else {
        world
            .process_at_rank(peer)
            .receive_into(std::slice::from_mut(&mut remote_len));
        world
            .process_at_rank(peer)
            .send(std::slice::from_ref(&local_len));
    }

    let mut remote = vec![0u8; remote_len as usize];
    if rank < peer {
        world.process_at_rank(peer).send(local);
        world.process_at_rank(peer).receive_into(&mut remote);
    } else {
        world.process_at_rank(peer).receive_into(&mut remote);
        world.process_at_rank(peer).send(local);
    }
    remote
}
