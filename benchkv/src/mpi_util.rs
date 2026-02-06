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
