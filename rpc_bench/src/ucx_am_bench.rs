use crate::parquet_out::BenchRow;
use crate::{CommonConfig, ModeCmd};

pub fn run(
    _common: &CommonConfig,
    world: &mpi::topology::SimpleCommunicator,
    _mode: &ModeCmd,
) -> Vec<BenchRow> {
    use mpi::topology::Communicator;
    if world.rank() == 0 {
        eprintln!("ucx-am: not available (libucx-dev not installed)");
    }
    Vec::new()
}
