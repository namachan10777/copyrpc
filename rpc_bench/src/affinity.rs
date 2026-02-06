use crate::AffinityMode;

/// Compute the CPU core ID for a given thread, assigning downward from `start_core`.
///
/// - `multinode`:  core = start_core - thread_index
/// - `singlenode`: core = start_core - (local_rank * threads_per_rank + thread_index)
fn core_for_thread(
    mode: AffinityMode,
    start_core: usize,
    local_rank: usize,
    threads_per_rank: usize,
    thread_index: usize,
) -> usize {
    let offset = match mode {
        AffinityMode::Multinode => thread_index,
        AffinityMode::Singlenode => local_rank * threads_per_rank + thread_index,
    };
    assert!(
        offset <= start_core,
        "Affinity error: need core offset {} but start_core is {} \
         (local_rank={}, threads_per_rank={}, thread_index={})",
        offset, start_core, local_rank, threads_per_rank, thread_index
    );
    start_core - offset
}

/// Get the local rank of this process on the node.
/// Uses OMPI_COMM_WORLD_LOCAL_RANK (OpenMPI), falling back to global MPI rank.
fn get_local_rank(mpi_rank: i32) -> usize {
    if let Ok(val) = std::env::var("OMPI_COMM_WORLD_LOCAL_RANK")
        && let Ok(lr) = val.parse::<usize>() {
            return lr;
        }
    mpi_rank as usize
}

fn num_online_cores() -> usize {
    unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) as usize }
}

fn pin_to_core(core_id: usize) -> Result<(), i32> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core_id, &mut set);
        let ret = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        if ret == 0 {
            Ok(())
        } else {
            Err(*libc::__errno_location())
        }
    }
}

/// Pin the current thread if affinity_mode is configured. Does nothing if None.
pub fn pin_thread_if_configured(
    affinity_mode: Option<AffinityMode>,
    affinity_start: Option<usize>,
    mpi_rank: i32,
    threads_per_rank: usize,
    thread_index: usize,
) {
    let Some(mode) = affinity_mode else {
        return;
    };

    let start_core = affinity_start.unwrap_or_else(|| num_online_cores() - 1);
    let local_rank = match mode {
        AffinityMode::Multinode => 0,
        AffinityMode::Singlenode => get_local_rank(mpi_rank),
    };

    let core_id = core_for_thread(mode, start_core, local_rank, threads_per_rank, thread_index);

    match pin_to_core(core_id) {
        Ok(()) => {
            eprintln!(
                "rank {} thread {} pinned to core {}",
                mpi_rank, thread_index, core_id
            );
        }
        Err(errno) => {
            eprintln!(
                "rank {} thread {}: failed to pin to core {} (errno={})",
                mpi_rank, thread_index, core_id, errno
            );
        }
    }
}
