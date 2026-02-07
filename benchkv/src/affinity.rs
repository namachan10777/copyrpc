//! NUMA-aware CPU affinity for benchkv.
//!
//! Detects the NUMA node of the mlx5 device and assigns daemon/client threads
//! to cores on that node, skipping cores 0 and 1 (reserved for IRQ handling).

use std::fs;

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

/// Parse a cpulist string like "0-15,32-47" into a sorted Vec of core IDs.
fn parse_cpulist(s: &str) -> Vec<usize> {
    let mut cores = Vec::new();
    for part in s.trim().split(',') {
        let part = part.trim();
        if let Some((a, b)) = part.split_once('-') {
            if let (Ok(start), Ok(end)) = (a.parse::<usize>(), b.parse::<usize>()) {
                cores.extend(start..=end);
            }
        } else if let Ok(c) = part.parse::<usize>() {
            cores.push(c);
        }
    }
    cores.sort();
    cores
}

/// Get available cores for the given RDMA device, excluding cores 0 and 1.
pub fn get_available_cores(device_index: usize) -> Vec<usize> {
    // Try to detect NUMA node from mlx5 device
    let numa_node = detect_numa_node(device_index);

    let cores = match numa_node {
        Some(node) => {
            let path = format!("/sys/devices/system/node/node{}/cpulist", node);
            fs::read_to_string(&path)
                .ok()
                .map(|s| parse_cpulist(&s))
                .unwrap_or_default()
        }
        None => {
            // Fallback: use all online cores
            let n = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) as usize };
            (0..n).collect()
        }
    };

    // Exclude cores 0 and 1 (IRQ handling)
    cores.into_iter().filter(|&c| c >= 2).collect()
}

fn detect_numa_node(device_index: usize) -> Option<usize> {
    // Try mlx5_0, mlx5_1, etc.
    let path = format!(
        "/sys/class/infiniband/mlx5_{}/device/numa_node",
        device_index
    );
    fs::read_to_string(&path)
        .ok()
        .and_then(|s| s.trim().parse::<isize>().ok())
        .and_then(|n| if n >= 0 { Some(n as usize) } else { None })
}

/// Assign cores to threads: daemon threads first, then client threads.
/// `ranks_on_node` and `rank_on_node` partition cores across co-located ranks.
/// Returns (daemon_cores, client_cores).
pub fn assign_cores(
    available: &[usize],
    num_daemons: usize,
    num_clients: usize,
    ranks_on_node: u32,
    rank_on_node: u32,
) -> (Vec<usize>, Vec<usize>) {
    let total_needed = num_daemons + num_clients;

    // Partition available cores evenly across ranks on this node
    let per_rank = available.len() / ranks_on_node as usize;
    let offset = rank_on_node as usize * per_rank;
    let my_cores = &available[offset..offset + per_rank];

    let daemon_cores: Vec<usize> = my_cores.iter().take(num_daemons).copied().collect();
    let client_cores: Vec<usize> = my_cores
        .iter()
        .skip(num_daemons)
        .take(num_clients)
        .copied()
        .collect();

    if daemon_cores.len() + client_cores.len() < total_needed {
        eprintln!(
            "Warning: only {} cores available for {} threads (rank_on_node={})",
            my_cores.len(),
            total_needed,
            rank_on_node
        );
    }

    (daemon_cores, client_cores)
}

/// Pin the current thread to the given core.
pub fn pin_thread(core_id: usize, label: &str) {
    match pin_to_core(core_id) {
        Ok(()) => eprintln!("{} pinned to core {}", label, core_id),
        Err(errno) => eprintln!("{}: failed to pin to core {} (errno={})", label, core_id, errno),
    }
}
