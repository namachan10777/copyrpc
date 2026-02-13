//! NUMA-aware CPU affinity for benchkv.
//!
//! Detects the NUMA node of the mlx5 device and assigns daemon/client threads
//! with soft affinity priority across NUMA nodes.

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
    cores.dedup();
    cores
}

fn read_node_cores(node: usize) -> Option<Vec<usize>> {
    let path = format!("/sys/devices/system/node/node{}/cpulist", node);
    fs::read_to_string(path).ok().map(|s| parse_cpulist(&s))
}

fn list_numa_nodes() -> Vec<usize> {
    let mut nodes = Vec::new();
    if let Ok(entries) = fs::read_dir("/sys/devices/system/node") {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(num) = name
                .strip_prefix("node")
                .and_then(|s| s.parse::<usize>().ok())
            {
                nodes.push(num);
            }
        }
    }
    nodes.sort_unstable();
    nodes
}

fn online_cores() -> Vec<usize> {
    if let Ok(s) = fs::read_to_string("/sys/devices/system/cpu/online") {
        return parse_cpulist(&s);
    }
    let n = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) as usize };
    (0..n).collect()
}

/// Get available cores for the given RDMA device with soft affinity priority:
/// 1) cores on device NUMA node excluding 0,1
/// 2) cores on other NUMA nodes excluding 0,1
/// 3) cores 0,1 on device NUMA node
/// 4) cores 0,1 on other NUMA nodes
pub fn get_available_cores(device_index: usize) -> Vec<usize> {
    let preferred_node = detect_numa_node(device_index);

    let mut all_cores = Vec::new();
    for node in list_numa_nodes() {
        if let Some(mut v) = read_node_cores(node) {
            all_cores.append(&mut v);
        }
    }
    if all_cores.is_empty() {
        all_cores = online_cores();
    }
    all_cores.sort_unstable();
    all_cores.dedup();

    let preferred_cores = preferred_node.and_then(read_node_cores).unwrap_or_default();
    let preferred_set: std::collections::HashSet<usize> = preferred_cores.into_iter().collect();

    let mut ordered = Vec::with_capacity(all_cores.len());

    ordered.extend(
        all_cores
            .iter()
            .copied()
            .filter(|c| *c >= 2 && preferred_set.contains(c)),
    );
    ordered.extend(
        all_cores
            .iter()
            .copied()
            .filter(|c| *c >= 2 && !preferred_set.contains(c)),
    );
    ordered.extend(
        all_cores
            .iter()
            .copied()
            .filter(|c| *c < 2 && preferred_set.contains(c)),
    );
    ordered.extend(
        all_cores
            .iter()
            .copied()
            .filter(|c| *c < 2 && !preferred_set.contains(c)),
    );

    if ordered.is_empty() {
        online_cores()
    } else {
        ordered
    }
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
/// Rank-local partitioning is intentionally disabled for benchmark stability.
/// Returns (daemon_cores, client_cores).
pub fn assign_cores(
    available: &[usize],
    num_daemons: usize,
    num_clients: usize,
    _ranks_on_node: u32,
    _rank_on_node: u32,
) -> (Vec<usize>, Vec<usize>) {
    let total_needed = num_daemons + num_clients;
    let my_cores = available;

    let daemon_cores: Vec<usize> = my_cores.iter().take(num_daemons).copied().collect();
    let client_cores: Vec<usize> = my_cores
        .iter()
        .skip(num_daemons)
        .take(num_clients)
        .copied()
        .collect();

    if daemon_cores.len() + client_cores.len() < total_needed {
        eprintln!(
            "Warning: only {} cores available for {} threads",
            my_cores.len(),
            total_needed
        );
    }

    (daemon_cores, client_cores)
}

/// Pin the current thread to the given core.
pub fn pin_thread(core_id: usize, label: &str) {
    match pin_to_core(core_id) {
        Ok(()) => eprintln!("{} pinned to core {}", label, core_id),
        Err(errno) => eprintln!(
            "{}: failed to pin to core {} (errno={})",
            label, core_id, errno
        ),
    }
}
