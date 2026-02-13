use mpi::collective::CommunicatorCollectives;

fn parse_status_kb(field: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        if !line.starts_with(field) {
            return None;
        }
        line.split_whitespace().nth(1)?.parse::<u64>().ok()
    })
}

/// Peak resident set size in KiB.
/// Linux: uses `/proc/self/status` `VmHWM` (fallback: `VmRSS`).
pub fn peak_rss_kb() -> u64 {
    parse_status_kb("VmHWM:").or_else(|| parse_status_kb("VmRSS:")).unwrap_or(0)
}

/// Report local/global peak process memory.
/// Returns local peak RSS (KiB).
pub fn report_peak_process_memory(
    world: &mpi::topology::SimpleCommunicator,
    rank: u32,
    mode: &str,
) -> u64 {
    let local_peak_kb = peak_rss_kb();
    let mut max_peak_kb = 0u64;
    world.all_reduce_into(
        &local_peak_kb,
        &mut max_peak_kb,
        mpi::collective::SystemOperation::max(),
    );

    eprintln!(
        "  rank {} mode {}: peak process RSS = {} KiB ({:.2} MiB)",
        rank,
        mode,
        local_peak_kb,
        local_peak_kb as f64 / 1024.0
    );
    if rank == 0 {
        eprintln!(
            "  global max mode {}: peak process RSS = {} KiB ({:.2} MiB)",
            mode,
            max_peak_kb,
            max_peak_kb as f64 / 1024.0
        );
    }

    local_peak_kb
}
