use std::io;
use std::time::Instant;

#[derive(Clone)]
pub struct QdSample {
    pub elapsed_us: u64,
    pub copyrpc_inflight: u32,
    pub flux_pending: u32,
    pub ipc_inflight: u32,
    pub extra: u32,
}

#[derive(Clone)]
pub struct LoopSample {
    pub elapsed_us: u64,
    pub loops: u32,
    pub cqe_recv: u32,
    pub req_write: u32,
    pub res_write: u32,
    pub req_res_write_total: u32,
}

pub struct QdCollector {
    interval: u32,
    counter: u32,
    start: Instant,
    samples: Vec<QdSample>,
}

impl QdCollector {
    pub fn new(interval: u32, capacity: usize) -> Self {
        Self {
            interval,
            counter: 0,
            start: Instant::now(),
            samples: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn tick(
        &mut self,
        copyrpc_inflight: u32,
        flux_pending: u32,
        ipc_inflight: u32,
        extra: u32,
    ) {
        self.counter += 1;
        if self.counter >= self.interval {
            self.counter = 0;
            self.samples.push(QdSample {
                elapsed_us: self.start.elapsed().as_micros() as u64,
                copyrpc_inflight,
                flux_pending,
                ipc_inflight,
                extra,
            });
        }
    }

    pub fn into_samples(self) -> Vec<QdSample> {
        self.samples
    }
}

pub fn write_csv(path: &str, samples: &[QdSample]) -> io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)?;
    writeln!(
        f,
        "elapsed_us,copyrpc_inflight,flux_pending,ipc_inflight,extra"
    )?;
    for s in samples {
        writeln!(
            f,
            "{},{},{},{},{}",
            s.elapsed_us, s.copyrpc_inflight, s.flux_pending, s.ipc_inflight, s.extra
        )?;
    }
    Ok(())
}

pub fn write_loop_csv(path: &str, samples: &[LoopSample]) -> io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)?;
    writeln!(
        f,
        "elapsed_us,loops,cqe_recv,req_write,res_write,req_res_write_total"
    )?;
    for s in samples {
        writeln!(
            f,
            "{},{},{},{},{},{}",
            s.elapsed_us, s.loops, s.cqe_recv, s.req_write, s.res_write, s.req_res_write_total
        )?;
    }
    Ok(())
}
