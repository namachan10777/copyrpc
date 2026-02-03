// Simple program to inspect assembly of SPSC hot paths

use thread_channel::spsc;

// Prevent compiler from optimizing away the operations
#[inline(never)]
fn bench_write_flush(tx: &mut spsc::Sender<u64>, data: u64) {
    tx.write(data).unwrap();
    tx.flush();
}

#[inline(never)]
fn bench_poll_sync(rx: &mut spsc::Receiver<u64>) -> Option<u64> {
    let result = rx.poll();
    rx.sync();
    result
}

#[inline(never)]
fn bench_try_recv(rx: &mut spsc::Receiver<u64>) -> Result<u64, thread_channel::spsc::TryRecvError> {
    rx.try_recv()
}

fn main() {
    let (mut tx, mut rx) = spsc::channel::<u64>(1024);

    // Warmup
    for i in 0..100 {
        bench_write_flush(&mut tx, i);
        bench_poll_sync(&mut rx);
    }

    // Prevent optimization
    println!("Done");
}
