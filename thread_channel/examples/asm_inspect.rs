// Simple program to inspect assembly of SPSC hot paths

use thread_channel::transport::fastforward::{self, Sender, Receiver, TryRecvError};

// Prevent compiler from optimizing away the operations
#[inline(never)]
fn bench_send(tx: &mut Sender<u64>, data: u64) {
    tx.send(data).unwrap();
}

#[inline(never)]
fn bench_recv(rx: &mut Receiver<u64>) -> Option<u64> {
    rx.recv()
}

#[inline(never)]
fn bench_try_recv(rx: &mut Receiver<u64>) -> Result<u64, TryRecvError> {
    rx.try_recv()
}

fn main() {
    let (mut tx, mut rx) = fastforward::channel::<u64>(1024);

    // Warmup
    for i in 0..100 {
        bench_send(&mut tx, i);
        bench_recv(&mut rx);
    }

    // Prevent optimization
    println!("Done");
}
