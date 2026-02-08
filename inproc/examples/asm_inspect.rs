// Simple program to inspect assembly of SPSC hot paths

use inproc::transport::fastforward::{self, Sender, Receiver};

// Prevent compiler from optimizing away the operations
#[inline(never)]
fn bench_send(tx: &mut Sender<u64>, data: u64) {
    tx.send(data).unwrap();
}

#[inline(never)]
fn bench_recv(rx: &mut Receiver<u64>) -> Option<u64> {
    rx.recv()
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
