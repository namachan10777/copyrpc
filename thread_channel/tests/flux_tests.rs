//! Integration tests for Flux (SPSC-based n-to-n).

use std::thread;
use thread_channel::{create_flux, Flux, RecvError, SendError};

#[test]
fn test_two_node_communication() {
    let mut nodes: Vec<Flux<u32>> = create_flux(2, 64);

    // Node 0 -> Node 1
    nodes[0].send(1, 100).unwrap();
    nodes[0].send(1, 200).unwrap();

    let (from, val) = nodes[1].recv().unwrap();
    assert_eq!(from, 0);
    assert_eq!(val, 100);
    let (from, val) = nodes[1].recv().unwrap();
    assert_eq!(from, 0);
    assert_eq!(val, 200);

    // Node 1 -> Node 0
    nodes[1].send(0, 300).unwrap();
    let (from, val) = nodes[0].recv().unwrap();
    assert_eq!(from, 1);
    assert_eq!(val, 300);
}

#[test]
fn test_all_to_all() {
    let n = 4;
    let mut nodes: Vec<Flux<(usize, usize)>> = create_flux(n, 64);

    // Each node sends to all other nodes
    for i in 0..n {
        for j in 0..n {
            if i != j {
                nodes[i].send(j, (i, j)).unwrap();
            }
        }
    }

    // Each node receives from all other nodes
    for i in 0..n {
        let mut received = Vec::new();
        for _ in 0..(n - 1) {
            let (from, val) = nodes[i].recv().unwrap();
            assert_eq!(from, val.0);
            assert_eq!(i, val.1);
            received.push(from);
        }
        received.sort();
        let expected: Vec<usize> = (0..n).filter(|&j| j != i).collect();
        assert_eq!(received, expected);
    }
}

#[test]
fn test_round_robin_recv() {
    let mut nodes: Vec<Flux<usize>> = create_flux(4, 64);

    // Nodes 1, 2, 3 all send to node 0
    nodes[1].send(0, 1).unwrap();
    nodes[2].send(0, 2).unwrap();
    nodes[3].send(0, 3).unwrap();

    let mut received = Vec::new();
    for _ in 0..3 {
        let (from, val) = nodes[0].try_recv().unwrap();
        received.push((from, val));
    }

    // Should have received from all three
    assert!(received.contains(&(1, 1)));
    assert!(received.contains(&(2, 2)));
    assert!(received.contains(&(3, 3)));

    // Channel should be empty now
    assert!(matches!(nodes[0].try_recv(), Err(RecvError::Empty)));
}

#[test]
fn test_channel_full() {
    let mut nodes: Vec<Flux<u32>> = create_flux(2, 2); // Very small capacity

    // Fill the channel (capacity 2 = 1 slot usable)
    assert!(nodes[0].try_send(1, 1).is_ok());

    // Second send should fail with Full
    match nodes[0].try_send(1, 2) {
        Err(SendError::Full(2)) => {}
        other => panic!("expected Full error, got {:?}", other),
    }
}

#[test]
fn test_invalid_peer() {
    let mut nodes: Vec<Flux<u32>> = create_flux(2, 64);

    // Send to non-existent peer
    assert!(matches!(
        nodes[0].try_send(10, 42),
        Err(SendError::InvalidPeer(42))
    ));

    // Cannot send to self
    assert!(matches!(
        nodes[0].try_send(0, 42),
        Err(SendError::InvalidPeer(42))
    ));
}

#[test]
fn test_threaded_all_to_all() {
    let n = 8;
    let msgs_per_pair = 1000;
    let nodes: Vec<Flux<u64>> = create_flux(n, 2048);

    let handles: Vec<_> = nodes
        .into_iter()
        .map(|mut node| {
            thread::spawn(move || {
                let id = node.id();

                // Send to all peers
                for peer in 0..n {
                    if peer != id {
                        for i in 0..msgs_per_pair {
                            loop {
                                match node.try_send(peer, id as u64 * 1_000_000 + i as u64) {
                                    Ok(()) => break,
                                    Err(SendError::Full(_)) => {
                                        std::hint::spin_loop();
                                    }
                                    Err(e) => panic!("send error: {:?}", e),
                                }
                            }
                        }
                    }
                }

                // Receive from all peers
                let expected = (n - 1) * msgs_per_pair;
                let mut count = 0;
                while count < expected {
                    match node.try_recv() {
                        Ok((from, val)) => {
                            assert_eq!(val / 1_000_000, from as u64);
                            count += 1;
                        }
                        Err(RecvError::Empty) => std::hint::spin_loop(),
                        Err(e) => panic!("recv error: {:?}", e),
                    }
                }

                count
            })
        })
        .collect();

    for h in handles {
        let count = h.join().unwrap();
        assert_eq!(count, (n - 1) * msgs_per_pair);
    }
}

#[test]
fn test_ping_pong_pairs() {
    let n = 4;
    let iterations = 10000;
    let nodes: Vec<Flux<u64>> = create_flux(n, 64);

    let handles: Vec<_> = nodes
        .into_iter()
        .map(|mut node| {
            thread::spawn(move || {
                let id = node.id();

                // Pair up: 0<->1, 2<->3, etc.
                let partner = if id % 2 == 0 { id + 1 } else { id - 1 };

                if id < partner {
                    // Initiator
                    for i in 0..iterations {
                        node.send(partner, i).unwrap();
                        loop {
                            match node.try_recv() {
                                Ok((from, v)) => {
                                    assert_eq!(from, partner);
                                    assert_eq!(v, i);
                                    break;
                                }
                                Err(RecvError::Empty) => std::hint::spin_loop(),
                                Err(e) => panic!("recv error: {:?}", e),
                            }
                        }
                    }
                } else {
                    // Responder
                    for _ in 0..iterations {
                        let v = loop {
                            match node.try_recv() {
                                Ok((from, v)) => {
                                    assert_eq!(from, partner);
                                    break v;
                                }
                                Err(RecvError::Empty) => std::hint::spin_loop(),
                                Err(e) => panic!("recv error: {:?}", e),
                            }
                        };
                        node.send(partner, v).unwrap();
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}
