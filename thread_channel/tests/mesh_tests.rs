//! Integration tests for Mesh (MPSC-based n-to-n).

use std::thread;
use thread_channel::{create_mesh, Mesh, RecvError, SendError};

#[test]
fn test_two_node_communication() {
    let mut nodes: Vec<Mesh<u32>> = create_mesh(2);

    // Node 0 -> Node 1
    nodes[0].send(1, 100).unwrap();
    nodes[0].send(1, 200).unwrap();

    assert_eq!(nodes[1].try_recv().unwrap(), (0, 100));
    assert_eq!(nodes[1].try_recv().unwrap(), (0, 200));

    // Node 1 -> Node 0
    nodes[1].send(0, 300).unwrap();
    assert_eq!(nodes[0].try_recv().unwrap(), (1, 300));
}

#[test]
fn test_all_to_all() {
    let n = 4;
    let nodes: Vec<Mesh<(usize, usize)>> = create_mesh(n);

    // Each node sends to all other nodes in threads
    let handles: Vec<_> = nodes
        .into_iter()
        .map(|mut node| {
            thread::spawn(move || {
                let id = node.id();

                // Send to all peers
                for j in 0..n {
                    if j != id {
                        node.send(j, (id, j)).unwrap();
                    }
                }

                // Receive from all peers
                let mut received = Vec::new();
                for _ in 0..(n - 1) {
                    loop {
                        match node.try_recv() {
                            Ok((from, (sender, receiver))) => {
                                assert_eq!(from, sender);
                                assert_eq!(receiver, id);
                                received.push(from);
                                break;
                            }
                            Err(RecvError::Empty) => std::hint::spin_loop(),
                            Err(e) => panic!("recv error: {:?}", e),
                        }
                    }
                }

                received.sort();
                received
            })
        })
        .collect();

    for (i, h) in handles.into_iter().enumerate() {
        let received = h.join().unwrap();
        let expected: Vec<usize> = (0..n).filter(|&j| j != i).collect();
        assert_eq!(received, expected);
    }
}

#[test]
fn test_recv_from_multiple() {
    let mut nodes: Vec<Mesh<u32>> = create_mesh(4);

    // Multiple senders to node 0
    nodes[1].send(0, 100).unwrap();
    nodes[2].send(0, 200).unwrap();
    nodes[3].send(0, 300).unwrap();

    // Receive all messages
    let mut received = Vec::new();
    for _ in 0..3 {
        match nodes[0].try_recv() {
            Ok((from, val)) => received.push((from, val)),
            Err(RecvError::Empty) => break,
            Err(e) => panic!("recv error: {:?}", e),
        }
    }

    assert!(received.contains(&(1, 100)));
    assert!(received.contains(&(2, 200)));
    assert!(received.contains(&(3, 300)));
}

#[test]
fn test_invalid_peer() {
    let nodes: Vec<Mesh<u32>> = create_mesh(2);

    // Send to non-existent peer
    assert!(matches!(
        nodes[0].send(10, 42),
        Err(SendError::InvalidPeer(42))
    ));

    // Cannot send to self
    assert!(matches!(
        nodes[0].send(0, 42),
        Err(SendError::InvalidPeer(42))
    ));
}

#[test]
fn test_fan_in() {
    // Many nodes sending to one node
    let n = 8;
    let msgs_per_sender = 1000;
    let nodes: Vec<Mesh<(usize, u32)>> = create_mesh(n);

    let handles: Vec<_> = nodes
        .into_iter()
        .map(|mut node| {
            let id = node.id();
            thread::spawn(move || {
                if id == 0 {
                    // Receiver
                    let expected = (n - 1) * msgs_per_sender;
                    let mut counts = vec![0u32; n];
                    let mut total = 0;

                    while total < expected {
                        match node.try_recv() {
                            Ok((from, (sender, _))) => {
                                assert_eq!(from, sender);
                                counts[from] += 1;
                                total += 1;
                            }
                            Err(RecvError::Empty) => std::hint::spin_loop(),
                            Err(e) => panic!("recv error: {:?}", e),
                        }
                    }

                    for i in 1..n {
                        assert_eq!(counts[i], msgs_per_sender as u32);
                    }
                } else {
                    // Sender
                    for i in 0..msgs_per_sender {
                        node.send(0, (id, i as u32)).unwrap();
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_threaded_all_to_all() {
    let n = 8;
    let msgs_per_pair = 500;
    let nodes: Vec<Mesh<u64>> = create_mesh(n);

    let handles: Vec<_> = nodes
        .into_iter()
        .map(|mut node| {
            thread::spawn(move || {
                let id = node.id();

                // Send to all peers
                for peer in 0..n {
                    if peer != id {
                        for i in 0..msgs_per_pair {
                            node.send(peer, id as u64 * 1_000_000 + i as u64).unwrap();
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
fn test_mixed_recv_patterns() {
    let mut nodes: Vec<Mesh<u32>> = create_mesh(3);

    // Node 1 and 2 send to node 0
    nodes[1].send(0, 1).unwrap();
    nodes[2].send(0, 2).unwrap();
    nodes[1].send(0, 3).unwrap();
    nodes[2].send(0, 4).unwrap();

    // Receive all messages
    let mut received = Vec::new();
    for _ in 0..4 {
        match nodes[0].try_recv() {
            Ok((from, val)) => received.push((from, val)),
            Err(RecvError::Empty) => break,
            Err(e) => panic!("recv error: {:?}", e),
        }
    }

    assert_eq!(received.len(), 4);
}
