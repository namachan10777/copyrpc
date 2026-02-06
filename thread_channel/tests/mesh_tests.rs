//! Integration tests for Mesh (MPSC-based n-to-n).

use std::thread;
use thread_channel::{create_mesh, Mesh, ReceivedMessage, RecvError, SendError};

#[test]
fn test_two_node_communication() {
    let mut nodes: Vec<Mesh<u32>> = create_mesh(2);

    // Node 0 -> Node 1
    nodes[0].notify(1, 100).unwrap();
    nodes[0].notify(1, 200).unwrap();

    assert_eq!(
        nodes[1].try_recv().unwrap(),
        (0, ReceivedMessage::Notify(100))
    );
    assert_eq!(
        nodes[1].try_recv().unwrap(),
        (0, ReceivedMessage::Notify(200))
    );

    // Node 1 -> Node 0
    nodes[1].notify(0, 300).unwrap();
    assert_eq!(
        nodes[0].try_recv().unwrap(),
        (1, ReceivedMessage::Notify(300))
    );
}

#[test]
fn test_call_reply() {
    let mut nodes: Vec<Mesh<u32>> = create_mesh(2);

    // Node 0 calls Node 1
    let req_num = nodes[0].call(1, 42).unwrap();
    assert_eq!(req_num, 0);

    // Second call should have different req_num
    let req_num2 = nodes[0].call(1, 43).unwrap();
    assert_eq!(req_num2, 1);

    // Node 1 receives requests and replies
    let (from, msg) = nodes[1].recv().unwrap();
    assert_eq!(from, 0);
    match msg {
        ReceivedMessage::Request { req_num: r, data } => {
            assert_eq!(r, 0);
            assert_eq!(data, 42);
            nodes[1].reply(0, r, data * 2).unwrap();
        }
        _ => panic!("expected Request"),
    }

    let (from, msg) = nodes[1].recv().unwrap();
    assert_eq!(from, 0);
    match msg {
        ReceivedMessage::Request { req_num: r, data } => {
            assert_eq!(r, 1);
            assert_eq!(data, 43);
            nodes[1].reply(0, r, data * 2).unwrap();
        }
        _ => panic!("expected Request"),
    }

    // Node 0 receives responses
    let (from, msg) = nodes[0].recv().unwrap();
    assert_eq!(from, 1);
    match msg {
        ReceivedMessage::Response { req_num: r, data } => {
            assert_eq!(r, 0);
            assert_eq!(data, 84);
        }
        _ => panic!("expected Response"),
    }

    let (from, msg) = nodes[0].recv().unwrap();
    assert_eq!(from, 1);
    match msg {
        ReceivedMessage::Response { req_num: r, data } => {
            assert_eq!(r, 1);
            assert_eq!(data, 86);
        }
        _ => panic!("expected Response"),
    }
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
                        node.notify(j, (id, j)).unwrap();
                    }
                }

                // Receive from all peers
                let mut received = Vec::new();
                for _ in 0..(n - 1) {
                    loop {
                        match node.try_recv() {
                            Ok((from, ReceivedMessage::Notify((sender, receiver)))) => {
                                assert_eq!(from, sender);
                                assert_eq!(receiver, id);
                                received.push(from);
                                break;
                            }
                            Ok(_) => panic!("expected Notify"),
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
    nodes[1].notify(0, 100).unwrap();
    nodes[2].notify(0, 200).unwrap();
    nodes[3].notify(0, 300).unwrap();

    // Receive all messages
    let mut received = Vec::new();
    for _ in 0..3 {
        match nodes[0].try_recv() {
            Ok((from, ReceivedMessage::Notify(val))) => received.push((from, val)),
            Ok(_) => panic!("expected Notify"),
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
        nodes[0].notify(10, 42),
        Err(SendError::InvalidPeer(42))
    ));

    // Cannot send to self
    assert!(matches!(
        nodes[0].notify(0, 42),
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
                            Ok((from, ReceivedMessage::Notify((sender, _)))) => {
                                assert_eq!(from, sender);
                                counts[from] += 1;
                                total += 1;
                            }
                            Ok(_) => panic!("expected Notify"),
                            Err(RecvError::Empty) => std::hint::spin_loop(),
                            Err(e) => panic!("recv error: {:?}", e),
                        }
                    }

                    for count in &counts[1..n] {
                        assert_eq!(*count, msgs_per_sender as u32);
                    }
                } else {
                    // Sender
                    for i in 0..msgs_per_sender {
                        node.notify(0, (id, i as u32)).unwrap();
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
                            node.notify(peer, id as u64 * 1_000_000 + i as u64).unwrap();
                        }
                    }
                }

                // Receive from all peers
                let expected = (n - 1) * msgs_per_pair;
                let mut count = 0;
                while count < expected {
                    match node.try_recv() {
                        Ok((from, ReceivedMessage::Notify(val))) => {
                            assert_eq!(val / 1_000_000, from as u64);
                            count += 1;
                        }
                        Ok(_) => panic!("expected Notify"),
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
    nodes[1].notify(0, 1).unwrap();
    nodes[2].notify(0, 2).unwrap();
    nodes[1].notify(0, 3).unwrap();
    nodes[2].notify(0, 4).unwrap();

    // Receive all messages
    let mut received = Vec::new();
    for _ in 0..4 {
        match nodes[0].try_recv() {
            Ok((from, ReceivedMessage::Notify(val))) => received.push((from, val)),
            Ok(_) => panic!("expected Notify"),
            Err(RecvError::Empty) => break,
            Err(e) => panic!("recv error: {:?}", e),
        }
    }

    assert_eq!(received.len(), 4);
}

#[test]
fn test_ping_pong_pairs() {
    let n = 4;
    let iterations = 10000;
    let nodes: Vec<Mesh<u64>> = create_mesh(n);

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
                        let req_num = node.call(partner, i).unwrap();
                        loop {
                            match node.try_recv() {
                                Ok((from, ReceivedMessage::Response { req_num: r, data })) => {
                                    assert_eq!(from, partner);
                                    assert_eq!(r, req_num);
                                    assert_eq!(data, i);
                                    break;
                                }
                                Ok(_) => panic!("expected Response"),
                                Err(RecvError::Empty) => std::hint::spin_loop(),
                                Err(e) => panic!("recv error: {:?}", e),
                            }
                        }
                    }
                } else {
                    // Responder
                    for _ in 0..iterations {
                        let (from, req_num, v) = loop {
                            match node.try_recv() {
                                Ok((from, ReceivedMessage::Request { req_num, data })) => {
                                    assert_eq!(from, partner);
                                    break (from, req_num, data);
                                }
                                Ok(_) => panic!("expected Request"),
                                Err(RecvError::Empty) => std::hint::spin_loop(),
                                Err(e) => panic!("recv error: {:?}", e),
                            }
                        };
                        node.reply(from, req_num, v).unwrap();
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}
