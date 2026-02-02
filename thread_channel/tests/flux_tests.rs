//! Integration tests for Flux (SPSC-based n-to-n).

use std::thread;
use thread_channel::{create_flux, Flux, ReceivedMessage, SendError};

#[test]
fn test_two_node_communication() {
    let mut nodes: Vec<Flux<u32>> = create_flux(2, 64);

    // Node 0 -> Node 1
    nodes[0].call(1, 100).unwrap();
    nodes[0].call(1, 200).unwrap();
    nodes[0].flush();

    let (from, msg) = nodes[1].poll().unwrap();
    assert_eq!(from, 0);
    assert!(matches!(msg, ReceivedMessage::Request { data: 100, .. }));
    let (from, msg) = nodes[1].poll().unwrap();
    assert_eq!(from, 0);
    assert!(matches!(msg, ReceivedMessage::Request { data: 200, .. }));

    // Node 1 -> Node 0
    nodes[1].call(0, 300).unwrap();
    nodes[1].flush();
    let (from, msg) = nodes[0].poll().unwrap();
    assert_eq!(from, 1);
    assert!(matches!(msg, ReceivedMessage::Request { data: 300, .. }));
}

#[test]
fn test_call_reply() {
    let mut nodes: Vec<Flux<u32>> = create_flux(2, 64);

    // Node 0 calls Node 1
    let req_num = nodes[0].call(1, 42).unwrap();
    nodes[0].flush();
    assert_eq!(req_num, 0);

    // Second call should have different req_num
    let req_num2 = nodes[0].call(1, 43).unwrap();
    nodes[0].flush();
    assert_eq!(req_num2, 1);

    // Node 1 receives requests and replies
    let (from, msg) = nodes[1].poll().unwrap();
    assert_eq!(from, 0);
    match msg {
        ReceivedMessage::Request { req_num: r, data } => {
            assert_eq!(r, 0);
            assert_eq!(data, 42);
            nodes[1].reply(0, r, data * 2).unwrap();
        }
        _ => panic!("expected Request"),
    }

    let (from, msg) = nodes[1].poll().unwrap();
    assert_eq!(from, 0);
    match msg {
        ReceivedMessage::Request { req_num: r, data } => {
            assert_eq!(r, 1);
            assert_eq!(data, 43);
            nodes[1].reply(0, r, data * 2).unwrap();
        }
        _ => panic!("expected Request"),
    }
    nodes[1].flush();

    // Node 0 receives responses
    let (from, msg) = nodes[0].poll().unwrap();
    assert_eq!(from, 1);
    match msg {
        ReceivedMessage::Response { req_num: r, data } => {
            assert_eq!(r, 0);
            assert_eq!(data, 84);
        }
        _ => panic!("expected Response"),
    }

    let (from, msg) = nodes[0].poll().unwrap();
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
    let mut nodes: Vec<Flux<(usize, usize)>> = create_flux(n, 64);

    // Each node sends to all other nodes
    for i in 0..n {
        for j in 0..n {
            if i != j {
                nodes[i].call(j, (i, j)).unwrap();
            }
        }
        nodes[i].flush();
    }

    // Each node receives from all other nodes
    for i in 0..n {
        let mut received = Vec::new();
        for _ in 0..(n - 1) {
            let (from, msg) = nodes[i].poll().unwrap();
            match msg {
                ReceivedMessage::Request { data: (sender, receiver), .. } => {
                    assert_eq!(from, sender);
                    assert_eq!(i, receiver);
                    received.push(from);
                }
                _ => panic!("expected Request"),
            }
        }
        received.sort();
        let expected: Vec<usize> = (0..n).filter(|&j| j != i).collect();
        assert_eq!(received, expected);
    }
}

#[test]
fn test_round_robin_poll() {
    let mut nodes: Vec<Flux<usize>> = create_flux(4, 64);

    // Nodes 1, 2, 3 all send to node 0
    nodes[1].call(0, 1).unwrap();
    nodes[1].flush();
    nodes[2].call(0, 2).unwrap();
    nodes[2].flush();
    nodes[3].call(0, 3).unwrap();
    nodes[3].flush();

    let mut received = Vec::new();
    for _ in 0..3 {
        let (from, msg) = nodes[0].poll().unwrap();
        match msg {
            ReceivedMessage::Request { data: val, .. } => received.push((from, val)),
            _ => panic!("expected Request"),
        }
    }

    // Should have received from all three
    assert!(received.contains(&(1, 1)));
    assert!(received.contains(&(2, 2)));
    assert!(received.contains(&(3, 3)));

    // Channel should be empty now
    assert!(nodes[0].poll().is_none());
}

#[test]
fn test_channel_full() {
    let mut nodes: Vec<Flux<u32>> = create_flux(2, 2); // Very small capacity

    // Fill the channel (capacity 2 = 1 slot usable)
    assert!(nodes[0].call(1, 1).is_ok());

    // Second send should fail with Full
    match nodes[0].call(1, 2) {
        Err(SendError::Full(2)) => {}
        other => panic!("expected Full error, got {:?}", other),
    }
}

#[test]
fn test_invalid_peer() {
    let mut nodes: Vec<Flux<u32>> = create_flux(2, 64);

    // Send to non-existent peer
    assert!(matches!(
        nodes[0].call(10, 42),
        Err(SendError::InvalidPeer(42))
    ));

    // Cannot send to self
    assert!(matches!(
        nodes[0].call(0, 42),
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
                                match node.call(peer, id as u64 * 1_000_000 + i as u64) {
                                    Ok(_) => break,
                                    Err(SendError::Full(_)) => {
                                        node.flush();
                                        std::hint::spin_loop();
                                    }
                                    Err(e) => panic!("send error: {:?}", e),
                                }
                            }
                        }
                    }
                }
                node.flush();

                // Receive from all peers
                let expected = (n - 1) * msgs_per_pair;
                let mut count = 0;
                while count < expected {
                    if let Some((from, ReceivedMessage::Request { data: val, .. })) = node.poll() {
                        assert_eq!(val / 1_000_000, from as u64);
                        count += 1;
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
                        let req_num = node.call(partner, i).unwrap();
                        node.flush();
                        loop {
                            if let Some((from, ReceivedMessage::Response { req_num: r, data })) = node.poll() {
                                assert_eq!(from, partner);
                                assert_eq!(r, req_num);
                                assert_eq!(data, i);
                                break;
                            }
                        }
                    }
                } else {
                    // Responder
                    for _ in 0..iterations {
                        let (from, req_num, v) = loop {
                            if let Some((from, ReceivedMessage::Request { req_num, data })) = node.poll() {
                                assert_eq!(from, partner);
                                break (from, req_num, data);
                            }
                        };
                        node.reply(from, req_num, v).unwrap();
                        node.flush();
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}
