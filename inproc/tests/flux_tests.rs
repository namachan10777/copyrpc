//! Integration tests for Flux (SPSC-based n-to-n).

use inproc::{Flux, SendError, create_flux};
use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

#[test]
fn test_two_node_communication() {
    let mut nodes: Vec<Flux<u32, ()>> = create_flux(2, 64, 32);

    // Node 0 -> Node 1
    nodes[0].call(1, 100, ()).unwrap();
    nodes[0].call(1, 200, ()).unwrap();
    nodes[0].poll(|_, _| {}); // flush writes

    nodes[1].poll(|_, _| {});
    let handle = nodes[1].try_recv().unwrap();
    assert_eq!(handle.from(), 0);
    assert_eq!(handle.data(), 100);
    // Don't reply, just drop the handle

    // Need to poll again for the second message since we already consumed from the queue
    let handle = nodes[1].try_recv().unwrap();
    assert_eq!(handle.from(), 0);
    assert_eq!(handle.data(), 200);

    // Node 1 -> Node 0
    nodes[1].call(0, 300, ()).unwrap();
    nodes[1].poll(|_, _| {}); // flush writes

    nodes[0].poll(|_, _| {});
    let handle = nodes[0].try_recv().unwrap();
    assert_eq!(handle.from(), 1);
    assert_eq!(handle.data(), 300);
}

#[test]
fn test_call_reply_with_callback() {
    // Track responses via callback
    thread_local! {
        static RESPONSES: RefCell<Vec<(u32, u32)>> = const { RefCell::new(Vec::new()) };
    }

    let mut nodes: Vec<Flux<u32, u32>> = create_flux(2, 64, 32);

    let mut on_resp = |user_data: u32, data: u32| {
        RESPONSES.with(|r| r.borrow_mut().push((user_data, data)));
    };

    // Node 0 calls Node 1 with user_data
    nodes[0].call(1, 42, 100).unwrap(); // user_data = 100
    nodes[0].call(1, 43, 101).unwrap(); // user_data = 101
    nodes[0].poll(&mut on_resp); // flush writes

    // Node 1 receives requests and replies
    nodes[1].poll(|_, _| {});

    let handle = nodes[1].try_recv().unwrap();
    assert_eq!(handle.from(), 0);
    assert_eq!(handle.data(), 42);
    let data = handle.data();
    handle.reply(data * 2); // reply with 84

    let handle = nodes[1].try_recv().unwrap();
    assert_eq!(handle.from(), 0);
    assert_eq!(handle.data(), 43);
    let data = handle.data();
    handle.reply(data * 2); // reply with 86
    nodes[1].poll(|_, _| {}); // flush replies

    // Node 0 receives responses (via callback)
    nodes[0].poll(&mut on_resp);

    RESPONSES.with(|r| {
        let responses = r.borrow();
        assert_eq!(responses.len(), 2);
        // Responses should have user_data and response data
        assert!(responses.contains(&(100, 84)));
        assert!(responses.contains(&(101, 86)));
    });
}

#[test]
fn test_all_to_all() {
    let n = 4;
    let mut nodes: Vec<Flux<(usize, usize), ()>> = create_flux(n, 64, 32);

    // Each node sends to all other nodes
    for (i, node) in nodes.iter_mut().enumerate() {
        for j in 0..n {
            if i != j {
                node.call(j, (i, j), ()).unwrap();
            }
        }
        node.poll(|_, _| {}); // flush writes
    }

    // Each node receives from all other nodes
    for (i, node) in nodes.iter_mut().enumerate() {
        let mut received = Vec::new();
        node.poll(|_, _| {});
        while let Some(handle) = node.try_recv() {
            let (sender, receiver) = handle.data();
            assert_eq!(handle.from(), sender);
            assert_eq!(i, receiver);
            received.push(handle.from());
        }
        received.sort();
        let expected: Vec<usize> = (0..n).filter(|&j| j != i).collect();
        assert_eq!(received, expected);
    }
}

#[test]
fn test_round_robin_poll() {
    let mut nodes: Vec<Flux<usize, ()>> = create_flux(4, 64, 32);

    // Nodes 1, 2, 3 all send to node 0
    nodes[1].call(0, 1, ()).unwrap();
    nodes[1].poll(|_, _| {}); // flush
    nodes[2].call(0, 2, ()).unwrap();
    nodes[2].poll(|_, _| {}); // flush
    nodes[3].call(0, 3, ()).unwrap();
    nodes[3].poll(|_, _| {}); // flush

    nodes[0].poll(|_, _| {});

    let mut received = Vec::new();
    while let Some(handle) = nodes[0].try_recv() {
        received.push((handle.from(), handle.data()));
    }

    // Should have received from all three
    assert!(received.contains(&(1, 1)));
    assert!(received.contains(&(2, 2)));
    assert!(received.contains(&(3, 3)));

    // Queue should be empty now
    assert!(nodes[0].try_recv().is_none());
}

#[test]
fn test_channel_full() {
    let mut nodes: Vec<Flux<u32, ()>> = create_flux(2, 4, 2); // Very small capacity: cap=4, inflight_max=2

    // FastForward uses validity flags, so full capacity is available
    // inflight_max=2 means at most 2 concurrent requests per channel
    assert!(nodes[0].call(1, 1, ()).is_ok());
    assert!(nodes[0].call(1, 2, ()).is_ok());

    // Third send should fail with Full (exceeds inflight_max)
    match nodes[0].call(1, 3, ()) {
        Err(SendError::Full(3)) => {}
        other => panic!("expected Full error, got {:?}", other),
    }
}

#[test]
fn test_invalid_peer() {
    let mut nodes: Vec<Flux<u32, ()>> = create_flux(2, 64, 32);

    // Send to non-existent peer
    assert!(matches!(
        nodes[0].call(10, 42, ()),
        Err(SendError::InvalidPeer(42))
    ));

    // Cannot send to self
    assert!(matches!(
        nodes[0].call(0, 42, ()),
        Err(SendError::InvalidPeer(42))
    ));
}

#[test]
fn test_threaded_all_to_all() {
    let n = 8;
    let msgs_per_pair = 1000;

    // Create shared counters for response tracking
    let response_counts: Vec<Arc<AtomicU64>> =
        (0..n).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let nodes: Vec<Flux<u64, usize>> = create_flux(n, 4096, 1024);

    let handles: Vec<_> = nodes
        .into_iter()
        .enumerate()
        .map(|(node_idx, mut node)| {
            let response_counter = Arc::clone(&response_counts[node_idx]);
            thread::spawn(move || {
                let id = node.id();

                let mut on_resp = |_node_idx: usize, _: u64| {
                    response_counter.fetch_add(1, Ordering::Relaxed);
                };

                // Send to all peers
                for peer in 0..n {
                    if peer != id {
                        for i in 0..msgs_per_pair {
                            loop {
                                match node.call(peer, id as u64 * 1_000_000 + i as u64, node_idx) {
                                    Ok(_) => break,
                                    Err(SendError::Full(_)) => {
                                        node.poll(&mut on_resp);
                                        // Process any incoming requests
                                        while let Some(handle) = node.try_recv() {
                                            let data = handle.data();
                                            handle.reply(data);
                                        }
                                        std::hint::spin_loop();
                                    }
                                    Err(e) => panic!("send error: {:?}", e),
                                }
                            }
                        }
                    }
                }

                // Continue processing until all expected messages received
                let expected_responses = ((n - 1) * msgs_per_pair) as u64;
                let expected_requests = ((n - 1) * msgs_per_pair) as u64;
                let mut requests_processed = 0u64;

                while response_counter.load(Ordering::Relaxed) < expected_responses
                    || requests_processed < expected_requests
                {
                    node.poll(&mut on_resp);
                    while let Some(handle) = node.try_recv() {
                        let val = handle.data();
                        assert_eq!(val / 1_000_000, handle.from() as u64);
                        handle.reply(val);
                        requests_processed += 1;
                    }
                }

                response_counter.load(Ordering::Relaxed)
            })
        })
        .collect();

    for h in handles {
        let count = h.join().unwrap();
        assert_eq!(count, ((n - 1) * msgs_per_pair) as u64);
    }
}

#[test]
fn test_simple_ping_pong() {
    // Simple synchronous ping-pong test between 2 nodes
    let response_received = Arc::new(AtomicU64::new(0));
    let response_clone = Arc::clone(&response_received);

    let mut nodes: Vec<Flux<u64, u64>> = create_flux(2, 64, 32);

    let mut on_resp = move |expected: u64, response: u64| {
        assert_eq!(expected, response);
        response_clone.fetch_add(1, Ordering::Relaxed);
    };

    let iterations = 100u64;

    // Node 0 sends, Node 1 responds
    for i in 0..iterations {
        // Send request
        nodes[0].call(1, i, i).unwrap();
        nodes[0].poll(&mut on_resp); // flush

        // Node 1 receives and replies
        nodes[1].poll(|_, _| {});
        let handle = nodes[1].try_recv().unwrap();
        assert_eq!(handle.data(), i);
        let data = handle.data();
        handle.reply(data);
        nodes[1].poll(|_, _| {}); // flush reply

        // Node 0 receives response (via callback)
        nodes[0].poll(&mut on_resp);
    }

    assert_eq!(response_received.load(Ordering::Relaxed), iterations);
}
