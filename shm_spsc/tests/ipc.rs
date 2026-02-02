//! Integration tests for shm_spsc inter-process communication.

use shm_spsc::{Client, RecvError, SendError, Server};
use std::thread;
use std::time::Duration;

/// Test basic server-client communication using threads (simulated IPC).
#[test]
fn test_basic_communication() {
    let name = format!("/shm_spsc_basic_{}", std::process::id());
    let capacity = 64;

    unsafe {
        let mut server = Server::<u64>::create(&name, capacity).unwrap();

        let name_clone = name.clone();
        let client_thread = thread::spawn(move || {
            // Small delay to ensure server is ready
            thread::sleep(Duration::from_millis(10));
            let mut client = Client::<u64>::connect(&name_clone, capacity).unwrap();

            // Receive from server
            for i in 0..100 {
                let val = client.recv().unwrap();
                assert_eq!(val, i);
            }

            // Send to server
            for i in 0..100 {
                client.send(i + 1000).unwrap();
            }
        });

        // Wait for client to connect
        while !server.is_connected() {
            thread::sleep(Duration::from_millis(1));
        }

        // Send to client
        for i in 0..100 {
            server.send(i).unwrap();
        }

        // Receive from client
        for i in 0..100 {
            let val = server.recv().unwrap();
            assert_eq!(val, i + 1000);
        }

        client_thread.join().unwrap();
    }
}

/// Test high-throughput communication.
#[test]
fn test_high_throughput() {
    let name = format!("/shm_spsc_throughput_{}", std::process::id());
    let capacity = 4096;
    let count = 1_000_000u64;

    unsafe {
        let mut server = Server::<u64>::create(&name, capacity).unwrap();

        let name_clone = name.clone();
        let client_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            let mut client = Client::<u64>::connect(&name_clone, capacity).unwrap();

            for i in 0..count {
                let val = client.recv().unwrap();
                assert_eq!(val, i);
            }
        });

        while !server.is_connected() {
            thread::sleep(Duration::from_millis(1));
        }

        for i in 0..count {
            server.send(i).unwrap();
        }

        client_thread.join().unwrap();
    }
}

/// Test ping-pong latency.
#[test]
fn test_ping_pong() {
    let name = format!("/shm_spsc_pingpong_{}", std::process::id());
    let capacity = 64;
    let iterations = 10000;

    unsafe {
        let mut server = Server::<u64>::create(&name, capacity).unwrap();

        let name_clone = name.clone();
        let client_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            let mut client = Client::<u64>::connect(&name_clone, capacity).unwrap();

            for _ in 0..iterations {
                let val = client.recv().unwrap();
                client.send(val + 1).unwrap();
            }
        });

        while !server.is_connected() {
            thread::sleep(Duration::from_millis(1));
        }

        for i in 0..iterations {
            server.send(i).unwrap();
            let val = server.recv().unwrap();
            assert_eq!(val, i + 1);
        }

        client_thread.join().unwrap();
    }
}

/// Test server disconnect detection.
#[test]
fn test_server_disconnect() {
    let name = format!("/shm_spsc_server_disc_{}", std::process::id());
    let capacity = 64;

    unsafe {
        let name_clone = name.clone();
        let client_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            let mut client = Client::<u64>::connect(&name_clone, capacity).unwrap();

            // Try to receive after server is gone
            thread::sleep(Duration::from_millis(100));

            // Server should be disconnected
            assert!(client.is_disconnected());

            // Receive should return Disconnected
            match client.try_recv() {
                Err(RecvError::Disconnected) | Err(RecvError::Empty) => {}
                other => panic!("expected Disconnected or Empty, got {:?}", other),
            }
        });

        {
            let _server = Server::<u64>::create(&name, capacity).unwrap();
            // Wait for client to connect
            thread::sleep(Duration::from_millis(80));
            // Server drops here
        }

        client_thread.join().unwrap();
    }
}

/// Test client disconnect detection.
#[test]
fn test_client_disconnect() {
    let name = format!("/shm_spsc_client_disc_{}", std::process::id());
    let capacity = 64;

    unsafe {
        let mut server = Server::<u64>::create(&name, capacity).unwrap();

        let name_clone = name.clone();
        let client_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            let _client = Client::<u64>::connect(&name_clone, capacity).unwrap();
            // Client drops immediately
        });

        // Wait for client to connect and disconnect
        client_thread.join().unwrap();
        thread::sleep(Duration::from_millis(10));

        assert!(server.is_disconnected());

        // Send should return Disconnected
        match server.try_send(42) {
            Err(SendError::Disconnected(42)) => {}
            other => panic!("expected Disconnected, got {:?}", other),
        }
    }
}

/// Test with array type.
#[test]
fn test_array_type() {
    let name = format!("/shm_spsc_array_{}", std::process::id());
    let capacity = 64;

    unsafe {
        let mut server = Server::<[u8; 64]>::create(&name, capacity).unwrap();

        let name_clone = name.clone();
        let client_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            let mut client = Client::<[u8; 64]>::connect(&name_clone, capacity).unwrap();

            let data = client.recv().unwrap();
            assert_eq!(data[0], 0xAB);
            assert_eq!(data[63], 0xCD);
        });

        while !server.is_connected() {
            thread::sleep(Duration::from_millis(1));
        }

        let mut data = [0u8; 64];
        data[0] = 0xAB;
        data[63] = 0xCD;
        server.send(data).unwrap();

        client_thread.join().unwrap();
    }
}

/// Test capacity boundary conditions.
#[test]
fn test_capacity_boundary() {
    let name = format!("/shm_spsc_cap_{}", std::process::id());
    let capacity = 4; // Small capacity

    unsafe {
        let mut server = Server::<u64>::create(&name, capacity).unwrap();

        let name_clone = name.clone();
        let client_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            let mut client = Client::<u64>::connect(&name_clone, capacity).unwrap();

            // Drain slowly
            for i in 0..10 {
                thread::sleep(Duration::from_millis(5));
                let val = client.recv().unwrap();
                assert_eq!(val, i);
            }
        });

        while !server.is_connected() {
            thread::sleep(Duration::from_millis(1));
        }

        // Try to fill up the channel
        for i in 0..10 {
            server.send(i).unwrap();
        }

        client_thread.join().unwrap();
    }
}

/// Test that channel names with different formats work.
#[test]
fn test_channel_name_formats() {
    // Without leading slash
    let name1 = format!("shm_test_name1_{}", std::process::id());
    unsafe {
        let server = Server::<u64>::create(&name1, 64).unwrap();
        let _client = Client::<u64>::connect(&name1, 64).unwrap();
        drop(server);
    }

    // With leading slash
    let name2 = format!("/shm_test_name2_{}", std::process::id());
    unsafe {
        let server = Server::<u64>::create(&name2, 64).unwrap();
        let _client = Client::<u64>::connect(&name2, 64).unwrap();
        drop(server);
    }
}
