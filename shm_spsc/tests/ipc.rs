//! Integration tests for shm_spsc RPC slot communication.

use shm_spsc::{CallError, Client, ConnectError, RespondError, Server};
use std::thread;
use std::time::Duration;

/// Test basic RPC call.
#[test]
fn test_basic_rpc() {
    let name = format!("/shm_rpc_ipc_basic_{}", std::process::id());

    unsafe {
        let mut server = Server::<u64, u64>::create(&name, 4).unwrap();

        let name_clone = name.clone();
        let client_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            let mut client = Client::<u64, u64>::connect(&name_clone).unwrap();

            for i in 0..100u64 {
                let resp = client.call(i).unwrap();
                assert_eq!(resp, i + 1);
            }
        });

        let mut served = 0;
        while served < 100 {
            if let Some((cid, req)) = server.try_poll() {
                server.respond(cid, req + 1).unwrap();
                served += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        client_thread.join().unwrap();
    }
}

/// Test multiple clients concurrently.
#[test]
fn test_multi_client() {
    let name = format!("/shm_rpc_ipc_multi_{}", std::process::id());
    let num_clients = 4u32;
    let calls_per_client = 50u64;

    unsafe {
        let mut server =
            Server::<u64, u64>::create(&name, num_clients).unwrap();

        let mut handles = Vec::new();
        for client_idx in 0..num_clients {
            let name_clone = name.clone();
            handles.push(thread::spawn(move || {
                thread::sleep(Duration::from_millis(10 + client_idx as u64 * 5));
                let mut client = Client::<u64, u64>::connect(&name_clone).unwrap();

                for i in 0..calls_per_client {
                    let req = client_idx as u64 * 1000 + i;
                    let resp = client.call(req).unwrap();
                    assert_eq!(resp, req * 2);
                }
            }));
        }

        let total = num_clients as u64 * calls_per_client;
        let mut served = 0u64;
        while served < total {
            if let Some((cid, req)) = server.try_poll() {
                server.respond(cid, req * 2).unwrap();
                served += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        for h in handles {
            h.join().unwrap();
        }
    }
}

/// Test server disconnect detection.
#[test]
fn test_server_disconnect() {
    let name = format!("/shm_rpc_ipc_sdisc_{}", std::process::id());

    unsafe {
        let name_clone = name.clone();
        let client_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            let mut client = Client::<u64, u64>::connect(&name_clone).unwrap();

            // Wait for server to die
            thread::sleep(Duration::from_millis(100));

            assert!(!client.is_server_alive());

            // call should return ServerDisconnected
            match client.call(42) {
                Err(CallError::ServerDisconnected) => {}
                other => panic!("expected ServerDisconnected, got {:?}", other),
            }
        });

        {
            let _server = Server::<u64, u64>::create(&name, 4).unwrap();
            thread::sleep(Duration::from_millis(80));
            // server drops here
        }

        client_thread.join().unwrap();
    }
}

/// Test client disconnect detection.
#[test]
fn test_client_disconnect() {
    let name = format!("/shm_rpc_ipc_cdisc_{}", std::process::id());

    unsafe {
        let server = Server::<u64, u64>::create(&name, 4).unwrap();

        let name_clone = name.clone();
        let client_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            let client = Client::<u64, u64>::connect(&name_clone).unwrap();
            let cid = client.id();
            // client drops immediately
            drop(client);
            cid
        });

        let cid = client_thread.join().unwrap();
        thread::sleep(Duration::from_millis(10));

        assert!(!server.is_client_connected(cid));
    }
}

/// Test with array type (different Req/Resp).
#[test]
fn test_array_type() {
    let name = format!("/shm_rpc_ipc_array_{}", std::process::id());

    unsafe {
        let mut server = Server::<[u8; 64], [u8; 128]>::create(&name, 4).unwrap();

        let name_clone = name.clone();
        let client_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            let mut client = Client::<[u8; 64], [u8; 128]>::connect(&name_clone).unwrap();

            let mut req = [0u8; 64];
            req[0] = 0xAB;
            req[63] = 0xCD;

            let resp = client.call(req).unwrap();
            assert_eq!(resp[0], 0xAB);
            assert_eq!(resp[63], 0xCD);
            assert_eq!(resp[64], 0xFF);
            assert_eq!(resp[127], 0xEE);
        });

        loop {
            if let Some((cid, req)) = server.try_poll() {
                let mut resp = [0u8; 128];
                resp[..64].copy_from_slice(&req);
                resp[64] = 0xFF;
                resp[127] = 0xEE;
                server.respond(cid, resp).unwrap();
                break;
            }
            std::hint::spin_loop();
        }

        client_thread.join().unwrap();
    }
}

/// Test server full error.
#[test]
fn test_server_full() {
    let name = format!("/shm_rpc_ipc_full_{}", std::process::id());

    unsafe {
        let _server = Server::<u64, u64>::create(&name, 2).unwrap();

        let _c1 = Client::<u64, u64>::connect(&name).unwrap();
        let _c2 = Client::<u64, u64>::connect(&name).unwrap();

        // Third client should fail
        match Client::<u64, u64>::connect(&name) {
            Err(ConnectError::ServerFull) => {}
            Ok(_) => panic!("expected ServerFull, got Ok"),
            Err(e) => panic!("expected ServerFull, got {:?}", e),
        }
    }
}

/// Test respond errors.
#[test]
fn test_respond_errors() {
    let name = format!("/shm_rpc_ipc_rerr_{}", std::process::id());

    unsafe {
        let mut server = Server::<u64, u64>::create(&name, 4).unwrap();

        // Invalid client
        match server.respond(shm_spsc::ClientId(99), 0) {
            Err(RespondError::InvalidClient) => {}
            other => panic!("expected InvalidClient, got {:?}", other),
        }

        let name_clone = name.clone();
        let _client = Client::<u64, u64>::connect(&name_clone).unwrap();

        // No pending request
        match server.respond(shm_spsc::ClientId(0), 0) {
            Err(RespondError::NoRequest) => {}
            other => panic!("expected NoRequest, got {:?}", other),
        }
    }
}

/// Test channel name formats.
#[test]
fn test_channel_name_formats() {
    // Without leading slash
    let name1 = format!("shm_rpc_name1_{}", std::process::id());
    unsafe {
        let _server = Server::<u64, u64>::create(&name1, 4).unwrap();
        let _client = Client::<u64, u64>::connect(&name1).unwrap();
    }

    // With leading slash
    let name2 = format!("/shm_rpc_name2_{}", std::process::id());
    unsafe {
        let _server = Server::<u64, u64>::create(&name2, 4).unwrap();
        let _client = Client::<u64, u64>::connect(&name2).unwrap();
    }
}

/// Test connected_clients count.
#[test]
fn test_connected_clients() {
    let name = format!("/shm_rpc_ipc_cc_{}", std::process::id());

    unsafe {
        let server = Server::<u64, u64>::create(&name, 4).unwrap();
        assert_eq!(server.connected_clients(), 0);

        let c1 = Client::<u64, u64>::connect(&name).unwrap();
        assert_eq!(server.connected_clients(), 1);

        let c2 = Client::<u64, u64>::connect(&name).unwrap();
        assert_eq!(server.connected_clients(), 2);

        drop(c1);
        assert_eq!(server.connected_clients(), 1);

        drop(c2);
        assert_eq!(server.connected_clients(), 0);
    }
}
