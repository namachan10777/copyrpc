use crate::{CallError, MpscCaller, Serial};

/// Wraps an `MpscCaller` to attach user_data to each request and invoke a callback when responses arrive.
///
/// This adapter allows tracking arbitrary user_data (type `U`) alongside each RPC call, and automatically
/// invokes a callback `F: FnMut(U, Resp)` when the corresponding response is received.
///
/// # Generic Parameters
/// - `C`: Inner caller implementing `MpscCaller<Req, Resp>`
/// - `Req`, `Resp`: Serializable request/response types
/// - `U`: User data type (arbitrary state to track per request)
/// - `F`: Response callback closure taking `(user_data, response)`
///
/// # Internal Storage
/// Uses a fixed-capacity `Vec<Option<U>>` indexed by `token % capacity`. This works correctly for:
/// - Onesided transport: tokens are slot indices (0..capacity), so no collisions
/// - Fastforward/Lamport: tokens are monotonic, capacity must be >= max_inflight to avoid collisions
///
/// # Example
/// ```ignore
/// let caller = /* some MpscCaller */;
/// let mut with_ud = CallerWithUserData::new(caller, 128, |ctx, resp| {
///     println!("Request {} completed with response {:?}", ctx, resp);
/// });
/// with_ud.call(request, 42)?;
/// with_ud.poll(); // Invokes callback with (42, response)
/// ```
pub struct CallerWithUserData<C, Req, Resp, U, F>
where
    C: MpscCaller<Req, Resp>,
    Req: Serial,
    Resp: Serial,
    F: FnMut(U, Resp),
{
    caller: C,
    user_data: Vec<Option<U>>,
    on_response: F,
    _phantom: std::marker::PhantomData<(Req, Resp)>,
}

impl<C, Req, Resp, U, F> CallerWithUserData<C, Req, Resp, U, F>
where
    C: MpscCaller<Req, Resp>,
    Req: Serial,
    Resp: Serial,
    F: FnMut(U, Resp),
{
    /// Creates a new `CallerWithUserData` wrapper.
    ///
    /// # Parameters
    /// - `caller`: Inner `MpscCaller` to wrap
    /// - `capacity`: Size of the user_data slot array (must be >= max inflight requests)
    /// - `on_response`: Callback invoked for each completed response as `on_response(user_data, response)`
    pub fn new(caller: C, capacity: usize, on_response: F) -> Self {
        Self {
            caller,
            user_data: (0..capacity).map(|_| None).collect(),
            on_response,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Issues a request with associated user_data.
    ///
    /// On success, stores `user_data` indexed by the returned token, ready to be retrieved when the response arrives.
    ///
    /// # Errors
    /// Returns `CallError` if the inner caller fails (queue full, inflight exceeded, or disconnected).
    pub fn call(&mut self, req: Req, user_data: U) -> Result<(), CallError<Req>> {
        match self.caller.call(req) {
            Ok(token) => {
                let slot = token as usize % self.user_data.len();
                self.user_data[slot] = Some(user_data);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Polls for completed responses and invokes callbacks.
    ///
    /// Calls `sync()` on the inner caller to flush pending writes, then drains all available responses
    /// via `try_recv_response()`. For each response, retrieves the associated user_data and invokes
    /// `on_response(user_data, response)`.
    pub fn poll(&mut self) {
        self.caller.sync();
        while let Some((token, resp)) = self.caller.try_recv_response() {
            let slot = token as usize % self.user_data.len();
            if let Some(ud) = self.user_data[slot].take() {
                (self.on_response)(ud, resp);
            }
        }
    }

    /// Returns the number of inflight requests in the inner caller.
    pub fn pending_count(&self) -> usize {
        self.caller.pending_count()
    }

    /// Returns a reference to the inner caller.
    pub fn caller(&self) -> &C {
        &self.caller
    }

    /// Returns a mutable reference to the inner caller.
    pub fn caller_mut(&mut self) -> &mut C {
        &mut self.caller
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::rc::Rc;

    // Mock request/response types
    #[derive(Debug, Copy, Clone, PartialEq)]
    struct Req(u64);
    unsafe impl Serial for Req {}

    #[derive(Debug, Copy, Clone, PartialEq)]
    struct Resp(u64);
    unsafe impl Serial for Resp {}

    // Mock MpscCaller implementation for testing
    struct MockCaller {
        pending: VecDeque<(u64, Req)>,
        responses: VecDeque<(u64, Resp)>,
        next_token: u64,
        max_inflight: usize,
    }

    impl MockCaller {
        fn new(max_inflight: usize) -> Self {
            Self {
                pending: VecDeque::new(),
                responses: VecDeque::new(),
                next_token: 0,
                max_inflight,
            }
        }

        // Test helper: simulate server responding to the oldest pending request
        fn simulate_response(&mut self) {
            if let Some((token, req)) = self.pending.pop_front() {
                // Echo request value in response
                self.responses.push_back((token, Resp(req.0 * 2)));
            }
        }
    }

    impl MpscCaller<Req, Resp> for MockCaller {
        fn call(&mut self, req: Req) -> Result<u64, CallError<Req>> {
            if self.pending.len() >= self.max_inflight {
                return Err(CallError::InflightExceeded(req));
            }
            let token = self.next_token;
            self.next_token += 1;
            self.pending.push_back((token, req));
            Ok(token)
        }

        fn sync(&mut self) {
            // No-op for mock
        }

        fn try_recv_response(&mut self) -> Option<(u64, Resp)> {
            self.responses.pop_front()
        }

        fn pending_count(&self) -> usize {
            self.pending.len()
        }
    }

    #[test]
    fn test_basic_call_with_user_data() {
        let mock = MockCaller::new(8);
        let responses = Rc::new(RefCell::new(Vec::new()));
        let responses_clone = responses.clone();
        
        let mut caller = CallerWithUserData::new(mock, 8, move |ud: String, resp| {
            responses_clone.borrow_mut().push((ud, resp));
        });

        // Issue a call with user_data
        caller.call(Req(100), "first".to_string()).unwrap();
        assert_eq!(caller.pending_count(), 1);

        // Simulate server response
        caller.caller_mut().simulate_response();

        // Poll to receive response
        caller.poll();

        // Verify callback was invoked with correct user_data and response
        let resp_vec = responses.borrow();
        assert_eq!(resp_vec.len(), 1);
        assert_eq!(resp_vec[0].0, "first");
        assert_eq!(resp_vec[0].1, Resp(200)); // 100 * 2
        drop(resp_vec);
        assert_eq!(caller.pending_count(), 0);
    }

    #[test]
    fn test_multiple_calls_with_different_user_data() {
        let mock = MockCaller::new(16);
        let responses = Rc::new(RefCell::new(Vec::new()));
        let responses_clone = responses.clone();
        
        let mut caller = CallerWithUserData::new(mock, 16, move |ud: u32, resp| {
            responses_clone.borrow_mut().push((ud, resp));
        });

        // Issue multiple calls with different user_data
        caller.call(Req(10), 1000).unwrap();
        caller.call(Req(20), 2000).unwrap();
        caller.call(Req(30), 3000).unwrap();

        assert_eq!(caller.pending_count(), 3);

        // Simulate responses in order
        caller.caller_mut().simulate_response(); // token 0, req 10
        caller.caller_mut().simulate_response(); // token 1, req 20

        caller.poll();

        // Verify first two responses
        {
            let resp_vec = responses.borrow();
            assert_eq!(resp_vec.len(), 2);
            assert_eq!(resp_vec[0], (1000, Resp(20))); // 10 * 2
            assert_eq!(resp_vec[1], (2000, Resp(40))); // 20 * 2
        }
        assert_eq!(caller.pending_count(), 1);

        // Simulate third response
        caller.caller_mut().simulate_response(); // token 2, req 30
        caller.poll();

        // Verify third response
        let resp_vec = responses.borrow();
        assert_eq!(resp_vec.len(), 3);
        assert_eq!(resp_vec[2], (3000, Resp(60))); // 30 * 2
        drop(resp_vec);
        assert_eq!(caller.pending_count(), 0);
    }

    #[test]
    fn test_correct_user_data_response_pairing() {
        let mock = MockCaller::new(32);
        let responses = Rc::new(RefCell::new(Vec::new()));
        let responses_clone = responses.clone();
        
        let mut caller = CallerWithUserData::new(mock, 32, move |ud: (usize, String), resp| {
            responses_clone.borrow_mut().push((ud, resp));
        });

        // Issue requests with tagged user_data
        for i in 0..10 {
            let user_data = (i, format!("req_{}", i));
            caller.call(Req(i as u64), user_data).unwrap();
        }

        // Respond in arbitrary order (FIFO for this mock, but test verifies pairing)
        for _ in 0..10 {
            caller.caller_mut().simulate_response();
        }

        caller.poll();

        // Verify all 10 responses arrived with correct pairing
        let resp_vec = responses.borrow();
        assert_eq!(resp_vec.len(), 10);
        for (i, ((id, tag), resp)) in resp_vec.iter().enumerate() {
            assert_eq!(*id, i);
            assert_eq!(tag, &format!("req_{}", i));
            assert_eq!(resp.0, (i as u64) * 2);
        }
    }

    #[test]
    fn test_inflight_exceeded_error() {
        let mock = MockCaller::new(2);
        let mut caller = CallerWithUserData::new(mock, 4, |_ud: (), _resp| {});

        // Fill up to max_inflight
        caller.call(Req(1), ()).unwrap();
        caller.call(Req(2), ()).unwrap();

        // Third call should fail with InflightExceeded
        let result = caller.call(Req(3), ());
        assert!(matches!(result, Err(CallError::InflightExceeded(_))));
    }

    #[test]
    fn test_token_wrapping_with_modulo() {
        // Test that token % capacity works correctly even with large token values
        let mock = MockCaller::new(64);
        let responses = Rc::new(RefCell::new(Vec::new()));
        let responses_clone = responses.clone();
        let capacity = 8; // Small capacity to force modulo wrapping
        
        let mut caller = CallerWithUserData::new(mock, capacity, move |ud: u64, resp| {
            responses_clone.borrow_mut().push((ud, resp));
        });

        // Manually set high token value to simulate wrapping
        caller.caller_mut().next_token = 1000;

        // Issue calls that will have tokens 1000, 1001, 1002
        caller.call(Req(10), 100).unwrap();
        caller.call(Req(20), 200).unwrap();
        caller.call(Req(30), 300).unwrap();

        // Simulate responses
        for _ in 0..3 {
            caller.caller_mut().simulate_response();
        }

        caller.poll();

        // Verify correct pairing despite token wrapping
        let resp_vec = responses.borrow();
        assert_eq!(resp_vec.len(), 3);
        assert_eq!(resp_vec[0], (100, Resp(20)));
        assert_eq!(resp_vec[1], (200, Resp(40)));
        assert_eq!(resp_vec[2], (300, Resp(60)));
    }

    #[test]
    fn test_poll_multiple_times() {
        let mock = MockCaller::new(16);
        let responses = Rc::new(RefCell::new(Vec::new()));
        let responses_clone = responses.clone();
        
        let mut caller = CallerWithUserData::new(mock, 16, move |ud: usize, resp| {
            responses_clone.borrow_mut().push((ud, resp));
        });

        // Issue calls
        caller.call(Req(1), 10).unwrap();
        caller.call(Req(2), 20).unwrap();

        // Poll with no responses ready
        caller.poll();
        assert_eq!(responses.borrow().len(), 0);

        // Simulate one response
        caller.caller_mut().simulate_response();
        caller.poll();
        {
            let resp_vec = responses.borrow();
            assert_eq!(resp_vec.len(), 1);
            assert_eq!(resp_vec[0], (10, Resp(2)));
        }

        // Poll again with no new responses
        caller.poll();
        assert_eq!(responses.borrow().len(), 1);

        // Simulate second response
        caller.caller_mut().simulate_response();
        caller.poll();
        let resp_vec = responses.borrow();
        assert_eq!(resp_vec.len(), 2);
        assert_eq!(resp_vec[1], (20, Resp(4)));
    }
}
