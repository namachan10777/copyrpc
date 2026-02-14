//! Common transport traits shared by RC and DC implementations.

use crate::error;

pub trait RpcEndpoint<U> {
    fn call(
        &self,
        data: &[u8],
        user_data: U,
        response_allowance: u64,
    ) -> std::result::Result<u32, error::CallError<U>>;
}

pub trait RpcRecvHandle {
    fn data(&self) -> &[u8];
    fn reply(&self, data: &[u8]) -> error::Result<()>;
}

pub trait RpcContext<U> {
    type RecvHandle<'a>: RpcRecvHandle
    where
        Self: 'a,
        U: 'a;

    fn poll<F>(&self, on_response: F)
    where
        F: FnMut(U, &[u8]);

    fn flush_endpoints(&self);

    fn recv(&self) -> Option<Self::RecvHandle<'_>>;
}

impl<U> RpcEndpoint<U> for crate::rc::Endpoint<U> {
    fn call(
        &self,
        data: &[u8],
        user_data: U,
        response_allowance: u64,
    ) -> std::result::Result<u32, error::CallError<U>> {
        self.call(data, user_data, response_allowance)
    }
}

impl<U> RpcEndpoint<U> for crate::dc::Endpoint<U> {
    fn call(
        &self,
        data: &[u8],
        user_data: U,
        response_allowance: u64,
    ) -> std::result::Result<u32, error::CallError<U>> {
        self.call(data, user_data, response_allowance)
    }
}

impl<U> RpcRecvHandle for crate::rc::RecvHandle<'_, U> {
    fn data(&self) -> &[u8] {
        self.data()
    }

    fn reply(&self, data: &[u8]) -> error::Result<()> {
        self.reply(data)
    }
}

impl<U> RpcRecvHandle for crate::dc::RecvHandle<'_, U> {
    fn data(&self) -> &[u8] {
        self.data()
    }

    fn reply(&self, data: &[u8]) -> error::Result<()> {
        self.reply(data)
    }
}

impl<U> RpcContext<U> for crate::rc::Context<U> {
    type RecvHandle<'a>
        = crate::rc::RecvHandle<'a, U>
    where
        Self: 'a,
        U: 'a;

    fn poll<F>(&self, on_response: F)
    where
        F: FnMut(U, &[u8]),
    {
        self.poll(on_response)
    }

    fn flush_endpoints(&self) {
        self.flush_endpoints()
    }

    fn recv(&self) -> Option<Self::RecvHandle<'_>> {
        self.recv()
    }
}

impl<U> RpcContext<U> for crate::dc::Context<U> {
    type RecvHandle<'a>
        = crate::dc::RecvHandle<'a, U>
    where
        Self: 'a,
        U: 'a;

    fn poll<F>(&self, on_response: F)
    where
        F: FnMut(U, &[u8]),
    {
        self.poll(on_response)
    }

    fn flush_endpoints(&self) {
        self.flush_endpoints()
    }

    fn recv(&self) -> Option<Self::RecvHandle<'_>> {
        self.recv()
    }
}
