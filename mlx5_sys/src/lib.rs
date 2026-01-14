#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(clippy::all)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use std::ops::{Deref, DerefMut};

/// Inner packed struct for mlx5_wqe_ctrl_seg.
/// Rust doesn't support both packed and aligned attributes simultaneously,
/// so we use this inner struct with packed layout and wrap it in an aligned outer struct.
#[repr(C, packed)]
#[derive(Debug, Copy, Clone)]
pub struct mlx5_wqe_ctrl_seg_inner {
    pub opmod_idx_opcode: __be32,
    pub qpn_ds: __be32,
    pub signature: u8,
    pub dci_stream_channel_id: __be16,
    pub fm_ce_se: u8,
    pub imm: __be32,
}

/// mlx5_wqe_ctrl_seg with correct layout: packed fields with 4-byte alignment.
/// C definition: __attribute__((__packed__)) __attribute__((__aligned__(4)))
#[repr(C, align(4))]
#[derive(Debug, Copy, Clone)]
pub struct mlx5_wqe_ctrl_seg(pub mlx5_wqe_ctrl_seg_inner);

impl Deref for mlx5_wqe_ctrl_seg {
    type Target = mlx5_wqe_ctrl_seg_inner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for mlx5_wqe_ctrl_seg {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
