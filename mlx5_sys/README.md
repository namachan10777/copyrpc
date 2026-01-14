# mlx5_sys

Rust FFI bindings for mlx5dv (Mellanox mlx5 Direct Verbs) API.

## Overview

This crate provides low-level bindings to the mlx5dv API, which allows direct access to Mellanox ConnectX hardware features bypassing the kernel.

## Generated Bindings

- 256 `mlx5dv_*` functions
- DevX API for low-level hardware access
- Direct QP/CQ creation and manipulation

## Known Issues

### `mlx5_wqe_ctrl_seg` Layout

The C header defines `mlx5_wqe_ctrl_seg` with both `__packed__` and `__aligned__(4)` attributes:

```c
struct mlx5_wqe_ctrl_seg {
    ...
} __attribute__((__packed__)) __attribute__((__aligned__(4)));
```

Rust doesn't support both `#[repr(packed)]` and `#[repr(align(N))]` simultaneously. To work around this, we:

1. Blocklist `mlx5_wqe_ctrl_seg` from bindgen
2. Manually define `mlx5_wqe_ctrl_seg_inner` with `#[repr(C, packed)]`
3. Wrap it in `mlx5_wqe_ctrl_seg` with `#[repr(C, align(4))]`
4. Implement `Deref`/`DerefMut` for transparent field access

```rust
// Access fields via Deref
let ctrl: mlx5_wqe_ctrl_seg = ...;
ctrl.opmod_idx_opcode = 0x1234;
```
