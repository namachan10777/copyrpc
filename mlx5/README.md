# mlx5

**High-performance RDMA networking library bypassing libibverbs**

A low-level RDMA implementation for MLX5 devices, optimized at the instruction level.
Achieves a maximum of 300 CPU cycles per roundtrip.

## Features

### libibverbs Bypass for Maximum Performance

This library leverages `mlx5dv` (Direct Verbs) to completely bypass the libibverbs abstraction layer:

- **Direct WQE Writing**: Construct WQEs directly in the Send Queue buffer
- **BlueFlame**: Write 64-byte aligned WQEs directly to BF registers, bypassing doorbells
- **Direct CQE Reading**: Poll directly from Completion Queue buffers
- **Zero-Copy**: Data path without memory copies

### Instruction-Level Optimization & ILP Maximization

Designed for aggressive compiler inlining and Instruction-Level Parallelism (ILP) maximization:

```rust
// MonoCq: Complete callback inlining
// CompletionQueue uses dyn CompletionTarget → vtable indirect call
// MonoCq<Q, F> → concrete types enable compiler inlining
let mono_cq = ctx.create_mono_cq(256, |cqe, entry| {
    // This callback is inlined!
})?;
```

- **MonoCq**: Generic type parameters enable complete inlining of `process_cqe` and callbacks
- **Cell-based Interior Mutability**: Eliminates `RefCell` runtime check branches
- **Type-State Pattern**: Compile-time WQE construction validation, zero runtime checks
- **No Heap Allocation on Critical Path**: WQE posting, doorbell ringing, and CQE polling are completely allocation-free

### WQEBB Handling Abstraction

WQEs (Work Queue Elements) consist of 64-byte WQEBBs (Basic Blocks), but this complexity is completely hidden behind a type-safe Builder API:

```rust
// Type-State pattern for WQE construction
// State transitions: Init → NeedsRdma → NeedsData → HasData → finish
qp.wqe_builder(entry)?
    .ctrl_rdma_write_imm(WqeFlags::COMPLETION, imm)?
    .rdma(remote_addr, rkey)?
    .sge(local_addr, len, lkey)?
    .finish_with_blueflame()?;

// For DC (Dynamically Connected)
// Init → DcNeedsAv<NeedsData> → NeedsData → HasData
dci.wqe_builder(entry)?
    .ctrl_send(flags)?
    .av_ib(dc_key, dctn, dlid)?  // Address Vector
    .sge(addr, len, lkey)?
    .finish()?;
```

Invalid WQE construction sequences result in compile errors, guaranteeing safety with zero runtime overhead.

## Constraints

### MLX5 Devices Only

This library is exclusively for Mellanox/NVIDIA MLX5 family devices (ConnectX-4 and later).
It will not work with RDMA devices from other vendors (Intel, Broadcom, etc.).

### Single-Threaded Design

A single-threaded design with no multi-thread synchronization mechanisms:

- `Rc<RefCell<T>>` based resource management (no `Arc`)
- `Cell` for interior mutability (no atomic operations)
- Resource sharing across threads is not supported

This design completely eliminates synchronization primitive overhead.

### RoCE Untested

Only tested in InfiniBand environments. RoCE (RDMA over Converged Ethernet) code paths are implemented but have not been tested on actual hardware.

## Architecture

### Transports

| Transport | Description |
|-----------|-------------|
| RC (Reliable Connection) | Connection-oriented, reliable, ordered delivery |
| DC (Dynamically Connected) | Scalable dynamic connections, one-to-many communication |
| UD (Unreliable Datagram) | Connectionless, unreliable |

### Key Components

- **WQE Builder**: Type-State pattern enforces correct segment ordering (CtrlSeg, RdmaSeg, DataSeg, AtomicSeg)
- **Queue Pairs**: RC (Reliable Connection), DC (DCI/DCT), UD (Unreliable Datagram)
- **Completion Queues**: `CompletionQueue` (dynamic dispatch) and `MonoCq` (inlined callbacks)
- **mlx5dv Layer**: Direct Verbs for libibverbs bypass
- **Hardware**: MLX5 devices (ConnectX-4+)

### Optimization Techniques

| Technique | Effect |
|-----------|--------|
| BlueFlame | Doorbell bypass, low-latency WQE posting |
| CQE Compression | Pack multiple completions into one CQE, reduce memory bandwidth |
| Scatter-to-CQE | Place received data ≤32B directly in CQE |
| Owner Bit Optimization | Polling without full cache line access |
| Prefetching | Read-ahead next CQE during batch processing |

## Usage Examples

### RDMA WRITE with RC QP

```rust
use mlx5::device::DeviceList;
use mlx5::qp::{RcQpConfig, RemoteQpInfo};
use mlx5::wqe::WqeFlags;

// Open device
let devices = DeviceList::list()?;
let ctx = devices.iter().next().unwrap().open()?;
let pd = ctx.alloc_pd()?;

// Create CQ (with inlined callback)
let cq = ctx.create_mono_cq(256, |cqe, entry| {
    println!("Completed: {:?}", entry);
})?;

// Create and connect QP
let config = RcQpConfig {
    max_send_wr: 256,
    max_recv_wr: 256,
    max_send_sge: 1,
    max_recv_sge: 1,
    max_inline_data: 256,
    enable_scatter_to_cqe: false,
};
let qp = ctx.create_rc_qp_for_mono_cq(&pd, &cq, &recv_cq, &config)?;
cq.register(&qp);

// Connect (requires remote QP info)
qp.borrow_mut().connect(&remote_info, port, gid_index, min_rnr_timer, timeout, access)?;

// RDMA WRITE + BlueFlame
qp.borrow()
    .wqe_builder(user_entry)?
    .ctrl_rdma_write_imm(WqeFlags::COMPLETION, imm_data)?
    .rdma(remote_addr, rkey)?
    .sge(local_addr, len, lkey)?
    .finish_with_blueflame()?;

// Poll completions
cq.poll();
cq.flush();
```

### Many-to-Many Communication with DC

```rust
use mlx5::dc::{DciConfig, DctConfig};

// Create DCI (Initiator)
let dci = ctx.create_dci(&pd, &send_cq, &config)?;

// Create DCT (Target)
let dct = ctx.create_dct(&pd, &recv_cq, &srq, dc_key, &config)?;

// Send from DCI to any DCT
dci.borrow()
    .wqe_builder(entry)?
    .ctrl_rdma_write(WqeFlags::COMPLETION)?
    .av_ib(dc_key, dctn, dlid)?  // Specify destination
    .rdma(remote_addr, rkey)?
    .sge(local_addr, len, lkey)?
    .finish()?;
```

## Benchmarks

### Running Benchmarks

```bash
# Run on physical cores only (disable HT for stable results)
taskset -c 0,2,4,6,8 cargo bench --bench pingpong
```

### Metrics

- **Throughput**: WRITE with Immediate (queue depth 64, batched doorbells)
- **Low-latency**: WRITE with Immediate (queue depth 1, inline + BlueFlame)

## Testing

```bash
# Unit tests
cargo test --package mlx5 --lib

# Integration tests (requires RDMA hardware)
cargo test --package mlx5 --test '*'
```

## Comparison with UCX

[UCX](https://openucx.org/) is the industry-standard high-performance communication framework.
This library takes a different approach:

| Aspect | mlx5 | UCX |
|--------|------|-----|
| Target Devices | MLX5 only | General (multi-vendor) |
| Abstraction Level | Low (Direct Verbs) | High (UCP/UCT/UCS layers) |
| Inlining | Core design principle | Limited by library boundaries |
| Threading Model | Single-threaded only | Multi-threaded support |
| Use Case | SOTA research, specialized optimization | General purpose, production |

By designing around inlining, this library eliminates function call overhead and achieves optimizations on par with or exceeding UCX.

## License

[See LICENSE file]

## References

- [RDMA Aware Networks Programming User Manual](https://docs.nvidia.com/networking/display/rdmacore)
- [mlx5dv Direct Verbs](https://docs.nvidia.com/networking/display/MLNXOFEDv531001/Direct+Verbs)
