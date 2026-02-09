# mlx5 crate test coverage review (gaps)

This document summarizes gaps found in the current `mlx5` integration tests. The focus is on coverage of batched doorbell usage and large request volumes, plus other notable missing cases.

## Missing / weakly covered cases

### Batched doorbell and high-volume WQE posting

The current tests primarily post a single WQE and immediately ring the doorbell, then poll for one completion. There is no explicit coverage for:

- Posting many WQEs before ringing the doorbell (batched doorbell).
- Large WQE bursts (e.g., near `max_wr`, or exceeding it) to validate queue management, wrap-around, and completion ordering.
- CQ polling in batches for a large number of completions (e.g., `poll_cq_batch`).

**Where this shows up**

- RC: single-WQE post paths only (`mlx5/tests/rc_tests.rs`).
- DC: single-WQE post paths only (`mlx5/tests/dc_tests.rs`).
- UD: single-WQE post paths only (`mlx5/tests/ud_tests.rs`).
- SRQ/TM-SRQ: small fixed receive count (10) without testing near-capacity limits (`mlx5/tests/srq_tests.rs`, `mlx5/tests/tm_tests.rs`).

### SRQ/TM-SRQ boundary conditions

- SRQ receive posting is limited to 10 WQEs; no tests approach `max_wr` or validate queue full/error behavior.
- TM-SRQ unordered receives are also limited to 10 WQEs; no stress or boundary tests.

### TM + DC integration

The test `test_tm_tag_matching_with_dc` is setup-only and explicitly skips the full integration (no DCT bound to TM-SRQ), so tag matching with DC traffic is unverified.

## Suggested additions (for future tests)

- Add a batched doorbell test for RC/DC/UD that posts N WQEs (e.g., 128/256) before a single ring, then polls completions in a batch.
- Add a “near `max_wr`” SRQ/TM-SRQ receive posting test to verify queue depth handling and wrap-around.
- Add a stress test variant that posts > `max_wr` to confirm correct error handling (likely `#[ignore]` or behind a feature).
- Implement full TM + DC integration once the API exposes binding DCT to TM-SRQ.

## Notes

- Current tests validate basic correctness of single operations (send/recv, rdma read/write, atomics), but do not cover high-volume or batched-post scenarios.

