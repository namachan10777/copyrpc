use mlx5_sys::*;
use std::mem::{align_of, offset_of, size_of};

#[test]
fn test_struct_sizes() {
    // Expected sizes from C (from gcc output)
    assert_eq!(
        size_of::<mlx5_wqe_srq_next_seg>(),
        16,
        "mlx5_wqe_srq_next_seg size mismatch"
    );
    assert_eq!(
        size_of::<mlx5_wqe_data_seg>(),
        16,
        "mlx5_wqe_data_seg size mismatch"
    );
    assert_eq!(
        size_of::<mlx5_wqe_ctrl_seg>(),
        16,
        "mlx5_wqe_ctrl_seg size mismatch"
    );
    assert_eq!(size_of::<mlx5_wqe_av>(), 48, "mlx5_wqe_av size mismatch");
    assert_eq!(
        size_of::<mlx5_wqe_datagram_seg>(),
        48,
        "mlx5_wqe_datagram_seg size mismatch"
    );
    assert_eq!(
        size_of::<mlx5_wqe_raddr_seg>(),
        16,
        "mlx5_wqe_raddr_seg size mismatch"
    );
    assert_eq!(
        size_of::<mlx5_wqe_atomic_seg>(),
        16,
        "mlx5_wqe_atomic_seg size mismatch"
    );
    assert_eq!(
        size_of::<mlx5_wqe_inl_data_seg>(),
        4,
        "mlx5_wqe_inl_data_seg size mismatch"
    );
    assert_eq!(
        size_of::<mlx5_wqe_eth_seg>(),
        32,
        "mlx5_wqe_eth_seg size mismatch"
    );
    assert_eq!(
        size_of::<mlx5_wqe_tm_seg>(),
        32,
        "mlx5_wqe_tm_seg size mismatch"
    );
    assert_eq!(
        size_of::<mlx5_wqe_umr_ctrl_seg>(),
        48,
        "mlx5_wqe_umr_ctrl_seg size mismatch"
    );
    assert_eq!(
        size_of::<mlx5_wqe_umr_klm_seg>(),
        16,
        "mlx5_wqe_umr_klm_seg size mismatch"
    );
    assert_eq!(
        size_of::<mlx5_wqe_umr_repeat_ent_seg>(),
        16,
        "mlx5_wqe_umr_repeat_ent_seg size mismatch"
    );
    assert_eq!(size_of::<mlx5_cqe64>(), 64, "mlx5_cqe64 size mismatch");
}

#[test]
fn test_mlx5_wqe_ctrl_seg_layout() {
    // Size and alignment
    assert_eq!(size_of::<mlx5_wqe_ctrl_seg>(), 16);
    assert_eq!(align_of::<mlx5_wqe_ctrl_seg>(), 4);

    // Field offsets (via inner struct)
    // C offsets: opmod_idx_opcode=0, qpn_ds=4, signature=8,
    //            dci_stream_channel_id=9, fm_ce_se=11, imm=12
    assert_eq!(offset_of!(mlx5_wqe_ctrl_seg_inner, opmod_idx_opcode), 0);
    assert_eq!(offset_of!(mlx5_wqe_ctrl_seg_inner, qpn_ds), 4);
    assert_eq!(offset_of!(mlx5_wqe_ctrl_seg_inner, signature), 8);
    assert_eq!(
        offset_of!(mlx5_wqe_ctrl_seg_inner, dci_stream_channel_id),
        9
    );
    assert_eq!(offset_of!(mlx5_wqe_ctrl_seg_inner, fm_ce_se), 11);
    assert_eq!(offset_of!(mlx5_wqe_ctrl_seg_inner, imm), 12);
}
