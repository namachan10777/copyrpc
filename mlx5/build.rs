fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo::rustc-check-cfg=cfg(has_extended_port_attr)");
    println!("cargo::rustc-check-cfg=cfg(has_extended_device_attr)");

    // OFED がインストールされていることを確認
    let output = std::process::Command::new("ofed_info")
        .arg("-s")
        .output()
        .expect("ofed_info not found - MLNX_OFED must be installed");
    let version = String::from_utf8_lossy(&output.stdout);
    println!("cargo:warning=Detected OFED: {}", version.trim());

    // ヘッダを直接チェックして拡張フィールドの有無を検出
    let verbs_h = std::fs::read_to_string("/usr/include/infiniband/verbs.h")
        .expect("Cannot read verbs.h");
    let mlx5dv_h = std::fs::read_to_string("/usr/include/infiniband/mlx5dv.h")
        .expect("Cannot read mlx5dv.h");

    if verbs_h.contains("active_speed_ex") {
        println!("cargo:rustc-cfg=has_extended_port_attr");
    }
    if mlx5dv_h.contains("max_dc_rd_atom") {
        println!("cargo:rustc-cfg=has_extended_device_attr");
    }
}
