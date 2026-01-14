use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rustc-link-lib=mlx5");
    println!("cargo:rustc-link-lib=ibverbs");

    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .allowlist_function("mlx5dv_.*")
        .allowlist_type("mlx5dv_.*")
        .allowlist_type("mlx5_.*")
        .allowlist_var("MLX5.*")
        // mlx5_wqe_ctrl_seg has both __packed__ and __aligned__(4) which Rust doesn't support
        .blocklist_type("mlx5_wqe_ctrl_seg")
        .layout_tests(false)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
