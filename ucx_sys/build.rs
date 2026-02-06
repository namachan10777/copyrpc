use std::env;
use std::path::PathBuf;

fn main() {
    // Try to find UCX headers
    let ucx_include = find_ucx_include();

    println!("cargo:rustc-link-lib=ucp");
    println!("cargo:rustc-link-lib=ucs");
    println!("cargo:rustc-link-lib=uct");

    let mut builder = bindgen::Builder::default()
        .header_contents(
            "wrapper.h",
            "#include <ucp/api/ucp.h>\n",
        )
        .allowlist_function("ucp_.*")
        .allowlist_type("ucp_.*")
        .allowlist_var("UCP_.*")
        .allowlist_function("ucs_.*")
        .allowlist_type("ucs_.*")
        .allowlist_var("UCS_.*");

    if let Some(include_path) = ucx_include {
        builder = builder.clang_arg(format!("-I{}", include_path.display()));
        println!(
            "cargo:rustc-link-search=native={}",
            include_path.parent().unwrap().join("lib").display()
        );
    }

    let bindings = builder
        .generate()
        .expect("Unable to generate UCX bindings. Is libucx-dev installed?");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

fn find_ucx_include() -> Option<PathBuf> {
    // Check common paths
    let candidates = [
        "/usr/include",
        "/usr/local/include",
        "/opt/ucx/include",
    ];

    for path in &candidates {
        let header = PathBuf::from(path).join("ucp/api/ucp.h");
        if header.exists() {
            return Some(PathBuf::from(path));
        }
    }

    // Try pkg-config
    if let Ok(output) = std::process::Command::new("pkg-config")
        .args(["--cflags", "ucx"])
        .output()
    {
        if output.status.success() {
            let flags = String::from_utf8_lossy(&output.stdout);
            for flag in flags.split_whitespace() {
                if let Some(path) = flag.strip_prefix("-I") {
                    return Some(PathBuf::from(path));
                }
            }
        }
    }

    None
}
