use std::env;

fn main() {
    // Build the C++ static library via CMake
    let dst = cmake::Config::new("../kubera-executor-cpp")
        .define("CMAKE_BUILD_TYPE", "Release")
        .build();

    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    println!("cargo:rustc-link-lib=static=kubera_executor_cpp");

    // Ensure C++ standard library is linked where needed (mainly for Linux).
    // Rust/Cargo usually handles this, but we add it explicitly for safety.
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
        println!("cargo:rustc-link-lib=dylib=stdc++");
    }
}
