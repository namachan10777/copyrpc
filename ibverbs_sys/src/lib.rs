#![allow(
    non_upper_case_globals,
    non_camel_case_types,
    non_snake_case,
    clippy::missing_safety_doc,
    unnecessary_transmutes
)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[cfg(feature = "ofed")]
pub mod ofed;
#[cfg(feature = "ofed")]
pub use ofed as ported;
