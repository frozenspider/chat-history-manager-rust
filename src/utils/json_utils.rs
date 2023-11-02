// Reexporting simd_json for simplicity.
pub use simd_json::{BorrowedValue, StaticNode, Value as JValue, ValueAccess, ValueType};
pub use simd_json::borrowed::{Object, Value};

use crate::*;

pub struct ParseCallback<'a> {
    /// Key being processed
    pub key: &'a str,
    /// Value corresponding to tha key
    pub value: &'a BorrowedValue<'a>,
    /// Action to take when key is not expected
    pub wrong_key_action: &'a dyn Fn() -> EmptyRes,
}

//
// Macros for converting JSON value to X.
//

#[macro_export]
macro_rules! as_i32 {
    ($v:expr, $path:expr) => {
        $v.try_as_i32().with_context(|| format!("'{}' field conversion error", $path))?
    };
    ($v:expr, $path:expr, $path2:expr) => {as_i32!($v, format!("{}.{}", $path, $path2))};
}

#[macro_export]
macro_rules! as_i64 {
    ($v:expr, $path:expr) => {
        $v.try_as_i64().with_context(|| format!("'{}' field conversion error", $path))?
    };
    ($v:expr, $path:expr, $path2:expr) => {as_i64!($v, format!("{}.{}", $path, $path2))};
}

#[macro_export]
macro_rules! as_str_option_res {
    ($v:expr, $path:expr) => {
        $v.try_as_str().with_context(|| format!("'{}' field conversion error", $path)).map(|s|
            match s {
                "" => None,
                s  => Some(s),
            }
        )
    };
    ($v:expr, $path:expr, $path2:expr) => {as_str_option_res!($v, format!("{}.{}", $path, $path2))};
}

#[macro_export]
macro_rules! as_str_res {
    ($v:expr, $path:expr) => {
        as_str_option_res!($v, $path)?.ok_or_else(|| anyhow!("'{}' is an empty string", $path))
    };
    ($v:expr, $path:expr, $path2:expr) => {as_str_res!($v, format!("{}.{}", $path, $path2))};
}

#[macro_export]
macro_rules! as_string_res {
    ($v:expr, $path:expr) => {as_str_res!($v, $path).map(|s| s.to_owned())};
    ($v:expr, $path:expr, $path2:expr) => {as_string_res!($v, format!("{}.{}", $path, $path2))};
}

#[macro_export]
macro_rules! as_str {
    ($v:expr, $path:expr) => {as_str_res!($v, $path)?};
    ($v:expr, $path:expr, $path2:expr) => {as_str!($v, format!("{}.{}", $path, $path2))};
}

#[macro_export]
macro_rules! as_string {
    ($v:expr, $path:expr) => {as_str!($v, $path).to_owned()};
    ($v:expr, $path:expr, $path2:expr) => {as_string!($v, format!("{}.{}", $path, $path2))};
}

#[macro_export]
/// Empty string is None.
macro_rules! as_string_option {
    ($v:expr, $path:expr) => {as_str_option_res!($v, $path)?.map(|s| s.to_owned())};
    ($v:expr, $path:expr, $path2:expr) => {as_string_option!($v, format!("{}.{}", $path, $path2))};
}

#[macro_export]
macro_rules! as_array {
    ($v:expr, $path:expr) => {
        $v.try_as_array().with_context(|| format!("'{}' field conversion error", $path))?
    };
    ($v:expr, $path:expr, $path2:expr) => {as_array!($v, format!("{}.{}", $path, $path2))};
}

#[macro_export]
macro_rules! as_object {
    ($v:expr, $path:expr) => {
        $v.try_as_object().with_context(|| format!("'{}' field conversion error", $path))?
    };
    ($v:expr, $path:expr, $path2:expr) => {as_object!($v, format!("{}.{}", $path, $path2))};
}

//
// Macros for getting fields out of a JSON object and converting them to X.
//

#[macro_export]
macro_rules! get_field {
    ($v:expr, $path:expr, $txt:expr) => {
        $v.get($txt).ok_or(anyhow!("{}.{} field not found", $path, $txt))
    };
}
#[macro_export]
macro_rules! get_field_str {
    ($v:expr, $path:expr, $txt:expr) => {as_str!(get_field!($v, $path, $txt), format!("{}.{}", $path, $txt))};
}

#[macro_export]
macro_rules! get_field_string {
    ($v:expr, $path:expr, $txt:expr) => {as_string!(get_field!($v, $path, $txt), format!("{}.{}", $path, $txt))};
}

/// Empty string is None.
#[macro_export]
macro_rules! get_field_string_option {
    ($v:expr, $path:expr, $txt:expr) => {as_string_option!(get_field!($v, $path, $txt), format!("{}.{}", $path, $txt))};
}

//
// Parse functions
//

pub fn parse_bw_as_object(bw: &BorrowedValue,
                          path: &str,
                          process: impl FnMut(ParseCallback) -> EmptyRes) -> EmptyRes {
    parse_object(as_object!(bw, path), path, process)
}

pub fn parse_object(obj: &simd_json::borrowed::Object,
                    path: &str,
                    mut process: impl FnMut(ParseCallback) -> EmptyRes) -> EmptyRes {
    for (k, v) in obj.iter() {
        process(ParseCallback {
            key: k,
            value: v,
            wrong_key_action: &|| err!("Unexpected key: {}.{}", path, k),
        })?
    }
    Ok(())
}

pub fn consume() -> EmptyRes { Ok(()) }
