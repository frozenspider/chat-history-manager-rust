use std::cell::Cell;
use std::collections::HashMap;
use std::fs;
use std::hash::BuildHasherDefault;
use std::path::Path;
use std::time::Instant;
use hashers::fx_hash::FxHasher;

use simd_json::{BorrowedValue, ValueAccess, ValueType};
use uuid::Uuid;

use crate::*;
use crate::entities::*;
use crate::dao::in_memory_dao::InMemoryDao;

mod telegram;

type Hasher = BuildHasherDefault<FxHasher>;

struct ParseCallback<'a> {
    /// Key being processed
    key: &'a str,
    /// Value corresponding to tha key
    value: &'a BorrowedValue<'a>,
    /// Action to take when key is not expected
    wrong_key_action: &'a dyn Fn() -> EmptyRes,
}

pub fn parse_file(path: &str, choose_myself: &dyn ChooseMyselfTrait) -> Result<Box<InMemoryDao>> {
    let uuid = Uuid::new_v4();
    telegram::parse_file(Path::new(path), &uuid, choose_myself)
}

//
// Macros for converting JSON value to X.
//

#[macro_export]
macro_rules! as_i32 {
    ($v:expr, $path:expr) => {
        $v.try_as_i32().chain_err(|| format!("'{}' field conversion error", $path))?
    };
    ($v:expr, $path:expr, $path2:expr) => {as_i32!($v, format!("{}.{}", $path, $path2))};
}

#[macro_export]
macro_rules! as_i64 {
    ($v:expr, $path:expr) => {
        $v.try_as_i64().chain_err(|| format!("'{}' field conversion error", $path))?
    };
    ($v:expr, $path:expr, $path2:expr) => {as_i64!($v, format!("{}.{}", $path, $path2))};
}

#[macro_export]
macro_rules! as_str_option_res {
    ($v:expr, $path:expr) => {
        $v.try_as_str().chain_err(|| format!("'{}' field conversion: error", $path)).map(|s|
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
        as_str_option_res!($v, $path)?.ok_or_else(|| Error::from(format!("'{}' is an empty string", $path)))
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
        $v.try_as_array().chain_err(|| format!("'{}' field conversion error", $path))?
    };
    ($v:expr, $path:expr, $path2:expr) => {as_array!($v, format!("{}.{}", $path, $path2))};
}

#[macro_export]
macro_rules! as_object {
    ($v:expr, $path:expr) => {
        $v.try_as_object().chain_err(|| format!("'{}' field conversion error", $path))?
    };
    ($v:expr, $path:expr, $path2:expr) => {as_object!($v, format!("{}.{}", $path, $path2))};
}

//
// Macros for getting fields out of a JSON object and converting them to X.
//

#[macro_export]
macro_rules! get_field {
    ($v:expr, $path:expr, $txt:expr) => {
        $v.get($txt).ok_or(format!("{}.{} field not found", $path, $txt))
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
// Utility functions
//

fn name_or_unnamed(name_option: &Option<String>) -> String {
    name_option.as_ref().cloned().unwrap_or(UNNAMED.to_owned())
}

fn hasher() -> Hasher {
    BuildHasherDefault::<FxHasher>::default()
}

fn parse_bw_as_object(bw: &BorrowedValue,
                      path: &str,
                      process: impl FnMut(ParseCallback) -> EmptyRes) -> EmptyRes {
    parse_object(as_object!(bw, path), path, process)
}

fn parse_object(obj: &simd_json::borrowed::Object,
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

fn consume() -> EmptyRes { Ok(()) }
