use std::cell::Cell;
use std::collections::HashMap;
use std::fs;
use std::hash::BuildHasherDefault;
use std::path::Path;
use std::time::Instant;
use hashers::fx_hash::FxHasher;

use simd_json;
use simd_json::{BorrowedValue, ValueAccess, ValueType};
use uuid::Uuid;

use crate::{InMemoryDb, EmptyRes, Res, ChooseMyselfTrait};

mod telegram;

type Hasher = BuildHasherDefault<FxHasher>;

type ObjFn<'lt> = dyn FnMut(&BorrowedValue) -> EmptyRes + 'lt;
type BoxObjFn<'lt> = Box<ObjFn<'lt>>;
type ActionMap<'lt> = HashMap<&'lt str, BoxObjFn<'lt>, Hasher>;

// Macros for converting JSON value to X.

macro_rules! as_i32 {
    ($v:expr, $path:expr) => {
        $v.try_as_i32().map_err(|e| format!("'{}' field conversion: {:?}", $path, e))?
    };
    ($v:expr, $path:expr, $path2:expr) => {as_i32!($v, format!("{}.{}", $path, $path2))};
}

macro_rules! as_i64 {
    ($v:expr, $path:expr) => {
        $v.try_as_i64().map_err(|e| format!("'{}' field conversion: {:?}", $path, e))?
    };
    ($v:expr, $path:expr, $path2:expr) => {as_i64!($v, format!("{}.{}", $path, $path2))};
}

macro_rules! as_str_option_res {
    ($v:expr, $path:expr) => {
        $v.try_as_str().map_err(|e| format!("'{}' field conversion: {:?}", $path, e)).map(|s|
            match s {
                "" => None,
                s  => Some(s),
            }
        )
    };
    ($v:expr, $path:expr, $path2:expr) => {as_str_option_res!($v, format!("{}.{}", $path, $path2))};
}

macro_rules! as_str_res {
    ($v:expr, $path:expr) => {
        as_str_option_res!($v, $path)?.ok_or(format!("'{}' is an empty string", $path))
    };
    ($v:expr, $path:expr, $path2:expr) => {as_str_res!($v, format!("{}.{}", $path, $path2))};
}

macro_rules! as_string_res {
    ($v:expr, $path:expr) => {as_str_res!($v, $path).map(|s| s.to_owned())};
    ($v:expr, $path:expr, $path2:expr) => {as_string_res!($v, format!("{}.{}", $path, $path2))};
}

macro_rules! as_str {
    ($v:expr, $path:expr) => {as_str_res!($v, $path)?};
    ($v:expr, $path:expr, $path2:expr) => {as_str!($v, format!("{}.{}", $path, $path2))};
}

macro_rules! as_string {
    ($v:expr, $path:expr) => {as_str!($v, $path).to_owned()};
    ($v:expr, $path:expr, $path2:expr) => {as_string!($v, format!("{}.{}", $path, $path2))};
}

/// Empty string is None.
macro_rules! as_string_option {
    ($v:expr, $path:expr) => {as_str_option_res!($v, $path)?.map(|s| s.to_owned())};
    ($v:expr, $path:expr, $path2:expr) => {as_string_option!($v, format!("{}.{}", $path, $path2))};
}

macro_rules! as_array {
    ($v:expr, $path:expr) => {
        $v.try_as_array().map_err(|e| format!("'{}' field conversion: {:?}", $path, e))?
    };
    ($v:expr, $path:expr, $path2:expr) => {as_array!($v, format!("{}.{}", $path, $path2))};
}

macro_rules! as_object {
    ($v:expr, $path:expr) => {
        $v.try_as_object().map_err(|e| format!("'{}' field conversion: {:?}", $path, e))?
    };
    ($v:expr, $path:expr, $path2:expr) => {as_object!($v, format!("{}.{}", $path, $path2))};
}

// Macros for getting fields out of a JSON object and converting them to X.

macro_rules! get_field {
    ($v:expr, $path:expr, $txt:expr) => {
        $v.get($txt).ok_or(format!("{}.{} field not found", $path, $txt))
    };
}

macro_rules! get_field_str {
    ($v:expr, $path:expr, $txt:expr) => {as_str!(get_field!($v, $path, $txt), format!("{}.{}", $path, $txt))};
}

macro_rules! get_field_string {
    ($v:expr, $path:expr, $txt:expr) => {as_string!(get_field!($v, $path, $txt), format!("{}.{}", $path, $txt))};
}

/// Empty string is None.
macro_rules! get_field_string_option {
    ($v:expr, $path:expr, $txt:expr) => {as_string_option!(get_field!($v, $path, $txt), format!("{}.{}", $path, $txt))};
}

pub(crate) use as_array;
pub(crate) use as_i32;
pub(crate) use as_i64;
pub(crate) use as_str_res;
pub(crate) use as_str_option_res;
pub(crate) use as_object;
pub(crate) use as_str;
pub(crate) use as_string;
pub(crate) use as_string_option;
pub(crate) use as_string_res;
pub(crate) use get_field;
pub(crate) use get_field_str;
pub(crate) use get_field_string;
pub(crate) use get_field_string_option;

use crate::entities::*;

fn name_or_unnamed(name_option: &Option<String>) -> String {
    name_option.as_ref().map(|s| s.clone()).unwrap_or(UNNAMED.to_owned())
}

fn hasher() -> Hasher {
    BuildHasherDefault::<FxHasher>::default()
}

fn action_map<'lt, const N: usize>(actions: [(&'lt str, BoxObjFn<'lt>); N]) -> ActionMap<'lt> {
    ActionMap::from_iter(actions)
}

fn parse_bw_as_object<'lt>(bw: &BorrowedValue,
                           path: &str,
                           actions: ActionMap<'lt>) -> EmptyRes {
    parse_object(as_object!(bw, path), path, actions)
}

fn parse_object<'lt>(obj: &simd_json::borrowed::Object,
                     path: &str,
                     mut actions: ActionMap<'lt>) -> EmptyRes {
    for (k, v) in obj.iter() {
        let action =
            actions.get_mut(k.as_ref()).ok_or(format!("Unexpected key: {}.{}", path, k))?;
        action(v)?
    }
    Ok(())
}

fn consume<'lt>() -> BoxObjFn<'lt> {
    Box::new(|_| Ok(()))
}

pub fn parse_file(path: &str, choose_myself: &dyn ChooseMyselfTrait) -> Res<InMemoryDb> {
    let uuid = Uuid::new_v4();
    telegram::parse_file(Path::new(path), &uuid, choose_myself)
}
