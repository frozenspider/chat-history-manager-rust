use std::cell::Cell;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::Instant;
use chrono::Local;

use simd_json;
use simd_json::{BorrowedValue, ValueAccess, ValueType};
use uuid::Uuid;

use crate::{InMemoryDb, EmptyRes, Res, MyselfChooser};

mod telegram;

type ObjFn<'lt> = dyn FnMut(&BorrowedValue) -> EmptyRes + 'lt;
type BoxObjFn<'lt> = Box<ObjFn<'lt>>;
type ActionMap<'lt> = HashMap<&'lt str, BoxObjFn<'lt>>;

// Macros for converting JSON value to X.

macro_rules! as_i64 {
    ($v:expr, $txt:expr) => {
        $v.try_as_i64().map_err(|e| format!("{} conversion: {:?}", $txt, e))?
    };
}

macro_rules! as_str_option_res {
    ($v:expr, $txt:expr) => {
        $v.try_as_str().map_err(|e| format!("{} conversion: {:?}", $txt, e)).map(|s|
            match s {
                "" => None,
                s  => Some(s),
            }
        )
    };
}

macro_rules! as_str_res {
    ($v:expr, $txt:expr) => {
        as_str_option_res!($v, $txt)?.ok_or(format!("{} is an empty string", $txt))
    };
}

macro_rules! as_string_res {
    ($v:expr, $txt:expr) => {as_str_res!($v, $txt).map(|s| s.to_owned())};
}

macro_rules! as_str {
    ($v:expr, $txt:expr) => {as_str_res!($v, $txt)?};
}

macro_rules! as_string {
    ($v:expr, $txt:expr) => {as_str!($v, $txt).to_owned()};
}

/// Empty string is None.
macro_rules! as_string_option {
    ($v:expr, $txt:expr) => {as_str_option_res!($v, $txt)?.map(|s| s.to_owned())};
}

macro_rules! as_array {
    ($v:expr, $txt:expr) => {
        $v.try_as_array().map_err(|e| format!("{} conversion: {:?}", $txt, e))?
    };
}

macro_rules! as_object {
    ($v:expr, $txt:expr) => {
        $v.try_as_object().map_err(|e| format!("{} conversion: {:?}", $txt, e))?
    };
}

// Macros for getting fields out of a JSON object and converting them to X.

macro_rules! get_field {
    ($v:expr, $txt:expr) => {
        $v.get($txt).ok_or(format!("{} field not found", $txt))
    };
}

macro_rules! get_field_str {
    ($v:expr, $txt:expr) => {as_str!(get_field!($v, $txt), $txt)};
}

macro_rules! get_field_string {
    ($v:expr, $txt:expr) => {as_string!(get_field!($v, $txt), $txt)};
}

/// Empty string is None.
macro_rules! get_field_string_option {
    ($v:expr, $txt:expr) => {as_string_option!(get_field!($v, $txt), $txt)};
}

pub(crate) use as_array;
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

fn parse_bw_as_object<'lt>(bw: &BorrowedValue,
                           name: &str,
                           actions: ActionMap<'lt>) -> EmptyRes {
    parse_object(as_object!(bw, name), name, actions)
}

fn parse_object<'lt>(obj: &simd_json::borrowed::Object,
                     name: &str,
                     mut actions: ActionMap<'lt>) -> EmptyRes {
    for (k, v) in obj.iter() {
        let action =
            actions.get_mut(k.as_ref()).ok_or(format!("Unexpected key: {}.{}", name, k))?;
        action(v)?
    }
    Ok(())
}

fn consume<'lt>() -> BoxObjFn<'lt> {
    Box::new(|_| Ok(()))
}

pub fn parse_file(path: &str, myself_chooser: MyselfChooser) -> Res<InMemoryDb> {
    let uuid = Uuid::new_v4();

    // No choice yet.
    let src_alias = "Telegram";
    let src_type = "telegram";

    let now_str = Local::now().format("%Y-%m-%d");

    let mut parsed =
        telegram::parse_file(Path::new(path), &uuid, myself_chooser);

    parsed.iter_mut().for_each(|p| {
        p.dataset.alias = format!("{src_alias} data loaded @ {now_str}");
        p.dataset.source_type = src_type.to_owned();
    });

    parsed
}
