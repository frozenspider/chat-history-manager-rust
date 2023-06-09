use std::cell::Cell;
use std::collections::HashMap;
use std::fs;
use std::time::Instant;
use chrono::Local;

use simd_json;
use simd_json::{BorrowedValue, ValueAccess, ValueType};
use uuid::Uuid;

use crate::{InMemoryDb, EmptyRes, Res};

mod telegram;

type ObjFn<'lt> = dyn FnMut(&BorrowedValue) -> EmptyRes + 'lt;
type BoxObjFn<'lt> = Box<ObjFn<'lt>>;
type ActionMap<'lt> = HashMap<&'lt str, BoxObjFn<'lt>>;

macro_rules! as_i64 {
    ($v:expr, $txt:expr) => {
        $v.try_as_i64().map_err(|e| format!("{} conversion: {:?}", $txt, e))?
    };
}

macro_rules! as_str_res {
    ($v:expr, $txt:expr) => {
        $v.try_as_str().map_err(|e| format!("{} conversion: {:?}", $txt, e))
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

macro_rules! get_field_str {
    ($v:expr, $txt:expr) => {
        as_str!($v.get($txt).ok_or(format!("{} field not found", $txt)), $txt)
    };
}

macro_rules! get_field_string {
    ($v:expr, $txt:expr) => {get_field_str!($v, $txt).to_owned()};
}

pub(crate) use as_array;
pub(crate) use as_i64;
pub(crate) use as_object;
pub(crate) use as_str;
pub(crate) use as_str_res;
pub(crate) use as_string;
pub(crate) use as_string_res;
pub(crate) use get_field_str;
pub(crate) use get_field_string;

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

pub fn parse_file(path: &str) -> Res<InMemoryDb> {
    let uuid = Uuid::new_v4();

    // No choice yet.
    let src_alias = "Telegram";
    let src_type = "telegram";

    let now_str = Local::now().format("%Y-%m-%d");

    let results_json_path =
        if !path.ends_with("result.json") {
            format!("{path}/result.json")
        } else {
            path.to_owned()
        };
    println!("Parsing '{results_json_path}'");
    let mut parsed =
        telegram::parse_file(results_json_path.as_str(), &uuid);
    parsed.iter_mut().for_each(|p| {
        p.dataset.alias = format!("{src_alias} data loaded @ {now_str}");
        p.dataset.source_type = src_type.to_owned();
    });
    parsed
}
