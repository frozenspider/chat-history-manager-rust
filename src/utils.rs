use std::collections::Bound;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::hash::BuildHasherDefault;
use std::ops::RangeBounds;
use std::path::Path;
use std::time::Instant;

use chrono::Local;
pub use error_chain::{bail, error_chain};
use hashers::fx_hash::FxHasher;
use lazy_static::lazy_static;

pub mod entity_utils;

#[cfg(test)]
pub mod test_utils;

pub mod json_utils;

//
// Constants
//

lazy_static! {
    pub static ref LOCAL_TZ: Local = Local::now().timezone();
}

//
// Smart slice
//

pub trait SmartSlice<'a> {
    type Sliced: 'a;

    /**
     * Works as `x[a..b]`, but understands negative indexes as those going from the other end,
     * -1 being the last element.
     */
    fn smart_slice<R: RangeBounds<i32>>(&'a self, range: R) -> Self::Sliced;
}

macro_rules! smart_slice_impl {
    () => {
        fn smart_slice<R: RangeBounds<i32>>(&'a self, range: R) -> Self::Sliced {
            let lower_inc: usize = match range.start_bound() {
                Bound::Included(&idx) if idx < 0 => self.len() - (-idx as usize),
                Bound::Included(&idx) => idx as usize,
                Bound::Excluded(&idx) if idx < 0 => self.len() - (-idx as usize) + 1,
                Bound::Excluded(&idx) => (idx + 1) as usize,
                Bound::Unbounded => 0
            };
            let upper_inc: usize = match range.end_bound() {
                Bound::Included(&idx) if idx < 0 => self.len() - (-idx as usize),
                Bound::Included(&idx) => idx as usize,
                Bound::Excluded(&idx) if idx < 0 => self.len() - (-idx as usize) - 1,
                Bound::Excluded(&idx) => (idx - 1) as usize,
                Bound::Unbounded => self.len() - 1
            };
            &self[lower_inc..=upper_inc]
        }
    };
}

impl<'a, T: 'a> SmartSlice<'a> for [T] {
    type Sliced = &'a [T];
    smart_slice_impl!();
}

impl<'a> SmartSlice<'a> for &str {
    type Sliced = &'a str;
    smart_slice_impl!();
}

//
// File system
//

pub fn path_file_name(path: &Path) -> Result<&str> {
    path.file_name().and_then(|p: &OsStr| p.to_str()).ok_or_else(|| "Failed to convert filename to string".into())
}

pub fn path_to_str(path: &Path) -> Result<&str> {
    path.to_str().ok_or_else(|| "Failed to convert path to a string".into())
}

//
// Error handling
//

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    foreign_links {
        Io(std::io::Error);
        ParseInt(::std::num::ParseIntError);
        ParseFloat(::std::num::ParseFloatError);
        Json(simd_json::Error);
        JsonTryType(simd_json::TryTypeError);
        NetworkTransport(tonic::transport::Error);
        TaskJoin(tokio::task::JoinError);
        DateTimeParse(chrono::format::ParseError);
        SqliteError(rusqlite::Error);
        VcardParseError(ical::parser::ParserError);
    }
}

pub type EmptyRes = Result<()>;

#[macro_export]
macro_rules! err {
    ($($arg:tt)*) => {{
        Err(Error::from(format!($($arg)*)))
    }}
}

/// Evaluates boolean expression, and bails out if it doesn't hold.
/// First argument is the expression, then comes formatting string and its arguments.
#[macro_export]
macro_rules! require {
    ($expr:expr, $($bail_arg:tt)*) => {{
        if !$expr { bail!($($bail_arg)*); }
    }}
}

pub fn error_to_string(e: &Error) -> String {
    let mut s = String::new();
    s.push_str(&format!("{:?}", e.kind()));
    s.push_str(": ");

    for (level, err) in e.iter().enumerate() {
        if level > 0 {
            s.push_str("  └> ");
        }
        s.push_str(&err.to_string());
        s.push('\n');
    }
    s.trim_end().to_owned()
}

//
// Time measurement
//

pub fn measure<T, R>(block: T, after_call: impl Fn(&R, u128)) -> R
    where T: FnOnce() -> R
{
    let start_time = Instant::now();
    let result = block();
    let elapsed = start_time.elapsed().as_millis();
    after_call(&result, elapsed);
    result
}

//
// Hashing
//

pub type Hasher = BuildHasherDefault<FxHasher>;

pub fn hasher() -> Hasher {
    BuildHasherDefault::<FxHasher>::default()
}
