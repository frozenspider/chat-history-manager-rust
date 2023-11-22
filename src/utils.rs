use std::collections::Bound;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::fs::File;
use std::hash::{BuildHasher, BuildHasherDefault, Hasher as StdHasher};
use std::io::{BufReader, Read};
use std::ops::RangeBounds;
use std::path::Path;
use std::time::Instant;

pub use anyhow::{anyhow, bail, Context};
use chrono::Local;
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

    const EMPTY_SLICE: Self::Sliced;

    /**
     * Works as `x[a..b]`, but understands negative indexes as those going from the other end,
     * -1 being the last element.
     * Allows indexing past either end, safely ignoring it.
     * If lower bound is past the end or (negative) upper bound is past the beginning, returns empty slice.
     */
    fn smart_slice<R: RangeBounds<i32>>(&'a self, range: R) -> Self::Sliced;
}

macro_rules! smart_slice_impl {
    () => {
        fn smart_slice<R: RangeBounds<i32>>(&'a self, range: R) -> Self::Sliced {
            let lower_inc: usize = match range.start_bound() {
                Bound::Included(&idx) if idx < 0 => {
                    let shift_from_end = -idx as usize;
                    if shift_from_end > self.len() {
                        0
                    } else {
                        self.len() - shift_from_end
                    }
                }
                Bound::Included(&idx) if idx as usize >= self.len() => return Self::EMPTY_SLICE,
                Bound::Included(&idx) => idx as usize,
                Bound::Unbounded => 0,
                Bound::Excluded(_) => unreachable!(),
            };
            let upper_inc: usize = match range.end_bound() {
                Bound::Included(&idx) if idx < 0 => {
                    let shift_from_end = -idx as usize;
                    if shift_from_end > self.len() {
                        return Self::EMPTY_SLICE;
                    }
                    self.len() - shift_from_end
                }
                Bound::Included(&idx) if idx as usize >= self.len() => self.len() - 1,
                Bound::Included(&idx) => idx as usize,
                Bound::Excluded(&idx) if idx < 0 => {
                    let shift_from_end = -idx as usize + 1;
                    if shift_from_end > self.len() {
                        return Self::EMPTY_SLICE;
                    }
                    self.len() - shift_from_end
                }
                Bound::Excluded(&idx) if idx as usize > self.len() => self.len() - 1,
                Bound::Excluded(&idx) => (idx - 1) as usize,
                Bound::Unbounded => self.len() - 1
            };
            if lower_inc > upper_inc {
                Self::EMPTY_SLICE
            } else {
                &self[lower_inc..=upper_inc]
            }
        }
    };
}

impl<'a, T: 'a> SmartSlice<'a> for [T] {
    type Sliced = &'a [T];
    const EMPTY_SLICE: Self::Sliced = &[];
    smart_slice_impl!();
}

impl<'a> SmartSlice<'a> for &str {
    type Sliced = &'a str;
    const EMPTY_SLICE: Self::Sliced = "";
    smart_slice_impl!();
}

//
// File system
//

pub fn path_file_name(path: &Path) -> Result<&str> {
    path.file_name().and_then(|p: &OsStr| p.to_str()).context("Failed to convert filename to string")
}

pub fn path_to_str(path: &Path) -> Result<&str> {
    path.to_str().context("Failed to convert path to a string")
}

//
// Error handling
//

pub type Result<T> = anyhow::Result<T>;
pub type EmptyRes = Result<()>;

#[macro_export]
macro_rules! err {
    ($($arg:tt)*) => {{
        Err(anyhow!("{}", format!($($arg)*)))
    }}
}

/// Evaluates boolean expression, and bails out if it doesn't hold.
/// First argument is the expression, then comes formatting string and its arguments.
#[macro_export]
macro_rules! require {
    ($expr:expr) => {{
        if !$expr { bail!("{} was false!", stringify!($expr)); }
    }};
    ($expr:expr, $($bail_arg:tt)*) => {{
        if !$expr { bail!($($bail_arg)*); }
    }};
}

pub fn error_to_string(e: &anyhow::Error) -> String {
    format!("{:#}", e)
}

pub trait ToResult<T> {
    fn normalize_error(self) -> Result<T>;
}

impl<T> ToResult<T> for std::result::Result<T, Box<dyn std::error::Error + Send + Sync>> {
    /// Unfortunately, anyhow::Error::from_boxed is private so we're losing information.
    fn normalize_error(self) -> Result<T> {
        self.map_err(|e| anyhow!("{}", e.as_ref()))
    }
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

// Non-cryptographic non-DDOS-safe fast hasher.
pub type Hasher = BuildHasherDefault<FxHasher>;

pub fn hasher() -> Hasher {
    BuildHasherDefault::<FxHasher>::default()
}

pub fn file_hash(path: &Path) -> Result<String> {
    // We use two hashers to produce a longer hash, thus reducing collision chance.
    let mut hashers = [hasher().build_hasher(), hasher().build_hasher()];

    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut buffer = [0; 512];

    for i in [0, 1].iter().cycle() /* Can't cycle over a mutable iterator */ {
        let count = reader.read(&mut buffer)?;
        if count == 0 { break; }
        hashers[*i].write(&buffer[..count]);
    }

    Ok(format!("{:X}{:X}", hashers[0].finish(), hashers[1].finish()))
}

//
// Misc
//

pub fn transpose_option_result<T>(x: Option<Result<T>>) -> Result<Option<T>> {
    x.map_or(Ok(None), |v| v.map(Some))
}
