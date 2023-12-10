use std::collections::Bound;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::fs::File;
use std::hash::{BuildHasher, BuildHasherDefault, Hasher as StdHasher};
use std::io::{BufReader, Read};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::time::Instant;

pub use anyhow::{anyhow, bail, Context};
pub use std::error::Error as StdError;
use chrono::Local;
use hashers::fx_hash::FxHasher;
use lazy_static::lazy_static;
use unicode_segmentation::UnicodeSegmentation;

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

/// 64 KiB
pub const FILE_BUF_CAPACITY: usize = 64 * 1024;

pub fn path_file_name(path: &Path) -> Result<&str> {
    path.file_name().and_then(|p: &OsStr| p.to_str()).context("Failed to convert filename to string")
}

pub fn path_to_str(path: &Path) -> Result<&str> {
    path.to_str().context("Failed to convert path to a string")
}

/// List all files (not directories!) in the given path
pub fn list_all_files(p: &Path, recurse: bool) -> Result<Vec<PathBuf>> {
    let mut res = vec![];
    for entry in p.read_dir()? {
        let path = entry?.path();
        if path.is_file() {
            res.push(path);
        } else if recurse {
            res.extend(list_all_files(&path, recurse)?.into_iter());
        }
    }
    Ok(res)
}

/// Files are equal if they are equal byte-by-byte, or if they both don't exist
pub fn files_are_equal(f1: &Path, f2: &Path) -> Result<bool> {
    if !f1.exists() { return Ok(!f2.exists()); }
    if !f2.exists() { return Ok(!f1.exists()); }

    let f1 = File::open(f1)?;
    let f2 = File::open(f2)?;

    // Check if file sizes are different
    if f1.metadata().unwrap().len() != f2.metadata().unwrap().len() {
        return Ok(false);
    }

    // Use buf readers since they are much faster
    let f1 = BufReader::with_capacity(FILE_BUF_CAPACITY, f1);
    let f2 = BufReader::with_capacity(FILE_BUF_CAPACITY, f2);

    // Do a byte to byte comparison of the two files
    for (b1, b2) in f1.bytes().zip(f2.bytes()) {
        if b1.unwrap() != b2.unwrap() {
            return Ok(false);
        }
    }

    Ok(true)
}

//
// Error handling
//

pub type StdResult<T, E> = std::result::Result<T, E>;
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

impl<T> ToResult<T> for StdResult<T, Box<dyn StdError + Send + Sync>> {
    /// Unfortunately, anyhow::Error::from_boxed is private so we're losing information.
    fn normalize_error(self) -> Result<T> {
        self.map_err(|e| anyhow!("{}", e.as_ref()))
    }
}

//
// Time measurement
//

pub fn measure<T, AC, R>(block: T, after_call: AC) -> R
    where T: FnOnce() -> R,
          AC: FnOnce(&R, u128)
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
    let mut reader = BufReader::with_capacity(FILE_BUF_CAPACITY, file);
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

pub fn truncate_to(str: String, max_len: usize) -> String {
    str.graphemes(true).take(max_len).collect::<String>()
}

pub fn transpose_option_result<T>(x: Option<Result<T>>) -> Result<Option<T>> {
    x.map_or(Ok(None), |v| v.map(Some))
}

pub fn transpose_option_std_result<T, E: StdError + Send + Sync + 'static>(x: Option<StdResult<T, E>>) -> Result<Option<T>> {
    x.map_or(Ok(None), |v| Ok(v.map(Some)?))
}
