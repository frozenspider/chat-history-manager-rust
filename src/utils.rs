use std::collections::Bound;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::time::Instant;

pub use error_chain::{bail, error_chain};

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    foreign_links {
        Io(std::io::Error);
        ParseInt(::std::num::ParseIntError);
        Json(simd_json::Error);
        JsonTryType(simd_json::TryTypeError);
        NetworkTransport(tonic::transport::Error);
        TaskJoin(tokio::task::JoinError);
    }
}

pub trait SmartSlice {
    type Item;

    /**
     * Works as `x[a..b]`, but understands negative indexes as those going from the other end,
     * -1 being the last element.
     */
    fn smart_slice<R: RangeBounds<i32>>(&self, range: R) -> &[Self::Item];
}

impl<T> SmartSlice for Vec<T> {
    type Item = T;

    fn smart_slice<R: RangeBounds<i32>>(&self, range: R) -> &[T] {
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
}

pub type EmptyRes = Result<()>;

#[macro_export]
macro_rules! err {
    ($($arg:tt)*) => {{
        Err(Error::from(format!($($arg)*)))
    }}
}

pub fn error_to_string(e: &Error) -> String {
    let mut s = String::new();
    for (level, err) in e.iter().enumerate() {
        if level > 0 {
            s.push_str("  └> ");
        }
        s.push_str(&err.to_string());
        s.push_str("\n");
    }
    s
}

pub fn measure<T, R>(block: T, after_call: impl Fn(&R, u128)) -> R
    where T: FnOnce() -> R
{
    let start_time = Instant::now();
    let result = block();
    let elapsed = start_time.elapsed().as_millis();
    after_call(&result, elapsed);
    result
}
