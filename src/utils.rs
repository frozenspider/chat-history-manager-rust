use std::collections::Bound;
use std::error::Error;
use std::ops::RangeBounds;

pub trait SmartSlice<T> {
    /**
     * Works as `x[a..b]`, but understands negative indexes as those going from the other end,
     * -1 being the last element.
     */
    fn smart_slice<R: RangeBounds<i32>>(&self, range: R) -> &[T];
}

impl<T> SmartSlice<T> for Vec<T> {
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

// Use Result<T, Box<dyn std::error::Error>> maybe?
pub type Res<T> = Result<T, String>;
pub type EmptyRes = Res<()>;

pub fn error_to_string<E: Error>(e: E) -> String {
    let mut s = e.to_string();
    if let Some(src_e) = e.source() {
        s.push_str(&format!(" (caused by: {})", error_to_string(src_e)))
    }
    s
}
