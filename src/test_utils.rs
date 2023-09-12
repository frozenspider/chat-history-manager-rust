use chrono::*;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref BASE_DATE: DateTime<FixedOffset> = dt("2019-01-02 11:15:21", None);
}

pub fn dt(s: &str, offset: Option<&FixedOffset>) -> DateTime<FixedOffset> {
    let local = Local::now();
    let offset = offset.unwrap_or(local.offset());
    offset.from_local_datetime(&NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap()).unwrap()
}
