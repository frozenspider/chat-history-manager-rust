use rusqlite::ToSql;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, Value, ValueRef};

use rusqlite_from_row::FromRow;

use crate::protobuf::history::ChatType;

#[derive(FromRow)]
pub struct RawDataset {
    pub uuid: Vec<u8>,
    pub alias: String,
    pub source_type: String,
}

#[derive(FromRow)]
pub struct RawUser {
    pub ds_uuid: Vec<u8>,
    pub id: i64,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub username: Option<String>,
    pub phone_numbers: Option<String>,
    pub is_myself: bool,
}

#[derive(FromRow)]
pub struct RawChat {
    pub ds_uuid: Vec<u8>,
    pub id: i64,
    pub name: Option<String>,
    pub tpe: ChatType,
    pub img_path: Option<String>,
    /// Concatenated, comma-separated
    pub member_ids: String,
    pub msg_count: i64,
}

pub struct RawMessage {}

pub struct RawMessageContent {}

pub struct RawRichTextElement {}

//
// Converters
//

impl FromSql for ChatType {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value.as_str()? {
            "personal" => Ok(ChatType::Personal),
            "private_group" => Ok(ChatType::PrivateGroup),
            _ => Err(FromSqlError::InvalidType)
        }
    }
}

impl ToSql for ChatType {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Text(match self {
            ChatType::Personal => "personal",
            ChatType::PrivateGroup => "private_group",
        }.to_owned())))
    }
}
