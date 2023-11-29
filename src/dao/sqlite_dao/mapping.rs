use diesel::prelude::*;
use diesel::sql_types::*;

pub mod schema {
    diesel::table! {
        dataset (uuid) {
            uuid -> Binary,
            alias -> Text,
        }
    }

    diesel::table! {
        user (ds_uuid, id) {
            ds_uuid -> Binary,
            id -> BigInt,
            first_name -> Nullable<Text>,
            last_name -> Nullable<Text>,
            username -> Nullable<Text>,
            phone_numbers -> Nullable<Text>,
            is_myself -> Integer,
        }
    }

    diesel::table! {
        chat (ds_uuid, id) {
            ds_uuid -> Binary,
            id -> BigInt,
            name -> Nullable<Text>,
            source_type -> Text,
            #[sql_name = "type"]
            tpe -> Text,
            img_path -> Nullable<Text>,
            msg_count -> Integer,
            main_chat_id -> Nullable<BigInt>,
        }
    }

    diesel::table! {
        chat_member (ds_uuid, chat_id, user_id) {
            ds_uuid -> Binary,
            chat_id -> BigInt,
            user_id -> BigInt,
            order -> Integer,
        }
    }

    diesel::table! {
        message (internal_id) {
            internal_id -> BigInt,
            ds_uuid -> Binary,
            chat_id -> BigInt,
            source_id -> Nullable<BigInt>,
            #[sql_name = "type"]
            tpe -> Text,
            subtype -> Nullable<Text>,
            time_sent -> BigInt,
            time_edited -> Nullable<BigInt>,
            is_deleted -> Integer,
            from_id -> BigInt,
            forward_from_name -> Nullable<Text>,
            reply_to_message_id -> Nullable<BigInt>,
        }
    }

    diesel::table! {
        message_content (id) {
            id -> BigInt,
            message_internal_id -> BigInt,
            element_type -> Text,
            path -> Nullable<Text>,
            thumbnail_path -> Nullable<Text>,
            emoji -> Nullable<Text>,
            width -> Nullable<Integer>,
            height -> Nullable<Integer>,
            mime_type -> Nullable<Text>,
            title -> Nullable<Text>,
            performer -> Nullable<Text>,
            duration_sec -> Nullable<Integer>,
            is_one_time -> Nullable<Integer>,
            lat -> Nullable<Text>,
            lon -> Nullable<Text>,
            address -> Nullable<Text>,
            poll_question -> Nullable<Text>,
            first_name -> Nullable<Text>,
            last_name -> Nullable<Text>,
            phone_number -> Nullable<Text>,
            members -> Nullable<Text>,
            discard_reason -> Nullable<Text>,
            pinned_message_id -> Nullable<BigInt>,
            is_blocked -> Nullable<Integer>,
        }
    }

    diesel::table! {
        message_text_element (id) {
            id -> BigInt,
            message_internal_id -> Nullable<BigInt>,
            element_type -> Text,
            text -> Nullable<Text>,
            href -> Nullable<Text>,
            hidden -> Nullable<Integer>,
            language -> Nullable<Text>,
        }
    }

    diesel::table! {
        refinery_schema_history (version) {
            version -> Nullable<Integer>,
            name -> Nullable<Text>,
            applied_on -> Nullable<Text>,
            checksum -> Nullable<Text>,
        }
    }

    diesel::joinable!(chat -> dataset (ds_uuid));
    diesel::joinable!(message -> dataset (ds_uuid));
    diesel::joinable!(message_content -> message (message_internal_id));
    diesel::joinable!(message_text_element -> message (message_internal_id));
    diesel::joinable!(user -> dataset (ds_uuid));

    diesel::allow_tables_to_appear_in_same_query!(
        chat,
        chat_member,
        dataset,
        message,
        message_content,
        message_text_element,
        refinery_schema_history,
        user,
    );
}

//
// Entities
//

#[derive(Debug, PartialEq, Selectable, Queryable, Insertable)]
#[diesel(table_name = schema::dataset)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct RawDataset {
    pub uuid: Vec<u8>,
    pub alias: String,
}

#[derive(Debug, PartialEq, Selectable, Queryable, Insertable, AsChangeset)]
#[diesel(table_name = schema::user)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
#[diesel(treat_none_as_null = true)]
pub struct RawUser {
    pub ds_uuid: Vec<u8>,
    pub id: i64,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub username: Option<String>,
    pub phone_numbers: Option<String>,
    pub is_myself: i32,
}

#[derive(Debug, PartialEq, QueryableByName, Insertable, AsChangeset)]
#[diesel(table_name = schema::chat)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
#[diesel(treat_none_as_null = true)]
pub struct RawChat {
    pub ds_uuid: Vec<u8>,
    pub id: i64,
    pub name: Option<String>,
    pub source_type: String,
    pub tpe: String,
    pub img_path: Option<String>,
    pub msg_count: i32,
    // Unused for now
    pub main_chat_id: Option<i64>,
}

// We cannot use #[diesel(belongs_to(...))] because Diesel doesn't support multi-column foreign keys.
#[derive(Debug, PartialEq, Identifiable, Selectable, Queryable, Insertable)]
#[diesel(primary_key(ds_uuid, chat_id, user_id))]
// #[diesel(belongs_to(RawChat, foreign_key = (ds_uuid, chat_id)))]
// #[diesel(belongs_to(RawUser, foreign_key = (ds_uuid, user_id)))]
#[diesel(table_name = schema::chat_member)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct RawChatMember {
    pub ds_uuid: Vec<u8>,
    pub chat_id: i64,
    pub user_id: i64,
    pub order: i32,
}

#[derive(Debug, PartialEq, QueryableByName)]
#[diesel(table_name = schema::chat)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
#[diesel(treat_none_as_null = true)]
pub struct RawChatQ {
    #[diesel(embed)]
    pub chat: RawChat,

    // Derived, artificial fields
    #[diesel(sql_type = Nullable < Text >)]
    pub member_ids: Option<String>,

    #[diesel(sql_type = Nullable < BigInt >)]
    pub last_message_internal_id: Option<i64>,
}

#[derive(Debug, PartialEq, Clone, Identifiable, Selectable, Queryable, Insertable)]
#[diesel(primary_key(internal_id))]
#[diesel(table_name = schema::message)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
#[diesel(treat_none_as_null = true)]
pub struct RawMessage {
    #[diesel(deserialize_as = i64)]
    pub internal_id: Option<i64>,
    pub ds_uuid: Vec<u8>,
    pub chat_id: i64,
    pub source_id: Option<i64>,
    pub tpe: String,
    pub subtype: Option<String>,
    pub time_sent: i64,
    pub time_edited: Option<i64>,
    /// Boolean value
    pub is_deleted: i32,
    pub from_id: i64,
    pub forward_from_name: Option<String>,
    pub reply_to_message_id: Option<i64>,
}

#[derive(Debug, PartialEq, Default, Identifiable, Selectable, Queryable, Insertable, Associations)]
#[diesel(belongs_to(RawMessage, foreign_key = message_internal_id))]
#[diesel(table_name = schema::message_content)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
#[diesel(treat_none_as_null = true)]
pub struct RawMessageContent {
    #[diesel(deserialize_as = i64)]
    pub id: Option<i64>,
    pub message_internal_id: i64,

    pub element_type: String,

    pub path: Option<String>,
    pub thumbnail_path: Option<String>,
    pub emoji: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub mime_type: Option<String>,
    pub title: Option<String>,
    pub performer: Option<String>,
    pub duration_sec: Option<i32>,
    pub is_one_time: Option<i32>,
    pub lat: Option<String>,
    pub lon: Option<String>,
    pub address: Option<String>,
    pub poll_question: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub phone_number: Option<String>,
    pub members: Option<String>,
    pub discard_reason: Option<String>,
    pub pinned_message_id: Option<i64>,
    /// Boolean value
    pub is_blocked: Option<i32>,
}

#[derive(Debug, PartialEq, Identifiable, Selectable, Queryable, Insertable, Associations)]
#[diesel(belongs_to(RawMessage, foreign_key = message_internal_id))]
#[diesel(table_name = schema::message_text_element)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
#[diesel(treat_none_as_null = true)]
pub struct RawRichTextElement {
    #[diesel(deserialize_as = i64)]
    pub id: Option<i64>,
    // This is not supposed to be Option, but Self::belonging_to(&raw_messages) doesn't typecheck otherwise
    pub message_internal_id: Option<i64>,
    pub element_type: String,
    pub text: Option<String>,
    pub href: Option<String>,
    /// Boolean value
    pub hidden: Option<i32>,
    pub language: Option<String>,
}

pub struct FullRawMessage {
    pub m: RawMessage,
    pub mc: Option<RawMessageContent>,
    pub rtes: Vec<RawRichTextElement>,
}
