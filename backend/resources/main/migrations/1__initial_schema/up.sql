-- Even though SQLite unfortunately does not respect type parameter restrictions,
-- we keep them for documentation and historical purposes.

CREATE TABLE dataset (
  uuid                BLOB NOT NULL PRIMARY KEY,
  alias               TEXT NOT NULL
) STRICT, WITHOUT ROWID;

CREATE TABLE chat (
  ds_uuid             BLOB NOT NULL REFERENCES dataset (uuid),
  id                  INTEGER NOT NULL,
  name                TEXT,
  source_type         TEXT NOT NULL,
  type                TEXT NOT NULL,
  msg_count           INTEGER NOT NULL,
  img_path            TEXT,
  main_chat_id        INTEGER,
  PRIMARY KEY (ds_uuid, id)
) STRICT, WITHOUT ROWID;

CREATE TABLE user (
  ds_uuid             BLOB NOT NULL REFERENCES dataset (uuid),
  id                  INTEGER NOT NULL,
  first_name          TEXT,
  last_name           TEXT,
  username            TEXT,
  phone_numbers       TEXT, -- serialized
  is_myself           INTEGER NOT NULL, -- boolean
  PRIMARY KEY (ds_uuid, id)
) STRICT, WITHOUT ROWID;

CREATE TABLE chat_member (
  ds_uuid             BLOB NOT NULL,
  chat_id             INTEGER NOT NULL,
  user_id             INTEGER NOT NULL,
  "order"             INTEGER NOT NULL,
  PRIMARY KEY (ds_uuid, chat_id, user_id),
  FOREIGN KEY (ds_uuid, chat_id) REFERENCES chat (ds_uuid, id),
  FOREIGN KEY (ds_uuid, user_id) REFERENCES user (ds_uuid, id)
) STRICT;

CREATE TABLE message (
  internal_id         INTEGER PRIMARY KEY AUTOINCREMENT,
  ds_uuid             BLOB NOT NULL REFERENCES dataset (uuid),
  chat_id             INTEGER NOT NULL,
  source_id           INTEGER,
  type                TEXT NOT NULL,
  subtype             TEXT,
  time_sent           INTEGER NOT NULL, -- epoch seconds
  time_edited         INTEGER, -- epoch seconds
  is_deleted          INTEGER, -- boolean
  from_id             INTEGER NOT NULL,
  forward_from_name   TEXT,
  reply_to_message_id INTEGER, -- refers to source_id, although we don't declare it as FK

  FOREIGN KEY (ds_uuid, chat_id) REFERENCES chat (ds_uuid, id),
  FOREIGN KEY (ds_uuid, from_id) REFERENCES user (ds_uuid, id)
) STRICT;

CREATE UNIQUE INDEX message_source_id ON message(ds_uuid, chat_id, source_id); -- allows duplicate NULLs

-- Stores content, as well as added data for service messages.
-- Might be absent.
CREATE TABLE message_content (
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  message_internal_id INTEGER NOT NULL REFERENCES message (internal_id),

  element_type        TEXT NOT NULL, -- Will match message.subtype for service messages

  path                TEXT,
  thumbnail_path      TEXT,
  emoji               TEXT,
  width               INTEGER,
  height              INTEGER,
  mime_type           TEXT,
  title               TEXT,
  performer           TEXT,
  duration_sec        INTEGER,
  is_one_time         INTEGER, -- boolean
  lat                 TEXT, -- To not lose precision
  lon                 TEXT, -- To not lose precision
  address             TEXT,
  poll_question       TEXT,
  first_name          TEXT,
  last_name           TEXT,
  phone_number        TEXT,
  members             TEXT, -- serialized
  discard_reason      TEXT,
  pinned_message_id   INTEGER,
  is_blocked          INTEGER -- boolean
) STRICT;

CREATE UNIQUE INDEX message_content_idx ON message_content(message_internal_id);

CREATE TABLE message_text_element (
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  message_internal_id INTEGER NOT NULL REFERENCES message (internal_id),
  element_type        TEXT NOT NULL,
  text                TEXT,
  href                TEXT,
  hidden              INTEGER, -- boolean
  language            TEXT
) STRICT;

CREATE INDEX message_text_element_idx ON message_text_element(message_internal_id);
