--
-- Schema
--
CREATE TABLE match_person (
    id                TEXT NOT NULL PRIMARY KEY,
    name              TEXT NOT NULL,
    bio               TEXT,
    birth_date        INTEGER,
    gender            BLOB NOT NULL,
    photos            BLOB NOT NULL,
    badges            BLOB NOT NULL,
    jobs              BLOB NOT NULL,
    schools           BLOB NOT NULL,
    city              BLOB,
    membership_status TEXT
);

CREATE TABLE message (
    client_sequential_id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    id                       TEXT    NOT NULL UNIQUE,
    match_id                 TEXT    NOT NULL,
    to_id                    TEXT    NOT NULL,
    from_id                  TEXT    NOT NULL,
    text                     TEXT    NOT NULL,
    sent_date                INTEGER NOT NULL,
    is_liked                 INTEGER NOT NULL DEFAULT 0,
    type                     TEXT    NOT NULL,
    delivery_status          TEXT    NOT NULL,
    is_seen                  INTEGER NOT NULL DEFAULT 0,
    raw_message_data_version INTEGER NOT NULL DEFAULT -1,
    raw_message_data         TEXT    NOT NULL DEFAULT ""
);

--
-- Users
--
INSERT INTO match_person VALUES ('KEYU1', 'Abcde', 'My bio!', 848136841431, X'0801', X'', X'', X'', X'', NULL, NULL);

--
-- Chats
-- (raw_message_data is not populated)
--
INSERT INTO message
VALUES (275, '123456789ABCDEF000000', 'KEYU1MYKEY', 'KEYU1', 'MYKEY', 'Sending you a text!', 1699812983158, 0,
        'UNKNOWN', 'SUCCESS', 1, 0, '');
INSERT INTO message
VALUES (274, '123456789ABCDEF000001', 'KEYU1MYKEY', 'MYKEY', 'KEYU1',
        'https://media.tenor.com/mYFQztB4EHoAAAAC/house-hugh-laurie.gif?width=271&height=279', 1699813000165, 0,
        'UNKNOWN', 'SUCCESS', 1, 0, '');
