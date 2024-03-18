--
-- Schema
--

CREATE TABLE conversation_info (
    user_id                             text primary key on conflict replace,
    gender                              integer,
    user_name                           text,
    user_image_url                      text,
    user_deleted                        boolean,
    max_unanswered_messages             integer,
    sending_multimedia_enabled          integer,
    disabled_multimedia_explanation     text,
    multimedia_visibility_options       text,
    enlarged_emojis_max_count           integer,
    photo_url                           text,
    age                                 integer not null,
    is_inapp_promo_partner              boolean,
    game_mode                           integer,
    match_status                        text,
    chat_theme_settings                 text,
    chat_input_settings                 text    not null,
    is_open_profile_enabled             boolean,
    conversation_type                   text    not null,
    extra_message                       text,
    user_photos                         text    not null,
    photo_id                            text,
    work                                text,
    education                           text,
    photo_count                         integer not null,
    common_interest_count               integer not null,
    bumped_into_count                   integer not null,
    is_liked_you                        boolean not null,
    forwarding_settings                 text,
    is_reply_allowed                    boolean not null,
    live_location_settings              text,
    is_disable_private_detector_enabled boolean not null,
    member_count                        integer,
    is_url_parsing_allowed              boolean not null,
    is_user_verified                    boolean not null,
    last_message_status                 text,
    encrypted_user_id                   text,
    covid_preferences                   text,
    mood_status_emoji                   text,
    mood_status_name                    text,
    show_dating_hub_entry_point         boolean not null,
    hive_id                             text,
    hive_pending_join_request_count     integer,
    last_seen_message_id                text,
    is_best_bee                         boolean not null default 0,
    photo_background_color              integer
);

CREATE TABLE message (
    _id                   integer primary key autoincrement,
    id                    text unique,
    conversation_id       text    not null,
    sender_id             text,
    sender_name           text,
    recipient_id          text    not null,
    created_timestamp     int     not null,
    modified_timestamp    int     not null,
    status                text    not null,
    is_masked             int     not null,
    payload               text    not null,
    reply_to_id           text,
    is_reply_allowed      boolean not null,
    is_forwarded          boolean not null,
    is_forwarding_allowed boolean not null,
    send_error_type       string,
    sender_avatar_url     text,
    is_incoming           boolean not null,
    payload_type          text    not null,
    is_liked              int     not null,
    is_like_allowed       int     not null,
    is_likely_offensive   boolean not null,
    clear_chat_version    int     not null
);

--
-- Users
--

INSERT INTO conversation_info
VALUES ('1234567890', 1, 'Abcde', 'https://us1.badoocdn.com/some/irrelevant/url', 0, NULL, 1, NULL, NULL, 3,
        'https://us1.badoocdn.com/some/irrelevant/url', 27, 0, NULL, NULL, NULL,
        '{"json":"doesn''t matter"}', 1, 'User', '',
        '[{"id":"1375194859","url":"https:\/\/us1.badoocdn.com\/some/irrelevant/url"}]', '1375194859', NULL, NULL, 0, 0,
        0, 0, NULL, 1, NULL, 0, NULL, 0, 0, NULL,
        'abcde-encrypted-id', NULL, NULL, NULL, 0, NULL, NULL, '4313683957', 0, NULL);

--
-- Chats
--

INSERT INTO message
VALUES (1, '4313483375', '1234567890', 'abcde-encrypted-id', NULL, 'my-encrypypted-id', 1687425601000, 1687425601000,
        'ON_SERVER', 0, '{"text":"Hello there!","type":"TEXT","substitute_id":""}', NULL,
        1, 0, 0, NULL, NULL, 1, 'TEXT', 0, 1, 0, 1374986756);
INSERT INTO message
VALUES (2, '4313483378', '1234567890', 'my-encrypypted-id', NULL, 'abcde-encrypted-id', 1687425658000, 1687425658000,
        'ON_SERVER', 0, '{"text":"Reply there!","type":"TEXT","substitute_id":""}', '4313483375',
        1, 0, 0, NULL, NULL, 0, 'TEXT', 0, 0, 0, 1374986756);

INSERT INTO message
VALUES (3, '4313658961', '1234567890', 'abcde-encrypted-id', NULL, 'my-encrypypted-id', 1690856116000, 1690856116000,
        'ON_SERVER', 0,
        '{"id":"1375308869","waveform":[0,7,4,5,3,0],"url":"https:\/\/us1.badoocdn.com\/some/irrelevant/url","duration":23650,"expiration_timestamp":1695258720000}',
        NULL, 1, 0, 0, NULL, NULL, 1, 'AUDIO', 0, 1, 0, 1375179843);

INSERT INTO message
VALUES (4, '4313616080', '1234567890', 'abcde-encrypted-id', NULL, 'my-encrypypted-id', 1692781351000, 1692781351000,
        'ON_SERVER', 0,
        '{"photo_id":"1374985678","photo_url":"https:\/\/us1.badoocdn.com\/some/irrelevant/url","photo_width":640,"photo_height":480,"photo_expiration_timestamp":1693389603000,"emoji_reaction":"🤔","message":"Abcde reacted to your profile"}',
        NULL, 1, 0, 0, NULL, NULL, 1, 'REACTION', 0, 0, 0, 1375123987);
