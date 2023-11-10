--
-- Schema
--
CREATE TABLE call_log (    _id                              INTEGER PRIMARY KEY AUTOINCREMENT,    jid_row_id                       INTEGER,    from_me                          INTEGER,    call_id                          TEXT,    transaction_id                   INTEGER,    timestamp                        INTEGER,    video_call                       INTEGER,    duration                         INTEGER,    call_result                      INTEGER,    bytes_transferred                INTEGER,    group_jid_row_id                 INTEGER NOT NULL DEFAULT 0,    is_joinable_group_call           INTEGER,    call_creator_device_jid_row_id   INTEGER NOT NULL DEFAULT 0, call_random_id TEXT, call_link_row_id INTEGER NOT NULL DEFAULT 0, is_dnd_mode_on INTEGER, call_type INTEGER, offer_silence_reason INTEGER, scheduled_id TEXT);
CREATE TABLE chat (_id INTEGER PRIMARY KEY AUTOINCREMENT,jid_row_id INTEGER UNIQUE,hidden INTEGER,subject TEXT,created_timestamp INTEGER,display_message_row_id INTEGER,last_message_row_id INTEGER,last_read_message_row_id INTEGER,last_read_receipt_sent_message_row_id INTEGER,last_important_message_row_id INTEGER,archived INTEGER,sort_timestamp INTEGER,mod_tag INTEGER,gen REAL,spam_detection INTEGER,unseen_earliest_message_received_time INTEGER,unseen_message_count INTEGER,unseen_missed_calls_count INTEGER,unseen_row_count INTEGER,plaintext_disabled INTEGER,vcard_ui_dismissed INTEGER,change_number_notified_message_row_id INTEGER,show_group_description INTEGER,ephemeral_expiration INTEGER,last_read_ephemeral_message_row_id INTEGER,ephemeral_setting_timestamp INTEGER, unseen_important_message_count INTEGER NOT NULL DEFAULT 0, ephemeral_disappearing_messages_initiator INTEGER, group_type INTEGER NOT NULL DEFAULT 0, last_message_reaction_row_id INTEGER, last_seen_message_reaction_row_id INTEGER, unseen_message_reaction_count INTEGER, growth_lock_level INTEGER, growth_lock_expiration_ts INTEGER, last_read_message_sort_id INTEGER, display_message_sort_id INTEGER, last_message_sort_id INTEGER, last_read_receipt_sent_message_sort_id INTEGER, has_new_community_admin_dialog_been_acknowledged INTEGER NOT NULL DEFAULT 0, history_sync_progress INTEGER, ephemeral_displayed_exemptions INTEGER, chat_lock INTEGER);
CREATE TABLE jid (_id INTEGER PRIMARY KEY AUTOINCREMENT, user TEXT NOT NULL, server TEXT NOT NULL, agent INTEGER, device INTEGER, type INTEGER, raw_string TEXT);
CREATE TABLE message (_id INTEGER PRIMARY KEY AUTOINCREMENT, chat_row_id INTEGER NOT NULL, from_me INTEGER NOT NULL, key_id TEXT NOT NULL, sender_jid_row_id INTEGER, status INTEGER, broadcast INTEGER, recipient_count INTEGER, participant_hash TEXT, origination_flags INTEGER, origin INTEGER, timestamp INTEGER, received_timestamp INTEGER, receipt_server_timestamp INTEGER, message_type INTEGER, text_data TEXT, starred INTEGER, lookup_tables INTEGER, sort_id INTEGER NOT NULL DEFAULT 0 , message_add_on_flags INTEGER, view_mode INTEGER);
CREATE TABLE message_edit_info (message_row_id INTEGER PRIMARY KEY, original_key_id TEXT NOT NULL, edited_timestamp INTEGER NOT NULL, sender_timestamp INTEGER NOT NULL);
CREATE TABLE message_forwarded(message_row_id INTEGER PRIMARY KEY, forward_score INTEGER);
CREATE TABLE message_location (message_row_id INTEGER PRIMARY KEY, chat_row_id INTEGER, latitude REAL, longitude REAL, place_name TEXT, place_address TEXT, url TEXT, live_location_share_duration INTEGER, live_location_sequence_number INTEGER, live_location_final_latitude REAL, live_location_final_longitude REAL, live_location_final_timestamp INTEGER, map_download_status INTEGER);
CREATE TABLE message_media (  message_row_id INTEGER PRIMARY KEY, chat_row_id INTEGER, autotransfer_retry_enabled INTEGER, multicast_id TEXT, media_job_uuid TEXT, transferred INTEGER, transcoded INTEGER, file_path TEXT, file_size INTEGER, suspicious_content INTEGER, trim_from INTEGER, trim_to INTEGER, face_x INTEGER, face_y INTEGER, media_key BLOB, media_key_timestamp INTEGER, width INTEGER, height INTEGER, has_streaming_sidecar INTEGER, gif_attribution INTEGER, thumbnail_height_width_ratio REAL, direct_path TEXT, first_scan_sidecar BLOB, first_scan_length INTEGER, message_url TEXT, mime_type TEXT, file_length INTEGER, media_name TEXT, file_hash TEXT, media_duration INTEGER, page_count INTEGER, enc_file_hash TEXT, partial_media_hash TEXT, partial_media_enc_hash TEXT, is_animated_sticker INTEGER, original_file_hash TEXT, mute_video INTEGER DEFAULT 0, media_caption TEXT, media_upload_handle TEXT);
CREATE TABLE message_quoted (    message_row_id             INTEGER PRIMARY KEY AUTOINCREMENT,    chat_row_id                INTEGER NOT NULL,    parent_message_chat_row_id INTEGER NOT NULL,    from_me                    INTEGER NOT NULL,    sender_jid_row_id          INTEGER,    key_id                     TEXT    NOT NULL,    timestamp                  INTEGER,    message_type               INTEGER,    origin                     INTEGER,    text_data                  TEXT,    payment_transaction_id     TEXT,    lookup_tables              INTEGER);
CREATE TABLE message_revoked (message_row_id INTEGER PRIMARY KEY, revoked_key_id TEXT NOT NULL, admin_jid_row_id INTEGER, revoke_timestamp INTEGER);
CREATE TABLE message_system (message_row_id INTEGER PRIMARY KEY, action_type INTEGER NOT NULL);
CREATE TABLE message_system_block_contact (message_row_id INTEGER PRIMARY KEY, is_blocked INTEGER);
CREATE TABLE message_system_chat_participant (message_row_id INTEGER, user_jid_row_id INTEGER);
CREATE TABLE message_system_group (message_row_id INTEGER PRIMARY KEY, is_me_joined INTEGER);
CREATE TABLE message_system_number_change (message_row_id INTEGER PRIMARY KEY, old_jid_row_id INTEGER, new_jid_row_id INTEGER);
CREATE TABLE message_vcard (_id  INTEGER PRIMARY KEY AUTOINCREMENT, message_row_id INTEGER, vcard TEXT);
CREATE TABLE props (_id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT UNIQUE, value TEXT);

--
-- Data
--

-- Myself (#jid = 264)
INSERT INTO jid VALUES(264,'00000','s.whatsapp.net',0,0,0,'00000@s.whatsapp.net');
INSERT INTO props VALUES(48857,'user_push_name','Aaaaa Aaaaaaaaaaa');

-- User 1 (#jid = 252)
INSERT INTO jid VALUES(252,'11111','s.whatsapp.net',0,0,0,'11111@s.whatsapp.net');



-- A group (#jid = 254)
INSERT INTO chat VALUES(19,254,0,'My Group',1643607839000,750,750,750,750,1,1,1661417508000,0,0.0,1,0,0,0,0,1,0,1,0,0,NULL,0,0,0,0,0,0,0,0,0,750,750,750,750,0,0,NULL,NULL);
INSERT INTO jid VALUES(254,'100000000000000001','g.us',0,0,1,'100000000000000001@g.us');

-- Myself joining a group (#msg = 169)
INSERT INTO message VALUES(169,19,1,'GROUPMSG00100',252,6,0,5,NULL,0,0,1643607839000,0,-1,7,'',0,0,169,0,NULL);
INSERT INTO message_system VALUES(169,12);
INSERT INTO message_system_chat_participant VALUES(169,264);
INSERT INTO message_system_group VALUES(169,1);

-- Last group message (#msg = 750), reply to first (system) message, edited and forwarded (probably not possible in real data)
INSERT INTO message VALUES(750,19,1,'GROUPMSG99999',0,0,0,4,NULL,0,0,1661417508000,1661417509709,-1,0,'Last group message',0,0,750,0,NULL);
INSERT INTO message_edit_info VALUES(750,'GROUPMSG99999OLD',1661417955000,1661417999999);
INSERT INTO message_forwarded VALUES(750,1);
INSERT INTO message_quoted VALUES(750,19,19,1,252,'GROUPMSG00100',1643607839000,7,0,'',NULL,0);


-- Personal chat with user 1 (jid = #252)
INSERT INTO chat VALUES(148,252,0,NULL,1687705763841,7747,7747,7756,7756,1,1,1696244219000,NULL,NULL,1,0,0,0,0,1,0,1,0,86400,NULL,1696243309000,0,0,0,55,55,0,NULL,NULL,7756,7747,7747,7756,0,0,0,0);

-- Sharing location (#msg = 4863)
INSERT INTO message VALUES(4863,148,0,'PERSONALMSG100100',0,0,0,0,NULL,0,0,1687757170000,1687757170352,-1,16,NULL,0,0,4863,0,NULL);
INSERT INTO message_location VALUES(4863,148,-8.7038565050269092182,115.21673666751774955,'New Bahari','Jl. Gurita No.21x, Denpasar, Bali','https://foursquare.com/v/51e14cff498e834f4f815e43',123,NULL,NULL,NULL,NULL,2);

-- Deleted message
INSERT INTO message VALUES(7454,148,1,'PERSONALMSG999900',0,5,0,0,NULL,0,0,1693993938000,1693995957435,-1,15,NULL,0,0,7454,0,NULL);
INSERT INTO message_revoked VALUES(7454,'PERSONALMSGDELETED',NULL,1693993963000);
