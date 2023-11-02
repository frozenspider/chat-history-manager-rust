#![allow(unused_imports)]

use chrono::prelude::*;
use itertools::Itertools;
use lazy_static::lazy_static;
use pretty_assertions::{assert_eq, assert_ne};

use crate::{NoChooser, User};
use crate::dao::ChatHistoryDao;
use crate::entity_utils::*;
use crate::protobuf::history::*;
use crate::protobuf::history::content::SealedValueOptional::*;
use crate::protobuf::history::message::*;
use crate::protobuf::history::message_service::SealedValueOptional::*;

use super::*;

const LOADER: WhatsAppTextDataLoader = WhatsAppTextDataLoader;

//
// Tests
//

#[test]
fn loading_2023_10() -> EmptyRes {
    let res = resource("whatsapp-text_2023-10/WhatsApp Chat with +123 45 6789.txt");
    LOADER.looks_about_right(&res)?;

    let dao = LOADER.load(&res, &NoChooser)?;

    let ds_uuid = dao.dataset.uuid.unwrap_ref();
    let myself = &dao.myself;
    assert_eq!(myself, &expected_myself(ds_uuid));

    let member = User {
        ds_uuid: Some(ds_uuid.clone()),
        id: 2708866474201423075_i64,
        first_name_option: None,
        last_name_option: None,
        username_option: None,
        phone_number_option: Some("+123 45 6789".to_owned()),
    };

    assert_eq!(dao.users, vec![myself.clone(), member.clone()]);

    assert_eq!(dao.cwms.len(), 1);

    {
        let cwm = &dao.cwms[0];
        let chat = cwm.chat.unwrap_ref();
        assert_eq!(chat.tpe, ChatType::Personal as i32);

        assert_eq!(chat.member_ids.len(), 2);
        assert!(chat.member_ids.contains(&myself.id));
        assert!(chat.member_ids.contains(&member.id));

        let msgs = dao.first_messages(&chat, 99999)?;
        assert_eq!(msgs.len(), 10);
        assert_eq!(chat.msg_count, 10);

        msgs.iter().for_each(|m| {
            assert!(matches!(m.typed.unwrap_ref(), Typed::Regular(_)));
        });

        assert_eq!(msgs[0], Message {
            internal_id: 0,
            source_id_option: None,
            timestamp: dt("2023-06-30 16:14:00", None).timestamp(),
            from_id: myself.id,
            text: vec![
                RichTextElement {
                    searchable_string: "hello there! this is a multi-line message!".to_owned(),
                    val: Some(rich_text_element::Val::Plain(RtePlain {
                        text: "hello there!\n\nthis is a\nmulti-line message!".to_owned()
                    })),
                },
            ],
            searchable_string: "hello there! this is a multi-line message!".to_owned(),
            typed: Some(MESSAGE_REGULAR_NO_CONTENT.clone()),
        });
        assert_eq!(msgs[1], Message {
            internal_id: 1,
            source_id_option: None,
            timestamp: dt("2023-06-30 16:14:01", None).timestamp(),
            from_id: myself.id,
            text: vec![
                RichTextElement {
                    searchable_string: "and these messages".to_owned(),
                    val: Some(rich_text_element::Val::Plain(RtePlain {
                        text: "and these messages".to_owned()
                    })),
                },
            ],
            searchable_string: "and these messages".to_owned(),
            typed: Some(MESSAGE_REGULAR_NO_CONTENT.clone()),
        });
        assert_eq!(msgs[2], Message {
            internal_id: 2,
            source_id_option: None,
            timestamp: dt("2023-06-30 16:14:02", None).timestamp(),
            from_id: myself.id,
            text: vec![
                RichTextElement {
                    searchable_string: "should not be reordered!".to_owned(),
                    val: Some(rich_text_element::Val::Plain(RtePlain {
                        text: "should not be reordered!".to_owned()
                    })),
                },
            ],
            searchable_string: "should not be reordered!".to_owned(),
            typed: Some(MESSAGE_REGULAR_NO_CONTENT.clone()),
        });
        assert_eq!(msgs[3], Message {
            internal_id: 3,
            source_id_option: None,
            timestamp: dt("2023-06-30 16:14:03", None).timestamp(),
            from_id: member.id,
            text: vec![
                RichTextElement {
                    searchable_string: "should not be reordered indeed!".to_owned(),
                    val: Some(rich_text_element::Val::Plain(RtePlain {
                        text: "should not be reordered indeed!".to_owned()
                    })),
                },
            ],
            searchable_string: "should not be reordered indeed!".to_owned(),
            typed: Some(MESSAGE_REGULAR_NO_CONTENT.clone()),
        });
        assert_eq!(msgs[4], Message {
            internal_id: 4,
            source_id_option: None,
            timestamp: dt("2023-06-30 16:15:00", None).timestamp(),
            from_id: member.id,
            text: vec![
                RichTextElement {
                    searchable_string: "image comment".to_owned(),
                    val: Some(rich_text_element::Val::Plain(RtePlain {
                        text: "image comment".to_owned()
                    })),
                }
            ],
            searchable_string: "image comment".to_owned(),
            typed: Some(Typed::Regular(MessageRegular {
                edit_timestamp_option: None,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: Some(Content {
                    sealed_value_optional: Some(Photo(ContentPhoto {
                        path_option: Some("IMG-20230630-WA0000.jpg".to_owned()),
                        width: 0,
                        height: 0,
                        is_one_time: false,
                    }))
                }),
            })),
        });
        assert_eq!(msgs[5], Message {
            internal_id: 5,
            source_id_option: None,
            timestamp: dt("2023-06-30 16:15:01", None).timestamp(),
            from_id: member.id,
            text: vec![],
            searchable_string: "".to_owned(),
            typed: Some(Typed::Regular(MessageRegular {
                edit_timestamp_option: None,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: Some(Content {
                    sealed_value_optional: Some(Video(ContentVideo {
                        path_option: Some("VID-20230630-WA0001.mp4".to_owned()),
                        title_option: None,
                        performer_option: None,
                        width: 0,
                        height: 0,
                        mime_type: "video/mp4".to_owned(),
                        duration_sec_option: None,
                        thumbnail_path_option: None,
                        is_one_time: false,
                    }))
                }),
            })),
        });
        assert_eq!(msgs[6], Message {
            internal_id: 6,
            source_id_option: None,
            timestamp: dt("2023-06-30 16:15:02", None).timestamp(),
            from_id: member.id,
            text: vec![],
            searchable_string: "".to_owned(),
            typed: Some(Typed::Regular(MessageRegular {
                edit_timestamp_option: None,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: Some(Content {
                    sealed_value_optional: Some(VoiceMsg(ContentVoiceMsg {
                        path_option: Some("AUD-20230630-WA0002.opus".to_owned()),
                        mime_type: "audio/ogg".to_owned(),
                        duration_sec_option: None,
                    }))
                }),
            })),
        });
        assert_eq!(msgs[7], Message {
            internal_id: 7,
            source_id_option: None,
            timestamp: dt("2023-06-30 16:15:03", None).timestamp(),
            from_id: member.id,
            text: vec![],
            searchable_string: "".to_owned(),
            typed: Some(Typed::Regular(MessageRegular {
                edit_timestamp_option: None,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: Some(Content {
                    sealed_value_optional: Some(Sticker(ContentSticker {
                        path_option: Some("STK-20230630-WA0003.webp".to_owned()),
                        width: 0,
                        height: 0,
                        thumbnail_path_option: None,
                        emoji_option: None,
                    }))
                }),
            })),
        });
        assert_eq!(msgs[8], Message {
            internal_id: 8,
            source_id_option: None,
            timestamp: dt("2023-06-30 16:15:04", None).timestamp(),
            from_id: member.id,
            text: vec![],
            searchable_string: "".to_owned(),
            typed: Some(Typed::Regular(MessageRegular {
                edit_timestamp_option: None,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: Some(Content {
                    sealed_value_optional: Some(FILE_UNAVAILABLE.clone())
                }),
            })),
        });
        assert_eq!(msgs[9], Message {
            internal_id: 9,
            source_id_option: None,
            timestamp: dt("2023-06-30 16:15:05", None).timestamp(),
            from_id: member.id,
            text: vec![],
            searchable_string: "".to_owned(),
            typed: Some(Typed::Regular(MessageRegular {
                edit_timestamp_option: None,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: Some(Content {
                    sealed_value_optional: Some(FILE_UNAVAILABLE.clone())
                }),
            })),
        });
    }
    Ok(())
}

//
// Helpers
//

fn expected_myself(ds_uuid: &PbUuid) -> User {
    User {
        ds_uuid: Some(ds_uuid.clone()),
        id: 1_i64,
        first_name_option: Some("Aaaaa Aaaaaaaaaaa".to_owned()),
        last_name_option: None,
        username_option: None,
        phone_number_option: None,
    }
}

lazy_static! {
    static ref MESSAGE_REGULAR_NO_CONTENT: Typed = Typed::Regular(MessageRegular {
        edit_timestamp_option: None,
        forward_from_name_option: None,
        reply_to_message_id_option: None,
        content_option: None,
    });

    static ref FILE_UNAVAILABLE: content::SealedValueOptional = File(ContentFile {
        path_option: None,
        file_name_option: None,
        mime_type_option: None,
        thumbnail_path_option: None,
    });
}
