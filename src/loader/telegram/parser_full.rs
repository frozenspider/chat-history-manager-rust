use super::*;

pub(super) fn parse(root_obj: &Object,
                    ds_uuid: &PbUuid,
                    myself: &mut User) -> Result<(Users, Vec<ChatWithMessages>)> {
    let mut users: Users = Default::default();
    let mut chats_with_messages: Vec<ChatWithMessages> = vec![];

    parse_object(root_obj, "root", |CB { key, value, wrong_key_action }| match key {
        "about" => consume(),
        "profile_pictures" => consume(),
        "frequent_contacts" => consume(),
        "other_data" => consume(),
        "contacts" =>
            parse_bw_as_object(value, "personal_information", |CB { key, value, wrong_key_action }| match key {
                "about" => consume(),
                "list" => {
                    for v in value.as_array().ok_or(anyhow!("Contact list is not an array!"))? {
                        let mut contact = parse_contact("contact", v)?;
                        contact.ds_uuid = Some(ds_uuid.clone());
                        users.insert(contact);
                    }
                    Ok(())
                }
                _ => wrong_key_action()
            }),
        "personal_information" => {
            let json_path = "personal_information";
            parse_bw_as_object(value, json_path, |CB { key, value: v, wrong_key_action }| match key {
                "about" => consume(),
                "user_id" => {
                    myself.id = as_i64!(v, json_path, "user_id");
                    Ok(())
                }
                "first_name" => {
                    myself.first_name_option = Some(as_string!(v, json_path, "first_name"));
                    Ok(())
                }
                "last_name" => {
                    myself.last_name_option = Some(as_string!(v, json_path, "last_name"));
                    Ok(())
                }
                "username" => {
                    myself.username_option = Some(as_string!(v, json_path, "username"));
                    Ok(())
                }
                "phone_number" => {
                    myself.phone_number_option = Some(as_string!(v, json_path, "phone_number"));
                    Ok(())
                }
                "bio" => consume(),
                _ => wrong_key_action()
            })
        }
        "chats" => {
            let json_path = "chats";

            let chats_arr = as_object!(value, "chats")
                .get("list").ok_or(anyhow!("No chats list in dataset!"))?
                .as_array().ok_or(anyhow!("{json_path} list is not an array!"))?;

            for v in chats_arr {
                if let Some(mut cwm) = parse_chat(json_path, as_object!(v, json_path, "chat"),
                                                  ds_uuid, Some(&myself.id()), &mut users)? {
                    let c = cwm.chat.as_mut().unwrap();
                    c.ds_uuid = Some(ds_uuid.clone());
                    chats_with_messages.push(cwm);
                }
            }

            Ok(())
        }
        "left_chats" => {
            // We don't want to import "left_chats" section!
            consume()
        }
        _ => wrong_key_action()
    })?;

    users.insert(myself.clone());

    Ok((users, chats_with_messages))
}
