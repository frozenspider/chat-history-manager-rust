use crate::json::*;
use crate::json::telegram::*;

pub fn parse(root_obj: &Object,
             ds_uuid: &PbUuid,
             myself: &mut User) -> Res<(Users, Vec<ChatWithMessages>)> {
    let mut users: Users = Default::default();
    let mut chats_with_messages: Vec<ChatWithMessages> = vec!();

    parse_object(root_obj, "root", ActionMap::from([
        ("about", consume()),
        ("profile_pictures", consume()),
        ("frequent_contacts", consume()),
        ("other_data", consume()),
        ("contacts", Box::new(|v: &BorrowedValue| {
            parse_bw_as_object(v, "personal_information", ActionMap::from([
                ("about", consume()),
                ("list", Box::new(|v: &BorrowedValue| {
                    for v in v.as_array().ok_or("contact list is not an array!")? {
                        let mut contact = parse_contact(v, "contact")?;
                        contact.ds_uuid = Some(ds_uuid.clone());
                        users.insert(contact);
                    }
                    Ok(())
                })),
            ]))?;
            Ok(())
        })),
        ("personal_information", Box::new(|v: &BorrowedValue| {
            parse_bw_as_object(v, "personal_information", ActionMap::from([
                ("about", consume()),
                ("user_id", Box::new(|v: &BorrowedValue| {
                    myself.id = as_i64!(v, "ID");
                    Ok(())
                })),
                ("first_name", Box::new(|v: &BorrowedValue| {
                    myself.first_name_option = Some(as_string!(v, "first_name"));
                    Ok(())
                })),
                ("last_name", Box::new(|v: &BorrowedValue| {
                    myself.last_name_option = Some(as_string!(v, "last_name"));
                    Ok(())
                })),
                ("username", Box::new(|v: &BorrowedValue| {
                    myself.username_option = Some(as_string!(v, "username"));
                    Ok(())
                })),
                ("phone_number", Box::new(|v: &BorrowedValue| {
                    myself.phone_number_option = Some(as_string!(v, "phone_number"));
                    Ok(())
                })),
                ("bio", consume()),
            ]))
        })),
        ("chats", consume() /* Cannot borrow users the second time here! */),
        ("left_chats", consume() /* Cannot borrow users the second time here! */),
    ]))?;

    users.insert(myself.clone());

    fn parse_chats_inner(section: &str,
                         chat_json: &Object,
                         ds_uuid: &PbUuid,
                         myself_id: &Id,
                         users: &mut Users,
                         chats_with_messages: &mut Vec<ChatWithMessages>) -> EmptyRes {
        let chats_arr = chat_json
            .get("list").ok_or("No chats list in dataset!")?
            .as_array().ok_or(format!("{section} list is not an array!"))?;

        for v in chats_arr {
            if let Some(mut cwm) = parse_chat(as_object!(v, "chat"),
                                              &ds_uuid, myself_id, users)? {
                let mut c = cwm.chat.as_mut().unwrap();
                c.ds_uuid = Some(ds_uuid.clone());
                chats_with_messages.push(cwm);
            }
        }

        Ok(())
    }

    match root_obj.get("chats") {
        Some(chats_json) => parse_chats_inner(
            "chats", as_object!(chats_json, "chats"),
            &ds_uuid, &myself.id, &mut users, &mut chats_with_messages,
        )?,
        None => return Err(String::from("No chats in dataset!")),
    }

    // We don't want to import "left_chats" section!

    Ok((users, chats_with_messages))
}
