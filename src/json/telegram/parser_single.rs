use crate::json::*;
use crate::json::telegram::*;

pub fn parse(root_obj: &Object,
             ds_uuid: &PbUuid,
             myself: &mut User) -> Res<(Users, Vec<ChatWithMessages>)> {
    let mut users: Users = Default::default();
    let mut chats_with_messages: Vec<ChatWithMessages> = vec![];

    users.insert(myself.clone());

    let mut cwm = parse_chat(root_obj, &ds_uuid, &myself.id, &mut users)?;
    if let Some(ref mut c) = cwm.chat {
        c.ds_uuid = Some(ds_uuid.clone());
    }
    chats_with_messages.push(cwm);

    Ok((users, chats_with_messages))
}
