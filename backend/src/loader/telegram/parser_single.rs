use itertools::Itertools;

use super::*;

pub(super) fn parse(root_obj: &Object,
                    ds_uuid: &PbUuid,
                    myself: &mut User,
                    myself_chooser: &dyn MyselfChooser) -> Result<(Users, Vec<ChatWithMessages>)> {
    let mut users: Users = Default::default();
    let mut chats_with_messages: Vec<ChatWithMessages> = vec![];

    let cwm_option =
        parse_chat("<root>", root_obj, ds_uuid, None, &mut users)?;
    match cwm_option {
        None =>
            bail!("Chat was skipped entirely!"),
        Some(mut cwm) => {
            cwm.chat.ds_uuid = ds_uuid.clone();
            chats_with_messages.push(cwm);
        }
    }

    // In single chat, self section is not present. As such, myself must be populated from users.
    let mut users_vec = users.id_to_user.values().cloned().collect_vec();
    let myself_idx = myself_chooser.choose_myself(&users_vec)?;
    *myself = users_vec.swap_remove(myself_idx);

    Ok((users, chats_with_messages))
}
