use std::collections::HashSet;
use crate::protobuf::history::*;

use super::*;

/// Entity practical equality.
///
/// Practical equality is different from full equality like this:
/// 1. Internal IDs are ignored.
/// 2. External content paths might differ BUT the content itself must match.
/// 3. "Forwarded from" name is ignored (as its changes are not related to this message).
pub trait PracticalEq<Rhs/*: ?Sized*/ = Self> {
    fn practically_equals(&self, other: &Rhs) -> Result<bool>;
}

pub struct PracticalEqTuple<'a, T: 'a> {
    pub v: &'a T,
    pub ds_root: &'a DatasetRoot,
    pub cwd: &'a ChatWithDetails,
}

//
// General
//

type Tup<'a, T> = PracticalEqTuple<'a, T>;

impl<'a, T: 'a> Tup<'a, T> {
    pub fn new(v: &'a T, ds_root: &'a DatasetRoot, cwd: &'a ChatWithDetails) -> Self {
        Self { v, ds_root, cwd }
    }

    pub fn with<U>(&self, u: &'a U) -> Tup<'a, U> {
        Tup::new(u, self.ds_root, self.cwd)
    }

    pub fn apply<U>(&self, f: fn(&T) -> &U) -> Tup<'a, U> {
        Tup::new(f(self.v), self.ds_root, self.cwd)
    }
}

impl<'a, T: 'a> PracticalEq for Tup<'a, Option<T>> where for<'b> Tup<'a, T>: PracticalEq {
    fn practically_equals(&self, other: &Self) -> Result<bool> {
        match (self.v, other.v) {
            (None, None) => Ok(true),
            (Some(v1), Some(v2)) => self.with(v1).practically_equals(&other.with(v2)),
            _ => Ok(false),
        }
    }
}

macro_rules! cloned_equals_without {
    ($v1:expr, $v2:expr, $T:ident, $($key:ident : $val:expr),+) => {
        $T { $( $key: $val, )* ..(*$v1).clone() } == $T { $( $key: $val, )* ..(*$v2).clone() }
    };
}

//
// Chat
//

impl<'a> PracticalEq for Tup<'a, Chat> {
    fn practically_equals(&self, other: &Self) -> Result<bool> {
        Ok(cloned_equals_without!(self.v, other.v, Chat, img_path_option: None, member_ids: vec![]) &&
            self.v.member_ids.len() == other.v.member_ids.len() &&
            self.v.member_ids.iter().all(|e| other.v.member_ids.contains(e)) &&
            self.apply(|v| &v.img_path_option).practically_equals(&other.apply(|v| &v.img_path_option))?)
    }
}

//
//  Message
//

impl<'a> PracticalEq for Tup<'a, Message> {
    fn practically_equals(&self, other: &Self) -> Result<bool> {
        Ok(cloned_equals_without!(self.v, other.v, Message,
                                  internal_id: 0,
                                  source_id_option: None,
                                  searchable_string: "".to_owned(),
                                  typed: None) &&
            self.apply(|v| v.typed()).practically_equals(&other.apply(|v| v.typed()))?)
    }
}

impl<'a> PracticalEq for Tup<'a, message::Typed> {
    fn practically_equals(&self, other: &Self) -> Result<bool> {
        use message::Typed::*;
        match (self.v, other.v) {
            (Regular(c1), Regular(c2)) => self.with(c1).practically_equals(&other.with(c2)),
            (Service(c1), Service(c2)) => self.with(c1).practically_equals(&other.with(c2)),
            _ => Ok(false)
        }
    }
}

impl<'a> PracticalEq for Tup<'a, MessageRegular> {
    fn practically_equals(&self, other: &Self) -> Result<bool> {
        Ok(cloned_equals_without!(self.v, other.v, MessageRegular, forward_from_name_option: None, content_option: None) &&
            self.apply(|v| &v.content_option).practically_equals(&other.apply(|v| &v.content_option))?)
    }
}

impl<'a> PracticalEq for Tup<'a, MessageService> {
    fn practically_equals(&self, other: &Self) -> Result<bool> {
        use message_service::SealedValueOptional::*;
        macro_rules! case {
            ($T:ident, $name1:ident, $name2:ident) => { (Some($T($name1)), Some($T($name2))) };
        }
        match (self.v.sealed_value_optional.as_ref(), other.v.sealed_value_optional.as_ref()) {
            case!(PhoneCall, c1, c2) => Ok(c1 == c2),
            case!(SuggestProfilePhoto, c1, c2) => {
                // Only need to compare photos
                self.with(c1).apply(|c| &c.photo).practically_equals(&other.with(c2).apply(|c| &c.photo))
            }
            case!(PinMessage, c1, c2) => Ok(c1 == c2),
            case!(ClearHistory, c1, c2) => Ok(c1 == c2),
            case!(BlockUser, c1, c2) => Ok(c1 == c2),
            case!(StatusTextChanged, c1, c2) => Ok(c1 == c2),
            case!(Notice, c1, c2) => Ok(c1 == c2),
            case!(GroupCreate, c1, c2) =>
                Ok(c1.title == c2.title &&
                    members_practically_equals((&c1.members, self.cwd), (&c2.members, other.cwd))?),
            case!(GroupEditTitle, c1, c2) => Ok(c1 == c2),
            case!(GroupEditPhoto, c1, c2) => {
                // Only need to compare photos
                self.with(c1).apply(|c| &c.photo).practically_equals(&other.with(c2).apply(|c| &c.photo))
            }
            case!(GroupDeletePhoto, c1, c2) => Ok(c1 == c2),
            case!(GroupInviteMembers, c1, c2) =>
                members_practically_equals((&c1.members, self.cwd), (&c2.members, other.cwd)),
            case!(GroupRemoveMembers, c1, c2) =>
                members_practically_equals((&c1.members, self.cwd), (&c2.members, other.cwd)),
            case!(GroupMigrateFrom, c1, c2) => Ok(c1 == c2),
            case!(GroupMigrateTo, c1, c2) => Ok(c1 == c2),
            case!(GroupCall, c1, c2) =>
                members_practically_equals((&c1.members, self.cwd), (&c2.members, other.cwd)),
            _ => Ok(false)
        }
    }
}

//
// Content
//

impl<'a> PracticalEq for Tup<'a, Content> {
    fn practically_equals(&self, other: &Self) -> Result<bool> {
        use content::SealedValueOptional::*;
        match (self.v.sealed_value_optional.as_ref(), other.v.sealed_value_optional.as_ref()) { // @formatter:off
            (Some(Sticker(c1)),       Some(Sticker(c2)))       => self.with(c1).practically_equals(&other.with(c2)),
            (Some(Photo(c1)),         Some(Photo(c2)))         => self.with(c1).practically_equals(&other.with(c2)),
            (Some(VoiceMsg(c1)),      Some(VoiceMsg(c2)))      => self.with(c1).practically_equals(&other.with(c2)),
            (Some(Audio(c1)),         Some(Audio(c2)))         => self.with(c1).practically_equals(&other.with(c2)),
            (Some(VideoMsg(c1)),      Some(VideoMsg(c2)))      => self.with(c1).practically_equals(&other.with(c2)),
            (Some(Video(c1)),         Some(Video(c2)))         => self.with(c1).practically_equals(&other.with(c2)),
            (Some(File(c1)),          Some(File(c2)))          => self.with(c1).practically_equals(&other.with(c2)),
            (Some(Location(c1)),      Some(Location(c2)))      => self.with(c1).practically_equals(&other.with(c2)),
            (Some(Poll(c1)),          Some(Poll(c2)))          => self.with(c1).practically_equals(&other.with(c2)),
            (Some(SharedContact(c1)), Some(SharedContact(c2))) => self.with(c1).practically_equals(&other.with(c2)),
            _ => Ok(false)
        } // @formatter:on
    }
}

/// Treating String as Relative Path here.
/// (Cannot use newtype idiom - there's nobody to own the value)
impl<'a> PracticalEq for Tup<'a, String> {
    fn practically_equals(&self, other: &Self) -> Result<bool> {
        files_are_equal(&self.ds_root.0.join(self.v), &other.ds_root.0.join(other.v))
    }
}

macro_rules! practical_eq_with_path {
    ($T:ident, $($x:ident),+) => {
        impl<'a> PracticalEq for Tup<'a, $T> {
            fn practically_equals(&self, other: &Self) -> Result<bool> {
                Ok($( self.apply(|v| &v.$x).practically_equals(&other.apply(|v| &v.$x))? && )*
                    $T { $( $x: None, )* ..(*self.v).clone() } == $T { $( $x: None, )* ..(*other.v).clone() } &&
                    true)
            }
        }
    };
}

practical_eq_with_path!(ContentSticker, path_option, thumbnail_path_option);
practical_eq_with_path!(ContentPhoto, path_option);
practical_eq_with_path!(ContentVoiceMsg, path_option);
practical_eq_with_path!(ContentAudio, path_option);
practical_eq_with_path!(ContentVideoMsg, path_option, thumbnail_path_option);
practical_eq_with_path!(ContentVideo, path_option, thumbnail_path_option);
practical_eq_with_path!(ContentFile, path_option, thumbnail_path_option);
practical_eq_with_path!(ContentSharedContact, vcard_path_option);

impl<'a> PracticalEq for Tup<'a, ContentPoll> {
    fn practically_equals(&self, other: &Self) -> Result<bool> {
        // We don't really care about poll result
        Ok(self.v == other.v)
    }
}

impl<'a> PracticalEq for Tup<'a, ContentLocation> {
    fn practically_equals(&self, other: &Self) -> Result<bool> {
        // lat/lon are strings, trailing zeros should be ignored,
        Ok(self.v.lat()? == other.v.lat()? && self.v.lon()? == other.v.lon()? &&
            cloned_equals_without!(self.v, other.v, ContentLocation, lat_str: "".to_owned(), lon_str: "".to_owned()))
    }
}

//
// Helper functions
//

fn members_practically_equals((members1, cwd1): (&[String], &ChatWithDetails),
                              (members2, cwd2): (&[String], &ChatWithDetails)) -> Result<bool> {
    fn resolve_ids_set(members: &[String], cwd: &ChatWithDetails) -> HashSet<Option<i64>> {
        HashSet::from_iter(cwd.resolve_members(members).iter().map(|o| o.map(|u| u.id)))
    }
    let members1 = resolve_ids_set(members1, cwd1);
    let members2 = resolve_ids_set(members2, cwd2);
    Ok(members1 == members2)
}
