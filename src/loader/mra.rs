#![allow(clippy::reversed_empty_ranges)]

use std::{cmp, fmt, fs, mem, slice};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::PathBuf;

use lazy_static::lazy_static;
use num_traits::FromPrimitive;
use regex::{Captures, Regex};
use utf16string::{LE, WStr};

use crate::*;
use crate::dao::in_memory_dao::{DatasetEntry, InMemoryDao};
use crate::loader::DataLoader;
use crate::protobuf::history::*;

use super::*;

mod mra_dbs;
mod db;

pub struct MailRuAgentDataLoader;

const MRA_DBS: &str = "mra.dbs";

/// Using a first legal ID (i.e. "1") for myself
const MYSELF_ID: UserId = UserId(UserId::INVALID.0 + 1);

lazy_static! {
    static ref DB_FILE_DIRS: Vec<&'static str> = vec!["Agent", "ICQ"];

    // Expected entries are @mail.ru, @bk.ru, @inbox.ru and @uin.icq.
    // Could also be @chat.agent, which indicates a group chat.
    static ref EMAIL_REGEX: Regex = Regex::new(r"^[a-zA-Z0-9._-]+@([a-z-]+\.)+[a-z]+$").unwrap();

    static ref SMILE_TAG_REGEX: Regex = Regex::new(r"<SMILE>id=(?<id>[^ ]+)( alt='(?<alt>[^']+)')?</SMILE>").unwrap();
    static ref SMILE_INLINE_REGEX: Regex = Regex::new(r":(([А-ЯË][^:\n]+)|([0-9]{3,})):").unwrap();
    static ref SMILE_IMG_REGEX: Regex = Regex::new(r"<###(?<prefix>\d+)###img(?<id>\d+)>").unwrap();
}

impl DataLoader for MailRuAgentDataLoader {
    fn name(&self) -> &'static str { "Mail.Ru Agent" }

    fn src_alias(&self) -> &'static str { "MRA" }

    fn src_type(&self) -> SourceType { SourceType::Mra }

    fn looks_about_right_inner(&self, path: &Path) -> EmptyRes {
        if path_file_name(path)? != MRA_DBS {
            bail!("Given file is not {MRA_DBS}")
        }
        Ok(())
    }

    fn load_inner(&self, path: &Path, ds: Dataset, _myself_chooser: &dyn MyselfChooser) -> Result<Box<InMemoryDao>> {
        // We're not using the supplied dataset, just the name of it
        load_mra_dbs(path, ds.alias)
    }
}

fn load_mra_dbs(path: &Path, dao_name: String) -> Result<Box<InMemoryDao>> {
    let mut storage_path = path.parent().expect("Database file has no parent!");

    for subdir in DB_FILE_DIRS.iter() {
        let path = storage_path.join(subdir);
        if path.exists() {
            db::do_the_thing(&path)?;
        }
    }

    return Ok(Box::new(InMemoryDao::new(dao_name, storage_path.to_path_buf(), vec![])));

    if path_file_name(storage_path)? == "Base" {
        storage_path = storage_path.parent().expect(r#""Base" directory has no parent!"#);
    }
    let storage_path = storage_path.to_path_buf();

    // Read the whole file into the memory.
    let dbs_bytes = fs::read(path)?;

    // We'll be loading chats in three phases.
    // Phase 1: Read conversations in an MRA inner format, mapped to file bytes.
    let convs_with_msgs = mra_dbs::load_convs_with_msgs(&dbs_bytes)?;

    // Phase 2: Populate datasets and users with latest values, usernames being emails.
    let dataset_map = mra_dbs::collect_datasets(&convs_with_msgs, &storage_path)?;

    // Phase 3: Convert conversations to our format.
    let data = mra_dbs::convert_messages(&convs_with_msgs, dataset_map, &dbs_bytes)?;

    Ok(Box::new(InMemoryDao::new(
        dao_name,
        storage_path,
        data,
    )))
}

fn convert_microblog_record(
    raw_text: &str,
    target_name: Option<&str>,
) -> (Vec<RichTextElement>, message::Typed) {
    let text = replace_smiles_with_emojis(&raw_text);
    let text = format!("{}{}", target_name.map(|n| format!("(To {n})\n")).unwrap_or_default(), text);
    (vec![RichText::make_plain(text)], message::Typed::Service(MessageService {
        sealed_value_optional: Some(message_service::SealedValueOptional::StatusTextChanged(MessageServiceStatusTextChanged {}))
    }))
}

//
// Structs and enums
//

struct MraDatasetEntry {
    ds: Dataset,
    ds_root: PathBuf,
    /// Key is username (in most cases, email)
    users: HashMap<String, User>,
    /// Key is conversation name (in most cases, email or email-like name)
    cwms: HashMap<String, ChatWithMessages>,
}

//
// Helper functions
//

// All read functions read in Little Endian

fn read_n_bytes<const N: usize>(bytes: &[u8], shift: usize) -> [u8; N] {
    bytes[shift..(shift + N)].try_into().unwrap()
}

fn read_u32(bytes: &[u8], shift: usize) -> u32 {
    u32::from_le_bytes(read_n_bytes(bytes, shift))
}

fn next_n_bytes<const N: usize>(bytes: &[u8]) -> ([u8; N], &[u8]) {
    (bytes[..N].try_into().unwrap(), &bytes[N..])
}

fn next_u32(bytes: &[u8]) -> (u32, &[u8]) {
    (read_u32(bytes, 0), &bytes[4..])
}

fn next_u32_size(bytes: &[u8]) -> (usize, &[u8]) {
    (read_u32(bytes, 0) as usize, &bytes[4..])
}

/// Assumes the next 4 payload bytes to specify the size of the chunk. Read and return it, and the rest of the payload.
fn next_sized_chunk(payload: &[u8]) -> Result<(&[u8], &[u8])> {
    let (len, rest) = next_u32_size(payload);
    Ok(rest.split_at(len))
}

/// In the next <N_u32><...N bytes...> validate that N bytes correspond to the expected bytes provided
fn validate_skip_chunk<'a>(payload: &'a [u8], expected_bytes: &[u8]) -> Result<&'a [u8]> {
    let (len, payload) = next_u32_size(payload);
    require!(len == expected_bytes.len(),
             "Unexpected message payload format!");
    let (actual, rest) = payload.split_at(len);
    require!(actual == expected_bytes,
             "Unexpected message payload format!");
    Ok(rest)
}

fn u32_ptr_to_option(int: u32) -> Option<u32> {
    match int {
        0 => None,
        x => Some(x)
    }
}

fn filetime_to_timestamp(ft: u64) -> i64 {
    // TODO: Timezone are maybe off, even though both are UTC?
    // WinApi FILETIME epoch starts 1601-01-01T00:00:00Z, which is 11644473600 seconds before the
    // UNIX/Linux epoch (1970-01-01T00:00:00Z). FILETIME ticks are also in in 100 nanoseconds.
    const TICKS_PER_SECOND: u64 = 10_000_000;
    const SECONSDS_TO_UNIX_EPOCH: i64 = 11_644_473_600;
    let time = ft / TICKS_PER_SECOND;
    time as i64 - SECONSDS_TO_UNIX_EPOCH
}

fn find_first_position<T: PartialEq>(source: &[T], to_find: &[T], step: usize) -> Option<usize> {
    inner_find_positions_of(source, to_find, step, true).first().cloned()
}

/// Efficiently find all indexes of the given sequence occurrence within a longer source sequence.
/// Does not return indexes that overlap matches found earlier.
/// Works in O(n) of the source length, assuming to_find length to be negligible and not accounting for degenerate
/// input cases.
fn inner_find_positions_of<T: PartialEq>(source: &[T], to_find: &[T], step: usize, find_one: bool) -> Vec<usize> {
    assert!(to_find.len() % step == 0, "to_find sequence length is not a multiplier of {step}!");
    if to_find.is_empty() { panic!("to_find slice was empty!"); }
    let max_i = source.len() as i64 - to_find.len() as i64 + 1;
    if max_i <= 0 { return vec![]; }
    let max_i = max_i as usize;
    let mut res = vec![];
    let mut i = 0_usize;
    'outer: while i < max_i {
        for j in 0..to_find.len() {
            if source[i + j] != to_find[j] {
                i += step;
                continue 'outer;
            }
        }
        // Match found
        res.push(i);
        if find_one {
            return res;
        }
        i += to_find.len();
    }
    res
}

fn get_null_terminated_utf16le_slice(bs: &[u8]) -> Result<&[u8]> {
    static NULL_UTF16: &[u8] = &[0x00, 0x00];

    let null_term_idx = 2 * bs.chunks(2)
        .position(|bs| bs == NULL_UTF16)
        .context("Null terminator not found!")?;

    Ok(&bs[..null_term_idx])
}

fn bytes_to_pretty_string(bytes: &[u8], columns: usize) -> String {
    let mut result = String::with_capacity(bytes.len() * 3);
    for row in bytes.chunks(columns) {
        for group in row.chunks(4) {
            for b in group {
                result.push_str(&format!("{b:02x}"));
            }
            result.push(' ');
        }
        result.push('\n');
    }
    result.trim_end().to_owned()
}

/// Handles bold, italic and underline styles, interprets everything else as a plaintext
fn parse_rtf(rtf: &str) -> Result<Vec<RichTextElement>> {
    use rtf_grimoire::tokenizer::Token;

    let tokens = rtf_grimoire::tokenizer::parse_finished(rtf.as_bytes())
        .map_err(|_e| anyhow!("Unable to parse RTF {rtf}"))?;
    if tokens.is_empty() { return Ok(vec![]); }

    // \fcharset0 is cp1252
    require!(tokens.iter().any(|t|
                matches!(t, Token::ControlWord { name, arg: Some(arg) }
                            if name == "ansicpg" || (name == "fcharset" && *arg == 0) )
             ), "RTF is not ANSI-encoded!\nRTF: {rtf}");

    // Text of current styled section
    let mut curr_text: Option<String> = None;

    // Bytes of currently constructed UTF-16 LE string
    let mut unicode_bytes: Vec<u8> = vec![];

    // Returned text is mutable and should be appended.
    // Calling this will flush Unicode string under construction (if any).
    macro_rules! flush_and_get_curr_text {
        () => {{
            let text = curr_text.get_or_insert_with(|| "".to_owned());
            // Flush the existing unicode string, if any
            if !unicode_bytes.is_empty() {
                let string = WStr::from_utf16le(&unicode_bytes)?.to_utf8();
                text.push_str(&string);
                unicode_bytes.clear();
            }
            text
        }};
    }

    // If multiple styles are set, last one set will override the others
    enum Style { Plain, Bold, Italic, Underline }
    let mut style = Style::Plain;

    fn make_rich_text(src: String, style: &Style) -> RichTextElement {
        match style {
            Style::Plain => RichText::make_plain(src),
            Style::Bold => RichText::make_bold(src),
            Style::Italic => RichText::make_italic(src),
            Style::Underline => RichText::make_underline(src),
        }
    }

    let mut result: Vec<RichTextElement> = vec![];

    // Commits current styled section to a result, clearing current text.
    macro_rules! commit_section {
        () => {
            let text = flush_and_get_curr_text!();
            let text = text.trim();
            if !text.is_empty() {
                let text = replace_smiles_with_emojis(text);
                result.push(make_rich_text(text, &style));
            }
            curr_text.take();
        };
    }

    // Unicode control words are followed by a "backup" plaintext char in case client doesn't speak Unicode.
    // We do, so we skip that char.
    let mut skip_next_char = false;

    // We don't care about styling header, so we're skipping it.
    let colortbl = Token::ControlWord { name: "colortbl".to_owned(), arg: None };
    for token in tokens.into_iter().skip_while(|t| *t != colortbl).skip_while(|t| *t != Token::EndGroup) {
        let get_new_state = |arg: Option<i32>, desired: Style| -> Result<Style> {
            match arg {
                None => Ok(desired),
                Some(0) => Ok(Style::Plain),
                Some(_) => err!("Unknown RTF token {token:?}\nRTF: {rtf}")
            }
        };
        match token {
            Token::ControlWord { ref name, arg } if name == "i" => {
                commit_section!();
                style = get_new_state(arg, Style::Italic)?;
            }
            Token::ControlWord { ref name, arg } if name == "b" => {
                commit_section!();
                style = get_new_state(arg, Style::Bold)?;
            }
            Token::ControlWord { ref name, arg } if name == "ul" => {
                commit_section!();
                style = get_new_state(arg, Style::Underline)?;
            }
            Token::ControlWord { ref name, arg } if name == "ulnone" => {
                commit_section!();
                style = get_new_state(arg, Style::Plain)?;
            }
            Token::ControlWord { name, arg: Some(arg) } if name == "'" && arg >= 0 => {
                // Mail.Ru RTF seems to be hardcoded to use cp1251 even if \ansicpg says otherwise
                flush_and_get_curr_text!().push(cp1251_to_utf8_char(arg as u16)?);
            }
            Token::ControlWord { name, arg: Some(arg) } if name == "u" => {
                // As per spec, "Unicode values greater than 32767 must be expressed as negative numbers",
                // but Mail.Ru doesn't seem to care.
                require!(arg >= 0, "Unexpected Unicode value!\nRTF: {rtf}");
                let arg = arg as u16;
                unicode_bytes.extend_from_slice(&arg.to_le_bytes());
                skip_next_char = true;
            }
            Token::Text(t) => {
                let string = String::from_utf8(t)?;
                let mut str = string.as_str();
                if skip_next_char {
                    str = &str[1..];
                    skip_next_char = false;
                }
                if !str.is_empty() {
                    flush_and_get_curr_text!().push_str(str);
                }
            }
            Token::Newline(_) => {
                flush_and_get_curr_text!().push('\n');
            }
            Token::ControlSymbol(c) => {
                flush_and_get_curr_text!().push(c);
            }
            Token::ControlBin(_) =>
                bail!("Unexpected RTF token {token:?} in {rtf}"),
            _ => {}
        }
    }
    commit_section!();
    Ok(result)
}

fn cp1251_to_utf8_char(u: u16) -> Result<char> {
    let bytes = u.to_le_bytes();
    let res = cp1251_to_utf8(&bytes)?;
    let mut chars = res.chars();
    let result = Ok(chars.next().unwrap());
    assert!(chars.next() == Some('\0'));
    result
}

fn cp1251_to_utf8(bytes: &[u8]) -> Result<Cow<str>> {
    use encoding_rs::*;
    let enc = WINDOWS_1251;
    let (res, had_errors) = enc.decode_without_bom_handling(&bytes);
    if !had_errors {
        Ok(res)
    } else {
        err!("Couldn't decode {bytes:02x?}")
    }
}

/// Replaces <SMILE> tags and inline smiles with emojis
fn replace_smiles_with_emojis(s_org: &str) -> String {
    let s = SMILE_TAG_REGEX.replace_all(s_org, |capt: &Captures| {
        if let Some(smiley) = capt.name("alt") {
            let smiley = smiley.as_str();
            let emoji_option = smiley_to_emoji(smiley);
            emoji_option.unwrap_or_else(|| smiley.to_owned())
        } else {
            // Leave as-is
            capt.get(0).unwrap().as_str().to_owned()
        }
    });

    let s = SMILE_INLINE_REGEX.replace_all(&s, |capt: &Captures| {
        let smiley = capt.get(0).unwrap().as_str();
        let emoji_option = smiley_to_emoji(smiley);
        emoji_option.unwrap_or_else(|| smiley.to_owned())
    });

    // SMILE_IMG_REGEX is a third format, but I don't know a replacement for any of them
    //
    // let s = SMILE_IMG_REGEX.replace_all(&s, |capt: &Captures| {
    //     let smiley_id = capt.name("id").unwrap().as_str();
    //     println!("{}", smiley_id);
    //     todo!()
    // });

    s.into()
}

/// Replaces a :Smiley: code with an emoji character if known
fn smiley_to_emoji(smiley: &str) -> Option<String> {
    // This isn't a full list, just the ones I got.
    // There's also a bunch of numeric smileys like :6687: whose meaning isn't known.
    match smiley {
        ":Ок!:" | ":Да!:" => Some("👍"),
        ":Не-а:" | ":Нет!:" => Some("👎"),
        ":Отлично!:" => Some("💯"),
        ":Жжёшь!:" => Some("🔥"),
        ":Радуюсь:" | ":Радость:" | ":Улыбка до ушей:" | ":Улыбка_до_ушей:" | ":Смеюсь:" | "[:-D" => Some("😁"),
        ":Улыбаюсь:" => Some("🙂"),
        ":Лопну от смеха:" => Some("😂"),
        ":Хихикаю:" => Some("🤭"),
        ":Подмигиваю:" => Some("😉"),
        ":Расстраиваюсь:" | ":Подавлен:" => Some("😟"),
        ":Смущаюсь:" => Some("🤭"),
        ":Стыдно:" => Some("🫣"),
        ":Удивляюсь:" | ":Ты что!:" | ":Фига:" | ":Ой, ё:" => Some("😯"),
        ":Сейчас расплачусь:" | ":Извини:" => Some("🥺"),
        ":Хны...!:" => Some("😢"),
        ":Плохо:" | ":В печали:" => Some("😔"),
        ":Рыдаю:" => Some("😭"),
        ":Дразнюсь:" | ":Дурачусь:" | ":Показываю язык:" => Some("😝"),
        ":Виноват:" => Some("😅"),
        ":Сумасшествие:" | ":А я сошла с ума...:" => Some("🤪"),
        ":Целую:" => Some("😘"),
        ":Влюбленный:" | ":Влюблён:" => Some("😍️"),
        ":Поцелуй:" => Some("💋"),
        ":Поцеловали:" => Some("🥰"),
        ":Купидон:" | ":На крыльях любви:" => Some("💘️"),
        ":Сердце:" | ":Люблю:" | ":Любовь:" => Some("❤️"),
        ":Сердце разбито:" => Some("💔️"),
        ":Красотка:" => Some("😊"),
        ":Тошнит:" | ":Гадость:" => Some("🤮"),
        ":Пугаюсь:" => Some("😨"),
        ":Ура!:" | ":Уррра!:" => Some("🎉"),
        ":Кричу:" => Some("📢"),
        ":Подозреваю:" | ":Подозрительно:" => Some("🤨"),
        ":Думаю:" | ":Надо подумать:" => Some("🤔"),
        ":Взрыв мозга:" => Some("🤯"),
        ":Аплодисменты:" => Some("👏"),
        ":Требую:" => Some("🫴"),
        ":Не знаю:" => Some("🤷‍️"),
        ":Ангелок:" | ":Ангелочек:" => Some("😇"),
        ":Чертенок:" | ":Злорадствую:" => Some("😈"),
        ":Пристрелю!:" | ":Застрелю:" | ":Злюсь:" => Some("😡"),
        ":Свирепствую:" => Some("🤬"),
        ":Чертовски злюсь:" => Some("👿"),
        ":Отвали!:" => Some("🖕"),
        ":Побью:" | ":Побили:" | ":В атаку!:" | ":Атакую:" => Some("👊"),
        ":Задолбал!:" => Some("😒"),
        ":Сплю:" => Some("😴"),
        ":Мечтаю:" => Some("😌"),
        ":Прорвемся!:" => Some("💪"),
        ":Пока!:" | ":Пока-пока:" => Some("👋"),
        ":Устал:" | ":В изнеможении:" => Some("😮‍💨"),
        ":Танцую:" => Some("🕺"),
        ":Ктулху:" => Some("🐙"),
        ":Я круче:" => Some("😎"),
        ":Вояка:" => Some("🥷"),
        ":Пиво:" | ":Пивка?;):" => Some("🍺"),
        ":Алкоголик:" => Some("🥴"),
        ":Бойан:" => Some("🪗"),
        ":Лапками-лапками:" => Some("🐾"),
        ":Кондитер:" => Some("👨‍🍳"),
        ":Головой об стену:" => Some("🤕"),
        ":Слушаю музыку:" => Some("🎵"),
        ":Кушаю:" | ":Жую:" => Some("😋"),
        ":Дарю цветочек:" | ":Заяц с цветком:" | ":Не опаздывай:" => Some("🌷"),
        ":Пошалим?:" | ":Хочу тебя:" => Some("😏"),
        ":Ревность:" => Some("😤"),
        ":Внимание!:" => Some("⚠️"),
        ":Помоги:" => Some("🆘"),
        ":Мир!:" => Some("🤝"),
        r#":Левая "коза":"# | r#":Правая "коза":"# => Some("🤘"),
        ":Лучезарно:" => Some("☀️"),
        ":Пацанчик:" => Some("🤠️"),
        ":Карусель:" => Some("🎡"),
        ":Бабочка:" => Some("🦋"),
        ":Голубки:" => Some("🕊"),
        ":Бабло!:" => Some("💸"),
        ":Кот:" | ":Кошки-мышки:" => Some("🐈"),
        ":Пёс:" => Some("🐕"),
        ":Выпей яду:" => Some("☠️"),
        ":Серьёзен как никогда, ага:" => Some("😐️"),
        "[:-|" => Some("🗿"),
        other => {
            // Might also mean this is not a real smiley
            log::warn!("No emoji known for a smiley {other}");
            None
        }
    }.map(|s| s.to_owned())
}
