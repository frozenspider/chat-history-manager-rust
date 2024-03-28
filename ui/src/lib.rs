// Disables the command prompt window that would normally pop up on Windows if run as a bundled app
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use lazy_static::lazy_static;
use std::str::FromStr;
use tauri::{AppHandle, Manager, Runtime};
use tauri::menu::{Menu, MenuId, MenuItem, PredefinedMenuItem, Submenu};
use tauri_plugin_dialog::{DialogExt, MessageDialogKind};
use chat_history_manager_core::message_regular;
use chat_history_manager_core::protobuf::history::*;
use chat_history_manager_core::utils::entity_utils::RichText;

#[tauri::command]
fn open_popup(handle: AppHandle) {
    let file_path = "popup";
    let _settings_window = tauri::WebviewWindowBuilder::new(
        &handle,
        "my-popup", /* the unique window label */
        tauri::WebviewUrl::App(file_path.into()),
    )
        .title("My Popup")
        .build()
        .unwrap();
}

#[tauri::command]
fn get_message() -> tauri::Result<String> {
    let msg = Message {
        internal_id: 123,
        source_id_option: Some(345),
        timestamp: 1234565432,
        from_id: 111,
        text: vec![RichText::make_plain("Hello there!".to_owned())],
        searchable_string: "Search me!".to_string(),
        typed: Some(message_regular! {
            edit_timestamp_option: Some(1234567890),
            is_deleted: false,
            forward_from_name_option: Some("My name!".to_owned()),
            reply_to_message_id_option: Some(4313483375),
            content_option: Some(Content {
                sealed_value_optional: Some(content::SealedValueOptional::File(ContentFile {
                    path_option: Some("my/file/path".to_owned()),
                    file_name_option: Some("my_file_name.txt".to_owned()),
                    mime_type_option: Some("my:mime".to_owned()),
                    thumbnail_path_option: Some("my/thumbnail/path".to_owned()),
                }))
            }),
        }),
    };
    let msg = serde_json::to_string(&msg)?;
    log::debug!("{}", msg);
    Ok(msg)
}

#[tauri::command]
fn report_error_string(handle: AppHandle, error: String) {
    log::error!("UI reported error: {}", error);
    handle.dialog()
        .message(error)
        .title("Error")
        .kind(MessageDialogKind::Error)
        .show(|_res| ()/*Ignore the result*/);
}

lazy_static! {
    static ref MENU_ID_OPEN: MenuId = MenuId::from_str("open").unwrap();
}

pub fn start() {
    fn make_menu<R, M>(handle: &M) -> tauri::Result<Menu<R>>
        where R: Runtime, M: Manager<R>
    {
        // First menu will be a main dropdown menu on macOS
        let file_menu = Submenu::with_items(
            handle, "File", true,
            &[
                &MenuItem::with_id(handle, MENU_ID_OPEN.clone(), "Open", true, None::<&str>)?,
                &PredefinedMenuItem::separator(handle)?,
                &PredefinedMenuItem::quit(handle, None)?,
            ])?;

        Menu::with_items(handle, &[&file_menu])
    }

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
            let handle = app.handle();
            app.set_menu(make_menu(handle)?)?;
            app.on_menu_event(move |app, event| match event.id() {
                x if x == &*MENU_ID_OPEN => {
                    app.dialog()
                        .file()
                        .add_filter("Markdown", &["md"])
                        .pick_file(|path_buf| match path_buf {
                            Some(p) => { println!("Selected {p:?}") }
                            _ => {}
                        });
                }
                _ => {}
            });

            Ok(())
        })
        .menu(|handle| {
            make_menu(handle)
        })
        .invoke_handler(tauri::generate_handler![open_popup, report_error_string, get_message])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
