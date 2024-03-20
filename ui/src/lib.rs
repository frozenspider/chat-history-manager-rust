// Disables the command prompt window that would normally pop up on Windows if run as a bundled app
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

/// https://tauri.app/

use lazy_static::lazy_static;
use std::str::FromStr;
use tauri::{AppHandle, Manager, Runtime};
use tauri::menu::{Menu, MenuId, MenuItem, PredefinedMenuItem, Submenu};
use tauri_plugin_dialog::DialogExt;

#[tauri::command]
fn open_popup(handle: AppHandle) {
    let file_path = "popup.html";
    let _settings_window = tauri::WebviewWindowBuilder::new(
        &handle,
        "my-popup", /* the unique window label */
        tauri::WebviewUrl::App(file_path.into()),
    )
        .title("My Popup")
        .build()
        .unwrap();
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
        .invoke_handler(tauri::generate_handler![open_popup])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
