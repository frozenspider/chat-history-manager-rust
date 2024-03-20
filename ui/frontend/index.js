const tauri_window = window.__TAURI__.window;
const tauri_core = window.__TAURI__.core;

console.log("Hello, JS!");

const popup = new tauri_window.Window('popup-unique-label', {
  url: './ui/popup.html'
});
//
// // since the webview window is created asynchronously,
// // Tauri emits the `tauri://created` and `tauri://error` to notify you of the creation response
// popup.once('tauri://created', () => {
//   // webview window successfully created
//   console.log('Webview window successfully created');
// });
//
// popup.once('tauri://error', (e) => {
//   // an error occurred during webview window creation
//   console.error('Error creating webview window:', e);
// });

const popup_btn_js = document.getElementById('popup-btn-js');
popup_btn_js.onclick = () => {
  // invoke('greet', { name: 'World' })
};


const popup_btn_rust = document.getElementById('popup-btn-rust');
popup_btn_rust.onclick = () => {
  tauri_core.invoke('open_popup');
};
