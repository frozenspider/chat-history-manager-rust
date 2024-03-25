import { invoke, InvokeArgs } from "@tauri-apps/api/core";

export function ReportError(message: String) {
  InvokeTauri<void>('report_error_string', { error: message })
}

// We're abstracting the invoke function to work around the case when Tauri is not available.
// We don't return the Tauri promise.
export function InvokeTauri<T, R = void>(cmd: string, args?: InvokeArgs, callback?: ((arg: T) => R)) {
  // TODO: Make this a global constant?
  const isTauriAvailable = '__TAURI__' in window
  if (isTauriAvailable) {
    invoke<T>(cmd, args)
      .then(callback)
      .catch(console.error)
  } else {
    const msg = "Calling " + cmd + "(" + JSON.stringify(args) + ") but Tauri is not available"
    console.warn(msg)
    window.alert(msg)
  }
}
