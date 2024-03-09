Chat History Manager
====================

Parses, stores and reads chat histories exported from different sources in a dedicated SQLite database.
This includes not just text messages, but also media, stickers, call records and other salvageable data.
Big part of app's functionality is merging chat history snapshots taken on different dates under different settings.

Doesn't (currently) has a UI of its own, exposing a gRPC API instead.
UI written in Scala in available as a [spearate project](https://github.com/frozenspider/chat-history-manager) 

To run the gRPC server (on a hardcoded port 50051), use
```
cargo run --release
```

Supports a bunch of different history formats, refer to sections below for their list and instuction on how to
extract history.
Architecture is extensible, allowing more formats to easily be supported in the future.

Some general notes:
- To parse a standalone 1-on-1 chat, Scala UI needs to be running as it will be asked to identify self user.
- Most of these history formats are reverse engineered, so:
  - Some message types may not be supported as I simply haven't encountered them yet.
  - Compatibility might break unexpectedly as apps sometimes decide to change their storage format.


Telegram
--------
To export chats history, on a Desktop client, go to `Settings -> Advanced -> Export Telegram data`,
choose `Machine-readable JSON` format. 

Then load `result.json` in the app.

One limitation is that **chats containing topics are ignored**.

Note that at least on one occasion, the exported file did not contain `personal_information` section.
This needs to be fixed manually, e.g. by doing another export with no chats included, and copying over
`personal_information` from the new `result.json`.

WhatsApp
--------
Using a rooted Androind phone, download the database through `adb`:
- `adb shell su -c 'cp -r /data/data/com.whatsapp /storage/self/primary/Download/com.whatsapp'`
- `adb pull /storage/self/primary/Download/com.whatsapp`
- Optional cleanup:
  `adb shell su -c 'rm -rf /storage/self/primary/Download/com.whatsapp'`
- If you want media to be resolved, you need to pull it too:
  `adb pull /storage/self/primary/Android/media/com.whatsapp/WhatsApp/Media ./com.whatsapp/Media`
- Load `./databases/msgstore.db` (requires `wa.db` needs to be present in the same directory)

Can also import a WhatsApp exported chat, a text file named `WhatsApp Chat with <name>.txt`.
Note that this format is very limited. 
 
Mail.Ru Agent
-------------
Loads histories from two formats:
- `mra.dbs` used prior to 2014-08-28
- `<account-name>.db` (used after 2014-08-28 and up to 2018, more recent versions were not tested)

In either case, loading `mra.dbs` will load both.

Known issues:
- Only a subset of smile types is supported.
- Some smile types are not converted and left as-is since I don't have a reference to see how they looked like.
- In rare cases, Russian text is double-encoded as cp1251 within UTF-16 LE. Distorted text is passed as-is.
- In legacy database, timestamps are in some weird timezone (looks to be UTC+1?), and actual timezone is not known.
  Use `ShiftDatasetTime` to adjust the time to the correct timezone, if known. 
- Newer database often contains duplicate messages. Best effort is made to get rid of them,
  but the side effect is that it might also remove "legitimate" duplicates (i.e. if a user sent the same message
  multiple times in quick succession on purpose).

Tinder
------
(Required a rooted Androind phone) 

Note that what's stored on the device is just cached messages, so:
- It may contain messages from deleted/unmatched chats.
- If the chat has not been recently viewed in the app, its messages may not be present in the database.

To download the database use `adb`:
- `adb shell su -c 'cp -r /data/data/com.tinder/databases /storage/self/primary/Download/com.tinder'`
- `adb pull /storage/self/primary/Download/com.tinder`
- Optional cleanup: `adb shell su -c 'rm -rf /storage/self/primary/Download/com.tinder'`
- Load `./tinder-3.db`

Will attempt to download GIFs to `./Media/_downloaded` if not already there.

Badoo
-----
(Required a rooted Androind phone)

Note that what's stored on the device is just cached messages, so:
- It may contain messages from deleted/unmatched chats.
- If the chat has not been recently viewed in the app, its messages may not be present in the database.

To download the database use `adb`:
- `adb shell su -c 'cp -r /data/data/com.badoo.mobile/databases /storage/self/primary/Download/com.badoo.mobile'`
- `adb pull /storage/self/primary/Download/com.badoo.mobile`
- Optional cleanup: `adb shell su -c 'rm -rf /storage/self/primary/Download/com.badoo.mobile'`
- Load `./ChatComDatabase`
