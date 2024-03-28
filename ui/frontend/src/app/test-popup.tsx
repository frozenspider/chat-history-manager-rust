'use client'

import { useEffect, useState } from 'react';
import { InvokeTauri } from "@/app/utils";
import { Message } from "@/protobuf/core/protobuf/entities";

function OpenPopup() {
  let json: any = {
    "internal_id": 123,
    "source_id_option": 345,
    "timestamp": 1234565432,
    "from_id": 111,
    "text": [{"searchable_string": "Hello there!", "plain": {"text": "Hello there!"}}],
    "searchable_string": "Search me!",
    "regular": {
      "edit_timestamp_option": 1234567890,
      "is_deleted": false,
      "forward_from_name_option": "My name!",
      "reply_to_message_id_option": 4313483375,
      "content_option": {
        "file": {
          "path_option": "my/file/path",
          "file_name_option": "my_file_name.txt",
          "mime_type_option": "my:mime",
          "thumbnail_path_option": "my/thumbnail/path"
        }
      }
    }
  }
  let msg = Message.fromJSON(json);
  console.log(msg.fromId)
  console.log(msg)
  InvokeTauri<Message>('get_message', {})
  InvokeTauri<void>('open_popup', {name: 'Next.js'})
}

export default function TestPopup() {
  const [greeting, setGreeting] = useState('');

  // Necessary because we will have to use Greet as a component later.
  return (
    <div>
      <button onClick={OpenPopup}>Open popup</button>
    </div>
  );
}
