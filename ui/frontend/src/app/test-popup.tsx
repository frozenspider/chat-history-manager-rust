'use client'

import { useEffect, useState } from 'react';
import { invoke } from '@tauri-apps/api/core'

function OpenPopup() {
  invoke<void>('open_popup', { name: 'Next.js' })
    .then(result => console.log(result))
    .catch(console.error)
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
