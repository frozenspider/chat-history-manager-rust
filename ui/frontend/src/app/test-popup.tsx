'use client'

import { useEffect, useState } from 'react';
import { InvokeTauri } from "@/app/utils";

function OpenPopup() {
  InvokeTauri<void>('open_popup', { name: 'Next.js' })
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
