'use client'

import { ReportError } from "@/app/utils";

export default function Error(arg: {
  error: Error & { digest?: string }
  reset: () => void
}): string /* Should normally be React.JSX.Element */ {
  ReportError(arg.error.message)
  return "(This UI element been scrapped because of an error)"
}
