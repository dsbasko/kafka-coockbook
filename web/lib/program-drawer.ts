// Cross-component channel for opening the program drawer. The drawer state
// lives in AppShell; descendants dispatch this event to open it.
export const OPEN_PROGRAM_EVENT = 'kafka-cookbook:open-program';

export function openProgramDrawer(): void {
  if (typeof window === 'undefined') return;
  window.dispatchEvent(new CustomEvent(OPEN_PROGRAM_EVENT));
}
