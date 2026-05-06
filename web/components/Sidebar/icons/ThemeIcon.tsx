import type { SVGProps } from 'react';

export function ThemeIcon(props: SVGProps<SVGSVGElement>) {
  return (
    <svg
      width="24"
      height="24"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.75"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
      {...props}
    >
      <circle cx="12" cy="12" r="7.5" />
      <path d="M12 4.5v15a7.5 7.5 0 0 0 0-15Z" fill="currentColor" stroke="none" />
    </svg>
  );
}
