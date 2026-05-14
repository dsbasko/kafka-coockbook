import type { SVGProps } from 'react';

export function AaIcon(props: SVGProps<SVGSVGElement>) {
  return (
    <svg
      width="24"
      height="24"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
      {...props}
    >
      <text
        x="2"
        y="17"
        fill="currentColor"
        stroke="none"
        fontFamily="'Source Serif 4', Georgia, serif"
        fontSize="13"
        fontWeight="600"
      >
        A
      </text>
      <text
        x="11"
        y="17"
        fill="currentColor"
        stroke="none"
        fontFamily="'Source Serif 4', Georgia, serif"
        fontSize="10"
        fontWeight="600"
      >
        a
      </text>
    </svg>
  );
}
