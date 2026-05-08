import { ImageResponse } from 'next/og';

export const size = { width: 32, height: 32 };
export const contentType = 'image/png';

export default function Icon() {
  return new ImageResponse(
    (
      <div
        style={{
          width: '100%',
          height: '100%',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          background: '#c14a1f',
          color: '#faf6ef',
          fontSize: 22,
          fontWeight: 700,
          fontFamily: 'sans-serif',
          letterSpacing: -1,
          borderRadius: 6,
        }}
      >
        K
      </div>
    ),
    {
      ...size,
    },
  );
}
