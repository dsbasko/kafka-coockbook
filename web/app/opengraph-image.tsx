import { ImageResponse } from 'next/og';
import { loadCourse } from '@/lib/course-loader';

export const alt = 'Kafka Cookbook — курс по Apache Kafka на Go';
export const size = { width: 1200, height: 630 };
export const contentType = 'image/png';

export default function OpengraphImage() {
  const course = loadCourse();
  const description = course.description.replace(/\s+/g, ' ').trim();
  const truncated =
    description.length > 180 ? `${description.slice(0, 179).trimEnd()}…` : description;

  return new ImageResponse(
    (
      <div
        style={{
          width: '100%',
          height: '100%',
          display: 'flex',
          flexDirection: 'column',
          justifyContent: 'space-between',
          padding: '72px 80px',
          background: 'linear-gradient(135deg, #faf6ef 0%, #f1ead8 100%)',
          fontFamily: 'sans-serif',
          color: '#1c1a16',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 24 }}>
          <div
            style={{
              width: 96,
              height: 96,
              borderRadius: 48,
              background: '#c14a1f',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              color: '#faf6ef',
              fontSize: 60,
              fontWeight: 700,
              letterSpacing: -2,
            }}
          >
            K
          </div>
          <span
            style={{
              fontSize: 32,
              fontWeight: 500,
              color: '#5b544a',
              letterSpacing: -0.5,
            }}
          >
            Kafka Cookbook
          </span>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 28 }}>
          <div
            style={{
              fontSize: 96,
              fontWeight: 700,
              lineHeight: 1.05,
              letterSpacing: -3,
              color: '#1c1a16',
            }}
          >
            {course.title}
          </div>
          <div
            style={{
              fontSize: 32,
              fontWeight: 400,
              lineHeight: 1.35,
              color: '#5b544a',
              maxWidth: 980,
            }}
          >
            {truncated}
          </div>
        </div>

        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            paddingTop: 32,
            borderTop: '2px solid #e0d4b6',
          }}
        >
          <span style={{ fontSize: 28, color: '#847b6d' }}>9 модулей · 42 урока</span>
          <span style={{ fontSize: 28, color: '#c14a1f', fontWeight: 600 }}>
            Apache Kafka · Go
          </span>
        </div>
      </div>
    ),
    {
      ...size,
    },
  );
}
