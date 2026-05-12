import { ImageResponse } from 'next/og';
import { getTotalLessons } from '@/lib/course';
import { loadCourse } from '@/lib/course-loader';

export const alt = 'Kafka Cookbook — курс по Apache Kafka на Go';
export const size = { width: 1200, height: 630 };
export const contentType = 'image/png';

export function generateStaticParams() {
  return [{ __metadata_id__: [] }];
}

export default function OpengraphImage() {
  const course = loadCourse('ru');
  const description = course.description.replace(/\s+/g, ' ').trim();
  const truncated =
    description.length > 180 ? `${description.slice(0, 179).trimEnd()}…` : description;
  const moduleCount = course.modules.length;
  const lessonCount = getTotalLessons(course);
  const stats = `${moduleCount} ${pluralize(moduleCount, ['модуль', 'модуля', 'модулей'])} · ${lessonCount} ${pluralize(lessonCount, ['урок', 'урока', 'уроков'])}`;

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
          background: 'linear-gradient(135deg, #faf7f2 0%, #f3efe7 100%)',
          fontFamily: 'sans-serif',
          color: '#1a1a1a',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 24 }}>
          <div
            style={{
              width: 96,
              height: 96,
              borderRadius: 48,
              background: '#1a1a1a',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              color: '#faf7f2',
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
              color: '#5b5750',
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
              color: '#1a1a1a',
            }}
          >
            {course.title}
          </div>
          <div
            style={{
              fontSize: 32,
              fontWeight: 400,
              lineHeight: 1.35,
              color: '#5b5750',
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
            borderTop: '2px solid #e6e0d3',
          }}
        >
          <span style={{ fontSize: 28, color: '#8a857b' }}>{stats}</span>
          <span style={{ fontSize: 28, color: '#2a6fdb', fontWeight: 600 }}>
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

function pluralize(n: number, forms: [string, string, string]): string {
  const abs = Math.abs(n) % 100;
  const lastDigit = abs % 10;
  if (abs > 10 && abs < 20) return forms[2];
  if (lastDigit > 1 && lastDigit < 5) return forms[1];
  if (lastDigit === 1) return forms[0];
  return forms[2];
}
