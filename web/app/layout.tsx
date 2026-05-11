import type { Metadata } from 'next';
import { Manrope, Source_Serif_4 } from 'next/font/google';
import localFont from 'next/font/local';
import { AppShell } from '@/components/AppShell';
import { GateProvider } from '@/components/GateProvider';
import { ThemeProvider } from '@/components/ThemeProvider';
import { loadCourse } from '@/lib/course-loader';
import { buildGateInitScript } from '@/lib/gate-init-script';
import { buildGateMarkScript } from '@/lib/gate-mark-script';
import { buildSiteUrl, getRuntimeBasePath, getSiteUrl } from '@/lib/site-url';
import { THEME_INIT_SCRIPT } from '@/lib/theme';
import '@/styles/globals.css';

const manrope = Manrope({
  subsets: ['latin', 'cyrillic'],
  display: 'swap',
  variable: '--font-ui',
});

// Self-hosted JetBrains Mono with full glyph coverage (incl. Box Drawing U+2500–U+257F),
// avoids Google Fonts' subset split that breaks ASCII-art alignment.
const jetbrains = localFont({
  variable: '--font-mono',
  display: 'swap',
  src: [
    { path: '../public/fonts/jetbrains-mono/JetBrainsMono-Regular.woff2', weight: '400', style: 'normal' },
    { path: '../public/fonts/jetbrains-mono/JetBrainsMono-Italic.woff2', weight: '400', style: 'italic' },
    { path: '../public/fonts/jetbrains-mono/JetBrainsMono-Medium.woff2', weight: '500', style: 'normal' },
    { path: '../public/fonts/jetbrains-mono/JetBrainsMono-SemiBold.woff2', weight: '600', style: 'normal' },
    { path: '../public/fonts/jetbrains-mono/JetBrainsMono-Bold.woff2', weight: '700', style: 'normal' },
    { path: '../public/fonts/jetbrains-mono/JetBrainsMono-BoldItalic.woff2', weight: '700', style: 'italic' },
  ],
});

const sourceSerif = Source_Serif_4({
  subsets: ['latin', 'cyrillic'],
  display: 'swap',
  variable: '--font-serif',
  weight: ['400', '500', '600'],
});

export function generateMetadata(): Metadata {
  const course = loadCourse();
  const description =
    'Курс по Apache Kafka на Go: продюсеры, консьюмеры, надёжность, контракты, стримы, эксплуатация и use cases.';
  const url = buildSiteUrl(course.basePath);
  return {
    metadataBase: new URL(getSiteUrl()),
    title: course.title,
    description,
    alternates: { canonical: url },
    openGraph: {
      type: 'website',
      siteName: course.title,
      title: course.title,
      description,
      url,
      locale: 'ru_RU',
    },
    twitter: {
      card: 'summary_large_image',
      title: course.title,
      description,
    },
  };
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  const course = loadCourse();
  const basePath = getRuntimeBasePath(course.basePath);
  const gateInitScript = buildGateInitScript(course, basePath);
  const gateMarkScript = buildGateMarkScript(course, basePath);
  return (
    <html
      lang="ru"
      data-theme="light"
      className={`${manrope.variable} ${jetbrains.variable} ${sourceSerif.variable}`}
      suppressHydrationWarning
    >
      <head>
        <script
          id="theme-init"
          // FOUC-free: applies stored/system theme to <html data-theme> before hydration.
          dangerouslySetInnerHTML={{ __html: THEME_INIT_SCRIPT }}
        />
        <script
          id="gate-init"
          // Pre-hydration gate: stamps data-lesson-locked on <html> when the
          // current URL targets a locked lesson, so CSS can hide the content
          // before React mounts and there is no flash of "open" lesson body.
          dangerouslySetInnerHTML={{ __html: gateInitScript }}
        />
      </head>
      <body>
        <ThemeProvider>
          <GateProvider course={course} basePath={basePath}>
            <AppShell course={course}>{children}</AppShell>
          </GateProvider>
        </ThemeProvider>
        {/* Runs as the last body child — by that point every [data-lesson-key]
            element from server-rendered lists is in the DOM, so we can stamp
            data-locked before the browser paints. Stops the flash where rows
            momentarily appear unlocked before React's hydration cycle. */}
        <script
          id="gate-mark"
          dangerouslySetInnerHTML={{ __html: gateMarkScript }}
        />
      </body>
    </html>
  );
}
