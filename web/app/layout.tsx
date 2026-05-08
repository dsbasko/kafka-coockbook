import type { Metadata } from 'next';
import { Manrope, JetBrains_Mono } from 'next/font/google';
import { AppShell } from '@/components/AppShell';
import { ThemeProvider } from '@/components/ThemeProvider';
import { loadCourse } from '@/lib/course-loader';
import { THEME_INIT_SCRIPT } from '@/lib/theme';
import '@/styles/globals.css';

const manrope = Manrope({
  subsets: ['latin', 'cyrillic'],
  display: 'swap',
  variable: '--font-ui',
});

const jetbrains = JetBrains_Mono({
  subsets: ['latin', 'cyrillic'],
  display: 'swap',
  variable: '--font-mono',
});

export const metadata: Metadata = {
  title: 'Kafka Cookbook',
  description:
    'Курс по Apache Kafka на Go: продюсеры, консьюмеры, надёжность, контракты, стримы, эксплуатация и use cases.',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  const course = loadCourse();
  return (
    <html
      lang="ru"
      data-theme="light"
      className={`${manrope.variable} ${jetbrains.variable}`}
      suppressHydrationWarning
    >
      <head>
        <script
          id="theme-init"
          // FOUC-free: applies stored/system theme to <html data-theme> before hydration.
          dangerouslySetInnerHTML={{ __html: THEME_INIT_SCRIPT }}
        />
      </head>
      <body>
        <ThemeProvider>
          <AppShell course={course}>{children}</AppShell>
        </ThemeProvider>
      </body>
    </html>
  );
}
