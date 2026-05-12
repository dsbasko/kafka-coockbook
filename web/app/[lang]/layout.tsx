import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { AppShell } from '@/components/AppShell';
import { GateProvider } from '@/components/GateProvider';
import { loadCourse } from '@/lib/course-loader';
import { isLang, LANG_SYNC_SCRIPT, LANGS, type Lang } from '@/lib/lang';
import { getRuntimeBasePath } from '@/lib/site-url';

type LangLayoutProps = {
  children: React.ReactNode;
  params: { lang: string };
};

export function generateStaticParams(): Array<{ lang: Lang }> {
  return LANGS.map((lang) => ({ lang }));
}

export function generateMetadata({ params }: LangLayoutProps): Metadata {
  if (!isLang(params.lang)) return {};
  // The static HTML on disk always carries `<html lang={DEFAULT_LANG}>` because
  // Next renders <html> once at the root layout. `content-language` is the
  // per-lang signal crawlers read instead.
  return { other: { 'content-language': params.lang } };
}

export default function LangLayout({ children, params }: LangLayoutProps) {
  if (!isLang(params.lang)) notFound();
  const lang = params.lang as Lang;
  const course = loadCourse(lang);
  const basePath = getRuntimeBasePath(course.basePath);

  return (
    <GateProvider course={course} basePath={basePath}>
      <AppShell course={course}>{children}</AppShell>
      <script
        // Static HTML always carries `<html lang={DEFAULT_LANG}>`. Sync it to
        // the active route so in-browser APIs (Intl, screen readers) see the
        // correct value the moment this subtree mounts.
        id={`lang-sync-${lang}`}
        dangerouslySetInnerHTML={{ __html: LANG_SYNC_SCRIPT(lang) }}
      />
    </GateProvider>
  );
}
