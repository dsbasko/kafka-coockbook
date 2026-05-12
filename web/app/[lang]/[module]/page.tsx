import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { ModulePage } from '@/components/ModulePage';
import { loadCourse } from '@/lib/course-loader';
import { getDict } from '@/lib/i18n';
import { isLang, LANGS, type Lang } from '@/lib/lang';
import { buildLangMap } from '@/lib/sitemap';
import { buildAssetUrl, buildSiteUrl } from '@/lib/site-url';

type ModulePageProps = {
  params: { lang: string; module: string };
};

export function generateStaticParams(): Array<{ lang: Lang; module: string }> {
  // Module set is shared across languages (titles differ, ids don't), so
  // re-loading per lang is wasteful — parse once and fan out.
  const course = loadCourse('ru');
  const moduleIds = course.modules.map((m) => m.id);
  return LANGS.flatMap((lang) =>
    moduleIds.map((module) => ({ lang, module })),
  );
}

export function generateMetadata({ params }: ModulePageProps): Metadata {
  if (!isLang(params.lang)) return {};
  const lang = params.lang as Lang;
  const t = getDict(lang);
  const course = loadCourse(lang);
  const mod = course.modules.find((m) => m.id === params.module);
  if (!mod) {
    return { title: t.notFoundMetadataTitle };
  }
  const description = collapseWhitespace(mod.description);
  const title = `${mod.title} · ${course.title}`;
  const url = buildSiteUrl(course.basePath, [lang, mod.id]);
  const ogImage = {
    url: buildAssetUrl(course.basePath, '/opengraph-image'),
    width: 1200,
    height: 630,
    alt: t.ogImageAlt,
  };
  // See note in `[lang]/page.tsx`: `alternates` from the root layout is
  // overridden, not merged, so we have to re-emit the hreflang map.
  const languages = buildLangMap(course.basePath, [mod.id]);
  return {
    title,
    description,
    alternates: { canonical: url, languages },
    openGraph: {
      type: 'article',
      siteName: course.title,
      title,
      description,
      url,
      locale: lang === 'ru' ? 'ru_RU' : 'en_US',
      images: [ogImage],
    },
    twitter: {
      card: 'summary_large_image',
      title,
      description,
      images: [ogImage.url],
    },
  };
}

export default function ModuleRoute({ params }: ModulePageProps) {
  if (!isLang(params.lang)) notFound();
  const course = loadCourse(params.lang as Lang);
  const mod = course.modules.find((m) => m.id === params.module);
  if (!mod) {
    notFound();
  }
  return <ModulePage course={course} module={mod} level="Go" />;
}

function collapseWhitespace(text: string): string {
  return text.replace(/\s+/g, ' ').trim();
}
