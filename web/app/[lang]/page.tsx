import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { HomePage } from '@/components/HomePage';
import { loadCourse } from '@/lib/course-loader';
import { getDict } from '@/lib/i18n';
import { isLang, LANGS, type Lang } from '@/lib/lang';
import { buildAssetUrl, buildSiteUrl } from '@/lib/site-url';

type LangPageProps = {
  params: { lang: string };
};

export function generateStaticParams(): Array<{ lang: Lang }> {
  return LANGS.map((lang) => ({ lang }));
}

export function generateMetadata({ params }: LangPageProps): Metadata {
  if (!isLang(params.lang)) return {};
  // Root layout hardcodes DEFAULT_LANG values for canonical/og:url/og:locale/
  // description; without an override the /ru/ home would point crawlers at
  // /en/ as canonical and ship an EN description. Build a per-lang block
  // that matches the actual route.
  const lang = params.lang as Lang;
  const t = getDict(lang);
  const course = loadCourse(lang);
  const description = course.description.replace(/\s+/g, ' ').trim();
  const url = buildSiteUrl(course.basePath, [lang]);
  const ogImage = {
    url: buildAssetUrl(course.basePath, '/opengraph-image'),
    width: 1200,
    height: 630,
    alt: t.ogImageAlt,
  };
  return {
    title: course.title,
    description,
    alternates: { canonical: url },
    openGraph: {
      type: 'website',
      siteName: course.title,
      title: course.title,
      description,
      url,
      locale: lang === 'ru' ? 'ru_RU' : 'en_US',
      images: [ogImage],
    },
    twitter: {
      card: 'summary_large_image',
      title: course.title,
      description,
      images: [ogImage.url],
    },
  };
}

export default function Page({ params }: LangPageProps) {
  if (!isLang(params.lang)) notFound();
  const course = loadCourse(params.lang as Lang);
  // The course YAML doesn't carry a level field today; surface a sensible
  // default until it does, so the stats card has all four cells filled.
  return <HomePage course={course} level="Go" />;
}
