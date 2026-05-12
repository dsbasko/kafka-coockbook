import { notFound } from 'next/navigation';
import { HomePage } from '@/components/HomePage';
import { loadCourse } from '@/lib/course-loader';
import { isLang, LANGS, type Lang } from '@/lib/lang';

type LangPageProps = {
  params: { lang: string };
};

export function generateStaticParams(): Array<{ lang: Lang }> {
  return LANGS.map((lang) => ({ lang }));
}

export default function Page({ params }: LangPageProps) {
  if (!isLang(params.lang)) notFound();
  const course = loadCourse(params.lang as Lang);
  // The course YAML doesn't carry a level field today; surface a sensible
  // default until it does, so the stats card has all four cells filled.
  return <HomePage course={course} level="Go" />;
}
