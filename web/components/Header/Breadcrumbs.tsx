import Link from 'next/link';
import type { Lang } from '@/lib/lang';
import styles from './Header.module.css';

type BreadcrumbsProps = {
  lang: Lang;
  moduleId?: string;
  moduleTitle?: string;
  lessonTitle?: string;
};

export function Breadcrumbs({ lang, moduleId, moduleTitle, lessonTitle }: BreadcrumbsProps) {
  if (!moduleId || !moduleTitle) {
    return <span className={styles.breadcrumbRoot}>Kafka Cookbook</span>;
  }

  return (
    <>
      <Link href={`/${lang}`} className={styles.breadcrumbLink}>
        Kafka Cookbook
      </Link>
      <span className={styles.breadcrumbSeparator} aria-hidden="true">
        /
      </span>
      {lessonTitle ? (
        <Link href={`/${lang}/${moduleId}`} className={styles.breadcrumbLink}>
          {moduleTitle}
        </Link>
      ) : (
        <span className={styles.breadcrumbCurrent}>{moduleTitle}</span>
      )}
      {lessonTitle && (
        <>
          <span className={styles.breadcrumbSeparator} aria-hidden="true">
            /
          </span>
          <span className={styles.breadcrumbCurrent}>{lessonTitle}</span>
        </>
      )}
    </>
  );
}
