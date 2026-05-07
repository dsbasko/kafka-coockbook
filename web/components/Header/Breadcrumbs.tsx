import Link from 'next/link';
import styles from './Header.module.css';

type BreadcrumbsProps = {
  moduleId?: string;
  moduleTitle?: string;
  lessonTitle?: string;
};

export function Breadcrumbs({ moduleId, moduleTitle, lessonTitle }: BreadcrumbsProps) {
  if (!moduleId || !moduleTitle) {
    return <span className={styles.breadcrumbRoot}>Kafka Cookbook</span>;
  }

  return (
    <>
      <Link href="/" className={styles.breadcrumbLink}>
        Kafka Cookbook
      </Link>
      <span className={styles.breadcrumbSeparator} aria-hidden="true">
        /
      </span>
      {lessonTitle ? (
        <Link href={`/${moduleId}`} className={styles.breadcrumbLink}>
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
