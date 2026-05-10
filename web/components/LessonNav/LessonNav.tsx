import Link from 'next/link';
import styles from './LessonNav.module.css';

export type LessonNavLink = {
  moduleId: string;
  slug: string;
  title: string;
};

type LessonNavProps = {
  prev: LessonNavLink | null;
  next: LessonNavLink | null;
};

export function LessonNav({ prev, next }: LessonNavProps) {
  return (
    <nav className={styles.row} aria-label="Навигация по урокам">
      {prev ? (
        <Link
          href={`/${prev.moduleId}/${prev.slug}`}
          className={`${styles.card} ${styles.prev}`}
        >
          <span className={styles.label}>← Предыдущий урок</span>
          <span className={styles.title}>{prev.title}</span>
        </Link>
      ) : (
        <span className={styles.placeholder} aria-hidden="true" />
      )}
      {next ? (
        <Link
          href={`/${next.moduleId}/${next.slug}`}
          className={`${styles.card} ${styles.next}`}
        >
          <span className={styles.label}>Следующий урок →</span>
          <span className={styles.title}>{next.title}</span>
        </Link>
      ) : (
        <span className={styles.placeholder} aria-hidden="true" />
      )}
    </nav>
  );
}
