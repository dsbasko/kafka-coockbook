import Link from 'next/link';
import type { FlatLessonEntry } from '@/lib/course';
import styles from './LessonNav.module.css';

type LessonNavProps = {
  prev: FlatLessonEntry | null;
  next: FlatLessonEntry | null;
};

export function LessonNav({ prev, next }: LessonNavProps) {
  return (
    <nav className={styles.nav} aria-label="Навигация по урокам">
      <div className={styles.row}>
        {prev ? (
          <Link
            href={`/${prev.moduleId}/${prev.lesson.slug}`}
            className={`${styles.link} ${styles.prev}`}
          >
            <span className={styles.label}>← Предыдущий урок</span>
            <span className={styles.title}>{prev.lesson.title}</span>
          </Link>
        ) : (
          <span className={styles.placeholder} aria-hidden="true" />
        )}
        {next ? (
          <Link
            href={`/${next.moduleId}/${next.lesson.slug}`}
            className={`${styles.link} ${styles.next}`}
          >
            <span className={styles.label}>Следующий урок →</span>
            <span className={styles.title}>{next.lesson.title}</span>
          </Link>
        ) : (
          <span className={styles.placeholder} aria-hidden="true" />
        )}
      </div>
      <div className={styles.actions}>
        <button
          type="button"
          className={styles.markButton}
          disabled
          title="Будет доступно после Task 14"
        >
          Отметить пройденным
        </button>
      </div>
    </nav>
  );
}
