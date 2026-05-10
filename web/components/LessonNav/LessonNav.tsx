'use client';

import Link from 'next/link';
import { lessonKey, markCompleted, PROGRESS_CHANGE_EVENT } from '@/lib/progress';
import styles from './LessonNav.module.css';

export type LessonNavLink = {
  moduleId: string;
  slug: string;
  title: string;
};

type LessonNavProps = {
  prev: LessonNavLink | null;
  next: LessonNavLink | null;
  currentModuleId: string;
  currentSlug: string;
};

export function LessonNav({ prev, next, currentModuleId, currentSlug }: LessonNavProps) {
  const handleNextClick = () => {
    markCompleted(lessonKey(currentModuleId, currentSlug));
    window.dispatchEvent(new Event(PROGRESS_CHANGE_EVENT));
  };

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
          onClick={handleNextClick}
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
