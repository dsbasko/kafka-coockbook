'use client';

import Link from 'next/link';
import { useGate } from '@/components/GateProvider';
import { lessonKey, markCompletedAndAdvance } from '@/lib/progress';
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
  const gate = useGate();
  const handleNextClick = () => {
    // Single entry point: marks completed, advances the sticky furthest
    // pointer, dispatches the change event. Keeps gate state internally
    // consistent so an in-flight read in another tab doesn't see a half-write.
    markCompletedAndAdvance(gate.course, lessonKey(currentModuleId, currentSlug));
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
