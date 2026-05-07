import Link from 'next/link';
import type { FlatLessonEntry } from '@/lib/course';
import styles from './Header.module.css';

type HeaderLessonNavProps = {
  prev: FlatLessonEntry | null;
  next: FlatLessonEntry | null;
};

export function HeaderLessonNav({ prev, next }: HeaderLessonNavProps) {
  return (
    <>
      {prev ? (
        <Link
          href={`/${prev.moduleId}/${prev.lesson.slug}`}
          className={styles.navButton}
          title={`← ${prev.lesson.title}`}
          aria-label={`Предыдущий урок: ${prev.lesson.title}`}
        >
          <ChevronLeft />
        </Link>
      ) : (
        <span
          className={styles.navButtonDisabled}
          aria-hidden="true"
          title="Это первый урок"
        >
          <ChevronLeft />
        </span>
      )}
      {next ? (
        <Link
          href={`/${next.moduleId}/${next.lesson.slug}`}
          className={styles.navButton}
          title={`${next.lesson.title} →`}
          aria-label={`Следующий урок: ${next.lesson.title}`}
        >
          <ChevronRight />
        </Link>
      ) : (
        <span
          className={styles.navButtonDisabled}
          aria-hidden="true"
          title="Это последний урок"
        >
          <ChevronRight />
        </span>
      )}
    </>
  );
}

function ChevronLeft() {
  return (
    <svg
      width="20"
      height="20"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.75"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
    >
      <path d="M15 6 9 12l6 6" />
    </svg>
  );
}

function ChevronRight() {
  return (
    <svg
      width="20"
      height="20"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.75"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
    >
      <path d="m9 6 6 6-6 6" />
    </svg>
  );
}
