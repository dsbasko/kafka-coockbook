'use client';

import Link from 'next/link';
import { useGate } from '@/components/GateProvider';
import type { FlatLessonEntry } from '@/lib/course';
import { lessonKey, markCompletedAndAdvance } from '@/lib/progress';
import { useT } from '@/lib/use-i18n';
import styles from './Header.module.css';

type HeaderLessonNavProps = {
  prev: FlatLessonEntry | null;
  next: FlatLessonEntry | null;
  currentModuleId: string;
  currentSlug: string;
};

export function HeaderLessonNav({
  prev,
  next,
  currentModuleId,
  currentSlug,
}: HeaderLessonNavProps) {
  const gate = useGate();
  const t = useT();
  const handleNextClick = () => {
    // Mirror LessonNav: advance the sticky pointer before navigating so the
    // gate considers the next lesson reachable. Without this, clicking the
    // header chevron from a fresh-state lesson lands on the locked interstitial.
    markCompletedAndAdvance(gate.course, lessonKey(currentModuleId, currentSlug));
  };

  return (
    <>
      {prev ? (
        <Link
          href={`/${prev.moduleId}/${prev.lesson.slug}`}
          className={styles.navButton}
          title={`← ${prev.lesson.title}`}
          aria-label={`${t.prevLessonAria}: ${prev.lesson.title}`}
        >
          <ChevronLeft />
        </Link>
      ) : (
        <span
          className={styles.navButtonDisabled}
          aria-hidden="true"
          title={t.firstLessonTitle}
        >
          <ChevronLeft />
        </span>
      )}
      {next ? (
        <Link
          href={`/${next.moduleId}/${next.lesson.slug}`}
          className={styles.navButton}
          title={`${next.lesson.title} →`}
          aria-label={`${t.nextLessonAria}: ${next.lesson.title}`}
          onClick={handleNextClick}
        >
          <ChevronRight />
        </Link>
      ) : (
        <span
          className={styles.navButtonDisabled}
          aria-hidden="true"
          title={t.lastLessonTitle}
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
