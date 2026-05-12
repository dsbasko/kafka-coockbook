'use client';

import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useGate } from '@/components/GateProvider';
import {
  flattenLessons,
  getLessonIndex,
  getTotalLessons,
} from '@/lib/course';
import { navigateToFrontierHref } from '@/lib/frontier-link';
import { openProgramDrawer } from '@/lib/program-drawer';
import { useLang, useT } from '@/lib/use-i18n';
import styles from './LessonLockedInterstitial.module.css';

type LessonLockedInterstitialProps = {
  attemptedModuleId?: string;
  attemptedSlug?: string;
};

/**
 * Locked-lesson interstitial. Pure pre-hydrated baseline — the dynamic bits
 * (frontier title, current-step count, "steps until" counter, side card
 * progress) are slots filled by the gate-paint inline script before first
 * paint. No useGate-driven JSX, so SSR markup and post-hydration markup are
 * identical and React's hydration phase never re-renders the panel.
 */
export function LessonLockedInterstitial({
  attemptedModuleId,
  attemptedSlug,
}: LessonLockedInterstitialProps) {
  // useGate access is kept only for the static course shape (modules + total
  // lessons); none of these values change on hydration so they don't drive
  // flash. Reading from gate avoids drilling course through props.
  const gate = useGate();
  const { course, basePath } = gate;
  const router = useRouter();
  const t = useT();
  const lang = useLang();

  const attemptedLesson =
    attemptedModuleId && attemptedSlug
      ? course.modules
          .find((m) => m.id === attemptedModuleId)
          ?.lessons.find((l) => l.slug === attemptedSlug)
      : undefined;
  const attemptedModule = attemptedModuleId
    ? course.modules.find((m) => m.id === attemptedModuleId)
    : undefined;
  const attemptedModuleIndex = attemptedModuleId
    ? course.modules.findIndex((m) => m.id === attemptedModuleId)
    : -1;
  const attemptedIndex =
    attemptedModuleId && attemptedSlug
      ? getLessonIndex(course, attemptedModuleId, attemptedSlug)
      : -1;

  const totalLessons = getTotalLessons(course);
  const firstEntry = flattenLessons(course)[0] ?? null;
  // Bare path — Next `<Link>` prepends basePath; pre-baking it here would
  // produce `/kafka-cookbook/kafka-cookbook/...` on client-side navigation.
  const firstHref = firstEntry
    ? `/${lang}/${firstEntry.moduleId}/${firstEntry.lesson.slug}`
    : '#';

  return (
    <section className={styles.page} role="status" aria-live="polite">
      <div className={styles.hero}>
        <div className={styles.heroText}>
          <div className={styles.eyebrow}>
            <span className={styles.eyebrowBadge} aria-hidden="true">
              <SmallLockIcon />
              <span>{t.locked}</span>
            </span>
            {attemptedModuleIndex >= 0 && (
              <>
                <span className={styles.eyebrowDot}>·</span>
                <span>
                  {t.moduleNumberPrefix}{' '}
                  {String(attemptedModuleIndex + 1).padStart(2, '0')}
                </span>
              </>
            )}
          </div>

          <h1 className={styles.title}>{t.lockedTitle}</h1>
          <p className={styles.desc}>{t.lockedDesc}</p>

          {attemptedLesson && (
            <dl className={styles.targetCard} aria-label={t.attemptedLessonLabel}>
              <dt className={styles.targetLabel}>{t.attemptedYouTried}</dt>
              <dd className={styles.targetTitle}>
                {attemptedModule && (
                  <>
                    <span className={styles.targetModule}>
                      {attemptedModule.title}
                    </span>
                    <span className={styles.targetSep}> / </span>
                  </>
                )}
                <span className={styles.targetLesson}>{attemptedLesson.title}</span>
              </dd>
            </dl>
          )}

          <div
            className={styles.ctaRow}
            data-cta-frontier="global"
            data-cta-state="not-started"
            suppressHydrationWarning
          >
            {/* The "Open outline" button is always visible; the
                "Continue" link is the gate-paint-driven variant. */}
            <Link
              href={firstHref}
              className={`${styles.btn} ${styles.btnPrimary}`}
              data-cta-variant="in-progress"
              data-cta-frontier-link
              suppressHydrationWarning
              onClick={(e) => navigateToFrontierHref(e, router, basePath)}
            >
              {t.continueAction} ·{' '}
              <span data-cta-frontier-title suppressHydrationWarning>
                {firstEntry?.lesson.title ?? ''}
              </span>
              <span className={styles.btnArrow}>→</span>
            </Link>
            <Link
              href={firstHref}
              className={`${styles.btn} ${styles.btnPrimary}`}
              data-cta-variant="not-started"
            >
              {t.startFromFirst}
              <span className={styles.btnArrow}>→</span>
            </Link>
            <button
              type="button"
              onClick={openProgramDrawer}
              className={`${styles.btn} ${styles.btnSecondary}`}
            >
              {t.openProgram}
            </button>
          </div>
        </div>

        <aside
          className={styles.sideCard}
          aria-label={t.courseProgress}
          data-progress-scope="global"
          data-progress-state="not-started"
          suppressHydrationWarning
        >
          <div className={styles.sideRow}>
            <span className={styles.sideLabel}>{t.progress}</span>
            <span className={styles.sideVal}>
              <span data-progress-count suppressHydrationWarning>
                0
              </span>{' '}
              / {totalLessons}
            </span>
          </div>
          <div className={styles.sideBar} aria-hidden="true">
            <span
              className={styles.sideFill}
              data-progress-bar
              style={{ width: '0%' }}
              suppressHydrationWarning
            />
          </div>
          <div className={styles.sidePct}>
            <span data-progress-pct suppressHydrationWarning>
              0
            </span>
            %
          </div>

          <div className={styles.sideDivider} />

          <div
            className={styles.sideBlock}
            data-frontier-hint
            data-hint-state="hidden"
            suppressHydrationWarning
          >
            <span className={styles.sideLabel}>{t.nextStep}</span>
            <div className={styles.frontierLine}>
              <span
                className={styles.frontierModule}
                data-frontier-hint-module
                suppressHydrationWarning
              />
            </div>
            <div
              className={styles.frontierLesson}
              data-frontier-hint-lesson
              suppressHydrationWarning
            />
          </div>

          {attemptedIndex >= 0 && (
            <>
              <div className={styles.sideDivider} />
              <div className={styles.sideBlock}>
                <span className={styles.sideLabel}>{t.untilThisLesson}</span>
                <div
                  className={styles.stepsValue}
                  data-steps-until
                  data-steps-target-index={String(attemptedIndex)}
                  data-steps-state="hidden"
                  suppressHydrationWarning
                >
                  0
                </div>
              </div>
            </>
          )}
        </aside>
      </div>
    </section>
  );
}

function SmallLockIcon() {
  return (
    <svg
      width="11"
      height="11"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2.4"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
    >
      <rect x="4" y="11" width="16" height="10" rx="2" />
      <path d="M8 11V7a4 4 0 0 1 8 0v4" />
    </svg>
  );
}
