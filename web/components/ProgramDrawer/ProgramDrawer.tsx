'use client';

import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import Link from 'next/link';
import { useGate } from '@/components/GateProvider';
import type { Course } from '@/lib/course';
import { formatDurationShort, parseDurationMin } from '@/lib/format';
import { applyGatePainting } from '@/lib/gate-mark-script';
import { isCompleted, lessonKey } from '@/lib/progress';
import { useLang, useT } from '@/lib/use-i18n';
import { LockIcon } from './LockIcon';
import styles from './ProgramDrawer.module.css';

// useLayoutEffect on the client, no-op on the server — gate marking touches
// the DOM and only matters in the browser, but unconditional useLayoutEffect
// would warn during SSR.
const useIsomorphicLayoutEffect =
  typeof window === 'undefined' ? useEffect : useLayoutEffect;

type ProgramDrawerProps = {
  course: Course;
  currentModuleId?: string;
  currentSlug?: string;
  isOpen: boolean;
  onClose: () => void;
};

export function ProgramDrawer({
  course,
  currentModuleId,
  currentSlug,
  isOpen,
  onClose,
}: ProgramDrawerProps) {
  const gate = useGate();
  const t = useT();
  const lang = useLang();
  // Use the shared progress map from GateProvider (single source of truth) so
  // the drawer agrees with checkmark state elsewhere on the page.
  const progress = gate.hydrated ? gate.progress : null;

  // Default expanded set:
  //   • the module containing the active lesson, if any (so the user lands
  //     on their own context),
  //   • otherwise the first two modules — enough to communicate the
  //     accordion shape without the drawer becoming a wall of text.
  const initialExpanded = useMemo(() => {
    const map: Record<string, boolean> = {};
    course.modules.forEach((m, i) => {
      map[m.id] = currentModuleId ? m.id === currentModuleId : i < 2;
    });
    return map;
  }, [course, currentModuleId]);

  const [expanded, setExpanded] = useState<Record<string, boolean>>(initialExpanded);
  const asideRef = useRef<HTMLElement | null>(null);

  // Toggle `inert` on the drawer aside instead of relying on per-element
  // tabindex. The gate-mark script reaches into [data-lesson-key] elements
  // and strips `tabindex` on unlocked items so they regain default focus
  // behavior in HomePage / ModulePage / MDX cross-lesson links — which is
  // correct everywhere except inside a closed offscreen drawer. `inert`
  // removes the entire subtree from the focus order regardless of what
  // individual `tabindex` attributes say, so the two concerns no longer
  // fight each other. useLayoutEffect runs before paint so a freshly
  // closed drawer never leaks focus between render and effect.
  useIsomorphicLayoutEffect(() => {
    const aside = asideRef.current;
    if (!aside) return;
    if (isOpen) {
      aside.removeAttribute('inert');
    } else {
      aside.setAttribute('inert', '');
    }
  }, [isOpen]);

  // Re-seed expansion when the active module changes — opening the drawer
  // from a different lesson should snap to that module.
  useEffect(() => {
    setExpanded(initialExpanded);
  }, [initialExpanded]);

  useEffect(() => {
    if (!isOpen) return;
    function handleKey(event: KeyboardEvent) {
      if (event.key === 'Escape') onClose();
    }
    window.addEventListener('keydown', handleKey);
    return () => window.removeEventListener('keydown', handleKey);
  }, [isOpen, onClose]);

  // Lock the page behind the drawer while it's open (matches the referenced
  // prototype's body.style.overflow = 'hidden' behavior).
  useEffect(() => {
    if (!isOpen) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => {
      document.body.style.overflow = prev;
    };
  }, [isOpen]);

  const toggle = (id: string) =>
    setExpanded((prev) => ({ ...prev, [id]: !prev[id] }));

  // Drawer-rendered lesson rows are added to the DOM after the initial
  // gate-mark inline script ran, so re-apply marking whenever the expanded
  // set or progress changes. useLayoutEffect runs before paint, so locked
  // rows never flash as "open".
  useIsomorphicLayoutEffect(() => {
    if (!gate.hydrated) return;
    applyGatePainting(course, gate.furthestIndex, gate.basePath, lang);
  }, [gate.hydrated, gate.furthestIndex, gate.basePath, course, expanded, isOpen, lang]);

  return (
    <>
      <div
        className={styles.overlay}
        data-open={isOpen ? 'true' : 'false'}
        onClick={onClose}
        aria-hidden="true"
      />
      <aside
        ref={asideRef}
        className={styles.drawer}
        data-open={isOpen ? 'true' : 'false'}
        aria-label={t.programCourse}
        aria-hidden={!isOpen}
        role="dialog"
        aria-modal="true"
        // SSR-side `inert` so the closed drawer is non-focusable before
        // hydration runs the layout effect above. The effect keeps it in
        // sync on subsequent open/close transitions.
        {...((isOpen ? {} : { inert: '' }) as Record<string, string>)}
      >
        <header className={styles.header}>
          <div>
            <div className={styles.eyebrow}>/ contents</div>
            <h2 className={styles.title}>{t.programCourse}</h2>
          </div>
          <button
            type="button"
            className={styles.close}
            onClick={onClose}
            aria-label={t.close}
            tabIndex={isOpen ? 0 : -1}
          >
            <CloseIcon />
          </button>
        </header>

        <nav className={styles.body} aria-label={t.moduleListLabel}>
          <ol className={styles.modules}>
            {course.modules.map((mod, mIndex) => {
              const total = mod.lessons.length;
              const doneCount =
                progress === null
                  ? 0
                  : mod.lessons.filter((l) =>
                      isCompleted(progress, lessonKey(mod.id, l.slug)),
                    ).length;
              const isComplete = doneCount === total && total > 0;
              const isOpenModule = !!expanded[mod.id];
              return (
                <li key={mod.id} className={styles.module}>
                  <button
                    type="button"
                    className={styles.moduleHead}
                    onClick={() => toggle(mod.id)}
                    aria-expanded={isOpenModule}
                    tabIndex={isOpen ? 0 : -1}
                  >
                    <span className={styles.moduleNum}>
                      {String(mIndex + 1).padStart(2, '0')}
                    </span>
                    <span className={styles.moduleTitle}>{mod.title}</span>
                    <span
                      className={styles.moduleBadge}
                      data-complete={isComplete ? 'true' : 'false'}
                    >
                      {doneCount}/{total}
                    </span>
                    <span className={styles.moduleChevron} aria-hidden="true">
                      {isOpenModule ? '−' : '+'}
                    </span>
                  </button>
                  {isOpenModule && (
                    <ol className={styles.lessons}>
                      {mod.lessons.map((lesson, lIndex) => {
                        const key = lessonKey(mod.id, lesson.slug);
                        const done =
                          progress !== null && isCompleted(progress, key);
                        const isCurrent =
                          mod.id === currentModuleId && lesson.slug === currentSlug;
                        const durMin = parseDurationMin(lesson.duration);
                        return (
                          <li key={lesson.slug} className={styles.lesson}>
                            <Link
                              href={`/${lang}/${mod.id}/${lesson.slug}`}
                              className={styles.lessonLink}
                              aria-current={isCurrent ? 'page' : undefined}
                              data-completed={done ? 'true' : undefined}
                              data-current={isCurrent ? 'true' : undefined}
                              data-lesson-key={key}
                              onClick={(e) => {
                                if (
                                  e.currentTarget.getAttribute('data-locked') === 'true'
                                ) {
                                  e.preventDefault();
                                  return;
                                }
                                onClose();
                              }}
                              tabIndex={isOpen ? 0 : -1}
                              title={t.lessonLockTitle}
                              // Why: the gate-mark inline script (runs before
                              // hydration) strips `tabindex` from unlocked
                              // rows so they pick up the default focus order
                              // when the drawer opens. Hydration would
                              // otherwise warn that the SSR attribute (-1)
                              // disagrees with the post-script DOM.
                              suppressHydrationWarning
                            >
                              <span className={styles.lessonNum}>
                                {String(lIndex + 1).padStart(2, '0')}
                              </span>
                              <span className={styles.lessonTitle}>
                                {lesson.title}
                              </span>
                              <span className={styles.lessonMeta} aria-hidden="true">
                                <span className={styles.metaOpen}>
                                  {done ? (
                                    <span className={styles.lessonCheck}>✓</span>
                                  ) : (
                                    formatDurationShort(durMin, lang)
                                  )}
                                </span>
                                <span className={styles.metaLocked}>
                                  <LockIcon />
                                </span>
                              </span>
                            </Link>
                          </li>
                        );
                      })}
                    </ol>
                  )}
                </li>
              );
            })}
          </ol>
        </nav>
      </aside>
    </>
  );
}

function CloseIcon() {
  return (
    <svg
      width="16"
      height="16"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
    >
      <path d="M6 6l12 12M18 6 6 18" />
    </svg>
  );
}
