'use client';

import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from 'react';
import { usePathname } from 'next/navigation';
import type { Course, FlatLessonEntry } from '@/lib/course';
import { GATE_LOCKED_ATTR } from '@/lib/gate-init-script';
import { applyGatePainting } from '@/lib/gate-mark-script';
import { DEFAULT_LANG, stripLangFromPath } from '@/lib/lang';
import {
  getFrontierLesson,
  isLessonKeyUnlocked,
  resolveFurthestIndex,
} from '@/lib/lesson-gate';
import {
  FURTHEST_STORAGE_KEY,
  getFurthestKey,
  getProgress,
  PROGRESS_CHANGE_EVENT,
  PROGRESS_STORAGE_KEY,
  type LessonKey,
  type ProgressMap,
} from '@/lib/progress';

export type GateContextValue = {
  course: Course;
  basePath: string;
  hydrated: boolean;
  progress: ProgressMap;
  furthestKey: LessonKey | null;
  furthestIndex: number;
  isLessonUnlocked(moduleId: string, slug: string): boolean;
  getFrontier(): FlatLessonEntry | null;
};

export const GateContext = createContext<GateContextValue | null>(null);

type GateProviderProps = {
  course: Course;
  basePath: string;
  children: ReactNode;
};

export function GateProvider({ course, basePath, children }: GateProviderProps) {
  const [hydrated, setHydrated] = useState(false);
  const [progress, setProgress] = useState<ProgressMap>({});
  const [furthestKey, setFurthestKeyState] = useState<LessonKey | null>(null);
  const pathname = usePathname();

  useEffect(() => {
    setProgress(getProgress());
    setFurthestKeyState(getFurthestKey());
    setHydrated(true);

    function refresh() {
      setProgress(getProgress());
      setFurthestKeyState(getFurthestKey());
    }
    function syncStorage(e: StorageEvent) {
      if (e.key !== PROGRESS_STORAGE_KEY && e.key !== FURTHEST_STORAGE_KEY) return;
      refresh();
    }
    window.addEventListener(PROGRESS_CHANGE_EVENT, refresh);
    window.addEventListener('storage', syncStorage);
    return () => {
      window.removeEventListener(PROGRESS_CHANGE_EVENT, refresh);
      window.removeEventListener('storage', syncStorage);
    };
  }, []);

  // Keep gate markers in sync with current state. Two attributes are
  // managed here from a single source of truth (resolveFurthestIndex):
  //   • `data-lesson-locked` on <html> — controls the lesson page gate.
  //     The inline init script handles the very first paint on direct
  //     navigation; this effect covers SPA route changes and cross-tab
  //     localStorage updates.
  //   • `data-locked` on every `[data-lesson-key]` element — controls
  //     locked styling in lists (HomePage modules, ModulePage lessons,
  //     ProgramDrawer, MDX cross-lesson links). The inline gate-mark
  //     script sets these before first paint; this effect re-applies them
  //     after React updates the DOM (e.g. drawer opening, route change
  //     introducing new rows, cross-tab progress sync).
  useEffect(() => {
    if (!hydrated) return;
    if (typeof document === 'undefined') return;
    const root = document.documentElement;
    const furthestIndex = resolveFurthestIndex(course, furthestKey, progress);

    const { lang: parsedLang, rest } = stripLangFromPath(pathname ?? '/');
    const segments = rest
      .replace(/^\/+|\/+$/g, '')
      .split('/')
      .filter(Boolean);
    if (segments.length < 2) {
      root.removeAttribute(GATE_LOCKED_ATTR);
    } else {
      const [moduleId, slug] = segments;
      const locked = !isLessonKeyUnlocked(course, moduleId, slug, furthestKey, progress);
      if (locked) {
        root.setAttribute(GATE_LOCKED_ATTR, 'true');
      } else {
        root.removeAttribute(GATE_LOCKED_ATTR);
      }
    }

    applyGatePainting(course, furthestIndex, basePath, parsedLang ?? DEFAULT_LANG);
  }, [hydrated, pathname, course, basePath, furthestKey, progress]);

  const value = useMemo<GateContextValue>(() => {
    const furthestIndex = resolveFurthestIndex(course, furthestKey, progress);
    return {
      course,
      basePath,
      hydrated,
      progress,
      furthestKey,
      furthestIndex,
      isLessonUnlocked(moduleId, slug) {
        // Pre-hydration we cannot know what the user has unlocked — render
        // everything as reachable so SSR matches and the user never sees a
        // flash of locked items that immediately unlock on hydration.
        if (!hydrated) return true;
        return isLessonKeyUnlocked(course, moduleId, slug, furthestKey, progress);
      },
      getFrontier() {
        return getFrontierLesson(course, furthestIndex);
      },
    };
  }, [course, basePath, hydrated, progress, furthestKey]);

  return <GateContext.Provider value={value}>{children}</GateContext.Provider>;
}

export function useGate(): GateContextValue {
  const value = useContext(GateContext);
  if (!value) {
    throw new Error('useGate must be used within <GateProvider>');
  }
  return value;
}
