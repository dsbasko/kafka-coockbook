import type { Course } from './course';
import { flattenLessons } from './course';

export const PROGRESS_STORAGE_KEY = 'kafka-cookbook-progress';
export const FURTHEST_STORAGE_KEY = 'kafka-cookbook-furthest';
export const PROGRESS_CHANGE_EVENT = 'kafka-cookbook:progress-change';

export type LessonKey = `${string}/${string}`;

export interface ProgressEntry {
  completed: true;
  at: string;
}

export type ProgressMap = Record<LessonKey, ProgressEntry>;

export function lessonKey(moduleId: string, slug: string): LessonKey {
  return `${moduleId}/${slug}` as LessonKey;
}

export function getProgress(): ProgressMap {
  if (typeof window === 'undefined') return {};
  try {
    const raw = window.localStorage.getItem(PROGRESS_STORAGE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    if (!isPlainObject(parsed)) return {};
    const result: ProgressMap = {};
    for (const [key, value] of Object.entries(parsed)) {
      if (isProgressEntry(value)) {
        result[key as LessonKey] = value;
      }
    }
    return result;
  } catch {
    return {};
  }
}

function writeProgress(map: ProgressMap): void {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(PROGRESS_STORAGE_KEY, JSON.stringify(map));
  } catch {
    /* storage may be unavailable (private mode, quota); ignore. */
  }
}

export function markCompleted(key: LessonKey, now: () => string = () => new Date().toISOString()): ProgressMap {
  const current = getProgress();
  current[key] = { completed: true, at: now() };
  writeProgress(current);
  return current;
}

export function unmarkCompleted(key: LessonKey): ProgressMap {
  const current = getProgress();
  delete current[key];
  writeProgress(current);
  return current;
}

export function isCompleted(map: ProgressMap, key: LessonKey): boolean {
  return map[key]?.completed === true;
}

export function getCompletedCount(map: ProgressMap): number {
  return Object.values(map).filter((entry) => entry?.completed === true).length;
}

export function getCompletedPercent(map: ProgressMap, total: number): number {
  if (total <= 0) return 0;
  const ratio = Math.min(getCompletedCount(map), total) / total;
  return Math.round(ratio * 100);
}

/**
 * Furthest-reached lesson — a "sticky" pointer used by the gate to decide which
 * lessons are unlocked. Stored separately from progress so unchecking a lesson
 * does not retract access: once you've gone past a point, that ground stays
 * unlocked. The pointer is a LessonKey rather than a numeric index — the
 * course.yaml ordering can shift across releases, but a stable key survives
 * inserts/removals (and we recompute its index on the fly).
 */
export function getFurthestKey(): LessonKey | null {
  if (typeof window === 'undefined') return null;
  try {
    const raw = window.localStorage.getItem(FURTHEST_STORAGE_KEY);
    if (!raw) return null;
    if (!/^[^/\s]+\/[^/\s]+$/.test(raw)) return null;
    return raw as LessonKey;
  } catch {
    return null;
  }
}

function writeFurthestKey(key: LessonKey | null): void {
  if (typeof window === 'undefined') return;
  try {
    if (key === null) {
      window.localStorage.removeItem(FURTHEST_STORAGE_KEY);
    } else {
      window.localStorage.setItem(FURTHEST_STORAGE_KEY, key);
    }
  } catch {
    /* storage may be unavailable; ignore. */
  }
}

/**
 * Bump the furthest pointer toward `key` if (and only if) `key` sits past the
 * current furthest in linear course order. Unknown keys (not in `course`) are
 * ignored — we don't want a stale lesson key to clobber a valid one.
 */
export function bumpFurthest(course: Course, key: LessonKey): LessonKey | null {
  const flat = flattenLessons(course);
  const newIndex = flat.findIndex((e) => lessonKey(e.moduleId, e.lesson.slug) === key);
  if (newIndex === -1) return getFurthestKey();

  const currentKey = getFurthestKey();
  if (currentKey === null) {
    writeFurthestKey(key);
    return key;
  }
  const currentIndex = flat.findIndex(
    (e) => lessonKey(e.moduleId, e.lesson.slug) === currentKey,
  );
  if (currentIndex === -1 || newIndex > currentIndex) {
    writeFurthestKey(key);
    return key;
  }
  return currentKey;
}

/**
 * Mark a lesson complete and advance the furthest pointer in one atomic step,
 * then notify listeners. This is the single entry point UI code should use
 * when the user explicitly finishes a lesson (e.g. clicking "Next lesson") —
 * combining the writes keeps the gate state internally consistent.
 */
export function markCompletedAndAdvance(course: Course, key: LessonKey): void {
  markCompleted(key);
  bumpFurthest(course, key);
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new Event(PROGRESS_CHANGE_EVENT));
  }
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isProgressEntry(value: unknown): value is ProgressEntry {
  return (
    isPlainObject(value) &&
    value.completed === true &&
    typeof value.at === 'string' &&
    value.at.length > 0
  );
}
