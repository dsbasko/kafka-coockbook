import { TOTAL_LESSONS } from './course';

export const PROGRESS_STORAGE_KEY = 'kafka-cookbook-progress';

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

export function getCompletedPercent(map: ProgressMap): number {
  if (TOTAL_LESSONS <= 0) return 0;
  const ratio = getCompletedCount(map) / TOTAL_LESSONS;
  return Math.round(ratio * 100);
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
