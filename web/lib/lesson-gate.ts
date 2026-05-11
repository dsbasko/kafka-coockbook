import {
  flattenLessons,
  getLessonIndex,
  type Course,
  type FlatLessonEntry,
} from './course';
import { isCompleted, lessonKey, type LessonKey, type ProgressMap } from './progress';

/**
 * Resolve the linear index of the user's furthest reached lesson. Returns
 * the max of (a) the index of the stored pointer when it still maps to a
 * real lesson, and (b) the highest index among completed entries that still
 * exist in the course. Combining both sources protects against a cross-tab
 * `bumpFurthest` race that could persist a lower pointer than the user has
 * actually reached: the still-valid `progress` map heals the regression
 * without waiting for another advance write. Returns -1 only when both
 * sources are empty.
 */
export function resolveFurthestIndex(
  course: Course,
  furthestKey: LessonKey | null,
  progress: ProgressMap,
): number {
  const flat = flattenLessons(course);
  let max = -1;

  if (furthestKey !== null) {
    const idx = flat.findIndex((e) => lessonKey(e.moduleId, e.lesson.slug) === furthestKey);
    if (idx > max) max = idx;
  }

  for (let i = 0; i < flat.length; i += 1) {
    const key = lessonKey(flat[i].moduleId, flat[i].lesson.slug);
    if (isCompleted(progress, key) && i > max) {
      max = i;
    }
  }
  return max;
}

/**
 * A lesson is unlocked when its index is no further than `furthestIndex + 1`.
 * That extra step is what makes "the next lesson after the furthest" reachable
 * — without it, a fresh user (furthestIndex = -1) could not even open the
 * first lesson (index 0).
 */
export function isLessonUnlocked(lessonIndex: number, furthestIndex: number): boolean {
  if (lessonIndex < 0) return false;
  return lessonIndex <= furthestIndex + 1;
}

export function isLessonKeyUnlocked(
  course: Course,
  moduleId: string,
  slug: string,
  furthestKey: LessonKey | null,
  progress: ProgressMap,
): boolean {
  const idx = getLessonIndex(course, moduleId, slug);
  if (idx === -1) return false;
  return isLessonUnlocked(idx, resolveFurthestIndex(course, furthestKey, progress));
}

/**
 * The "current frontier" lesson — the one a fresh CTA should target. Always
 * the first locked-but-reachable lesson (`furthestIndex + 1`), so the user
 * lands on what they've not yet finished even when checkmarks have been
 * toggled. Returns null when the course is empty or the user has finished it.
 */
export function getFrontierLesson(
  course: Course,
  furthestIndex: number,
): FlatLessonEntry | null {
  const flat = flattenLessons(course);
  if (flat.length === 0) return null;
  const targetIndex = Math.min(furthestIndex + 1, flat.length - 1);
  if (targetIndex < 0) return null;
  return flat[targetIndex];
}
