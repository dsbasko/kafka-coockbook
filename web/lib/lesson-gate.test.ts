import { describe, expect, it } from 'vitest';
import { parseCourse, type Course } from './course';
import {
  getFrontierLesson,
  isLessonKeyUnlocked,
  isLessonUnlocked,
  resolveFurthestIndex,
} from './lesson-gate';
import { lessonKey, type LessonKey, type ProgressMap } from './progress';

const COURSE_YAML = `
title: Test
description: Test
basePath: /test
repoUrl: https://example.com/test
modules:
  - id: 01-foo
    title: Foo
    description: D
    lessons:
      - slug: 01-01-intro
        title: Intro
        duration: 30m
      - slug: 01-02-deep
        title: Deep
        duration: 45m
  - id: 02-bar
    title: Bar
    description: D
    lessons:
      - slug: 02-01-start
        title: Start
        duration: 20m
      - slug: 02-02-end
        title: End
        duration: 20m
`;

function loadCourse(): Course {
  return parseCourse(COURSE_YAML);
}

function completedMap(keys: LessonKey[]): ProgressMap {
  const map: ProgressMap = {};
  for (const key of keys) {
    map[key] = { completed: true, at: '2026-05-12T00:00:00.000Z' };
  }
  return map;
}

describe('isLessonUnlocked', () => {
  it('locks negative indices', () => {
    expect(isLessonUnlocked(-1, 5)).toBe(false);
  });

  it('unlocks index up to and including furthest + 1', () => {
    expect(isLessonUnlocked(0, -1)).toBe(true); // fresh user, first lesson
    expect(isLessonUnlocked(1, -1)).toBe(false);
    expect(isLessonUnlocked(3, 2)).toBe(true);
    expect(isLessonUnlocked(4, 2)).toBe(false);
  });
});

describe('resolveFurthestIndex', () => {
  it('returns -1 when there is no pointer and no progress', () => {
    const course = loadCourse();
    expect(resolveFurthestIndex(course, null, {})).toBe(-1);
  });

  it('returns the index of the stored key when it exists in the course', () => {
    const course = loadCourse();
    expect(resolveFurthestIndex(course, '02-bar/02-01-start' as LessonKey, {})).toBe(2);
  });

  it('falls back to the highest completed index when the pointer is gone', () => {
    const course = loadCourse();
    const progress = completedMap([
      lessonKey('01-foo', '01-01-intro'),
      lessonKey('01-foo', '01-02-deep'),
    ]);
    expect(resolveFurthestIndex(course, null, progress)).toBe(1);
  });

  it('falls back to the highest valid completed index when the stored key is unknown', () => {
    const course = loadCourse();
    const progress = completedMap([lessonKey('02-bar', '02-02-end')]);
    expect(
      resolveFurthestIndex(course, 'ghost/missing' as LessonKey, progress),
    ).toBe(3);
  });

  it('heals a regressed pointer by using the highest completed index', () => {
    const course = loadCourse();
    // Simulates a cross-tab race where bumpFurthest persisted a lower pointer
    // than the user has actually reached. Completed entries should win.
    const progress = completedMap([
      lessonKey('01-foo', '01-01-intro'),
      lessonKey('01-foo', '01-02-deep'),
      lessonKey('02-bar', '02-01-start'),
    ]);
    expect(
      resolveFurthestIndex(course, '01-foo/01-01-intro' as LessonKey, progress),
    ).toBe(2);
  });
});

describe('isLessonKeyUnlocked', () => {
  it('locks unknown lessons', () => {
    const course = loadCourse();
    expect(isLessonKeyUnlocked(course, 'ghost', 'missing', null, {})).toBe(false);
  });

  it('unlocks only the first lesson for a fresh user', () => {
    const course = loadCourse();
    expect(isLessonKeyUnlocked(course, '01-foo', '01-01-intro', null, {})).toBe(true);
    expect(isLessonKeyUnlocked(course, '01-foo', '01-02-deep', null, {})).toBe(false);
    expect(isLessonKeyUnlocked(course, '02-bar', '02-02-end', null, {})).toBe(false);
  });

  it('respects the stored pointer regardless of completed state', () => {
    const course = loadCourse();
    const pointer = '02-bar/02-01-start' as LessonKey;
    expect(isLessonKeyUnlocked(course, '01-foo', '01-01-intro', pointer, {})).toBe(true);
    expect(isLessonKeyUnlocked(course, '02-bar', '02-01-start', pointer, {})).toBe(true);
    expect(isLessonKeyUnlocked(course, '02-bar', '02-02-end', pointer, {})).toBe(true);
  });
});

describe('getFrontierLesson', () => {
  it('returns the first lesson when nothing is unlocked yet', () => {
    const course = loadCourse();
    expect(getFrontierLesson(course, -1)?.lesson.slug).toBe('01-01-intro');
  });

  it('returns the lesson right after the furthest', () => {
    const course = loadCourse();
    expect(getFrontierLesson(course, 1)?.lesson.slug).toBe('02-01-start');
  });

  it('clamps to the last lesson when furthest exceeds the course length', () => {
    const course = loadCourse();
    expect(getFrontierLesson(course, 10)?.lesson.slug).toBe('02-02-end');
  });
});
