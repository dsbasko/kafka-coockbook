import { describe, expect, it } from 'vitest';
import {
  flattenLessons,
  findLesson,
  getNextLesson,
  getPrevLesson,
  parseCourse,
  type Course,
} from './course';

const MIN_VALID_YAML = `
title: Test Course
description: Test description
basePath: /test
repoUrl: https://example.com/test
modules:
  - id: 01-foo
    title: Foo
    description: Foo module description
    lessons:
      - slug: 01-01-intro
        title: Intro
        duration: 30m
        tags: [foo]
      - slug: 01-02-deep
        title: Deep
        duration: 45m
  - id: 02-bar
    title: Bar
    description: Bar module description
    lessons:
      - slug: 02-01-start
        title: Start
        duration: 20m
`;

function loadFixture(): Course {
  return parseCourse(MIN_VALID_YAML);
}

describe('parseCourse — success', () => {
  it('parses a minimal valid yaml', () => {
    const course = loadFixture();
    expect(course.title).toBe('Test Course');
    expect(course.basePath).toBe('/test');
    expect(course.repoUrl).toBe('https://example.com/test');
    expect(course.modules).toHaveLength(2);
    expect(course.modules[0].lessons).toHaveLength(2);
    expect(course.modules[0].lessons[0].tags).toEqual(['foo']);
    expect(course.modules[0].lessons[1].tags).toEqual([]);
  });
});

describe('parseCourse — error cases', () => {
  it('throws when title is missing', () => {
    const yaml = MIN_VALID_YAML.replace('title: Test Course\n', '');
    expect(() => parseCourse(yaml)).toThrow(/title/);
  });

  it('throws when modules is missing', () => {
    const yaml = `
title: T
description: D
basePath: /x
repoUrl: https://example.com
`;
    expect(() => parseCourse(yaml)).toThrow(/modules/);
  });

  it('throws when modules is empty', () => {
    const yaml = `
title: T
description: D
basePath: /x
repoUrl: https://example.com
modules: []
`;
    expect(() => parseCourse(yaml)).toThrow(/modules/);
  });

  it('throws when lesson has no slug', () => {
    const yaml = `
title: T
description: D
basePath: /x
repoUrl: https://example.com
modules:
  - id: 01-foo
    title: Foo
    description: D
    lessons:
      - title: Intro
        duration: 30m
`;
    expect(() => parseCourse(yaml)).toThrow(/slug/);
  });

  it('throws on duplicate lesson slug within a module', () => {
    const yaml = `
title: T
description: D
basePath: /x
repoUrl: https://example.com
modules:
  - id: 01-foo
    title: Foo
    description: D
    lessons:
      - slug: 01-01-intro
        title: Intro
        duration: 30m
      - slug: 01-01-intro
        title: Intro Again
        duration: 30m
`;
    expect(() => parseCourse(yaml)).toThrow(/duplicate lesson slug/);
  });

  it('throws on duplicate module id', () => {
    const yaml = `
title: T
description: D
basePath: /x
repoUrl: https://example.com
modules:
  - id: 01-foo
    title: Foo
    description: D
    lessons:
      - slug: 01-01-intro
        title: Intro
        duration: 30m
  - id: 01-foo
    title: Foo Again
    description: D
    lessons:
      - slug: 01-02-other
        title: Other
        duration: 30m
`;
    expect(() => parseCourse(yaml)).toThrow(/duplicate module id/);
  });

  it('throws on invalid slug', () => {
    const yaml = `
title: T
description: D
basePath: /x
repoUrl: https://example.com
modules:
  - id: 01-foo
    title: Foo
    description: D
    lessons:
      - slug: BadSlug
        title: T
        duration: 30m
`;
    expect(() => parseCourse(yaml)).toThrow(/slug/);
  });

  it('throws on malformed YAML', () => {
    expect(() => parseCourse('title: [unclosed')).toThrow(/YAML/);
  });
});

describe('flattenLessons', () => {
  it('preserves course.yaml order with sequential indices', () => {
    const course = loadFixture();
    const flat = flattenLessons(course);
    expect(flat).toHaveLength(3);
    expect(flat[0]).toMatchObject({ moduleId: '01-foo', index: 0 });
    expect(flat[0].lesson.slug).toBe('01-01-intro');
    expect(flat[1]).toMatchObject({ moduleId: '01-foo', index: 1 });
    expect(flat[1].lesson.slug).toBe('01-02-deep');
    expect(flat[2]).toMatchObject({ moduleId: '02-bar', index: 2 });
    expect(flat[2].lesson.slug).toBe('02-01-start');
  });
});

describe('findLesson', () => {
  it('returns the matching lesson', () => {
    const course = loadFixture();
    expect(findLesson(course, '01-foo', '01-02-deep')?.title).toBe('Deep');
  });

  it('returns null for unknown module', () => {
    const course = loadFixture();
    expect(findLesson(course, 'no-such', '01-01-intro')).toBeNull();
  });

  it('returns null for unknown lesson', () => {
    const course = loadFixture();
    expect(findLesson(course, '01-foo', 'no-such')).toBeNull();
  });
});

describe('getNextLesson / getPrevLesson', () => {
  it('returns next within the same module', () => {
    const course = loadFixture();
    const next = getNextLesson(course, '01-foo', '01-01-intro');
    expect(next?.lesson.slug).toBe('01-02-deep');
  });

  it('crosses module boundaries', () => {
    const course = loadFixture();
    const next = getNextLesson(course, '01-foo', '01-02-deep');
    expect(next?.moduleId).toBe('02-bar');
    expect(next?.lesson.slug).toBe('02-01-start');
  });

  it('returns null after the last lesson (does not wrap)', () => {
    const course = loadFixture();
    expect(getNextLesson(course, '02-bar', '02-01-start')).toBeNull();
  });

  it('returns null before the first lesson (does not wrap)', () => {
    const course = loadFixture();
    expect(getPrevLesson(course, '01-foo', '01-01-intro')).toBeNull();
  });

  it('returns prev across module boundaries', () => {
    const course = loadFixture();
    const prev = getPrevLesson(course, '02-bar', '02-01-start');
    expect(prev?.moduleId).toBe('01-foo');
    expect(prev?.lesson.slug).toBe('01-02-deep');
  });

  it('returns null for unknown lesson', () => {
    const course = loadFixture();
    expect(getNextLesson(course, '01-foo', 'no-such')).toBeNull();
    expect(getPrevLesson(course, '01-foo', 'no-such')).toBeNull();
  });
});
