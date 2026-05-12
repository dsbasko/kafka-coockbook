import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { getLessonContent, getLessonReadmePath } from './lesson';

let fixtureRoot: string;

beforeAll(() => {
  fixtureRoot = mkdtempSync(path.join(tmpdir(), 'kafka-cookbook-lessons-'));
  const lessonDir = path.join(
    fixtureRoot,
    '02-producer',
    '02-04-batching',
    'i18n',
    'ru',
  );
  mkdirSync(lessonDir, { recursive: true });
  writeFileSync(
    path.join(lessonDir, 'README.md'),
    '# Тест\n\nКонтент лекции.\n',
    'utf8',
  );
});

afterAll(() => {
  rmSync(fixtureRoot, { recursive: true, force: true });
});

describe('getLessonContent', () => {
  it('reads README.md for an existing lesson', async () => {
    const result = await getLessonContent('02-producer', '02-04-batching', {
      lecturesRoot: fixtureRoot,
    });
    expect(result.markdown).toContain('# Тест');
    expect(result.markdown).toContain('Контент лекции.');
  });

  it('throws a descriptive error when README is missing', async () => {
    await expect(
      getLessonContent('02-producer', 'no-such', { lecturesRoot: fixtureRoot }),
    ).rejects.toThrow(/lesson README not found.*02-producer\/no-such/);
  });

  it('throws a descriptive error when module is missing', async () => {
    await expect(
      getLessonContent('99-missing', '99-01-x', { lecturesRoot: fixtureRoot }),
    ).rejects.toThrow(/lesson README not found.*99-missing\/99-01-x/);
  });
});

describe('getLessonReadmePath', () => {
  it('returns the absolute path to the README file', () => {
    const p = getLessonReadmePath('02-producer', '02-04-batching', {
      lecturesRoot: fixtureRoot,
    });
    expect(p).toBe(
      path.join(
        fixtureRoot,
        '02-producer',
        '02-04-batching',
        'i18n',
        'ru',
        'README.md',
      ),
    );
  });
});
