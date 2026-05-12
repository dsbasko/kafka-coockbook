import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { getLessonContent, getLessonReadmePath } from './lesson';

let fixtureRoot: string;

beforeAll(() => {
  fixtureRoot = mkdtempSync(path.join(tmpdir(), 'kafka-cookbook-lessons-'));
  const ruDir = path.join(
    fixtureRoot,
    '02-producer',
    '02-04-batching',
    'i18n',
    'ru',
  );
  mkdirSync(ruDir, { recursive: true });
  writeFileSync(
    path.join(ruDir, 'README.md'),
    '# Тест\n\nКонтент лекции.\n',
    'utf8',
  );
  // Lesson with both RU and EN translations — exercises the EN happy path.
  const bothDir = path.join(
    fixtureRoot,
    '02-producer',
    '02-05-bilingual',
    'i18n',
  );
  mkdirSync(path.join(bothDir, 'ru'), { recursive: true });
  mkdirSync(path.join(bothDir, 'en'), { recursive: true });
  writeFileSync(path.join(bothDir, 'ru', 'README.md'), '# RU\n', 'utf8');
  writeFileSync(path.join(bothDir, 'en', 'README.md'), '# EN\n', 'utf8');
});

afterAll(() => {
  rmSync(fixtureRoot, { recursive: true, force: true });
});

describe('getLessonContent', () => {
  it('reads RU README.md for an existing lesson', async () => {
    const result = await getLessonContent('02-producer', '02-04-batching', 'ru', {
      lecturesRoot: fixtureRoot,
    });
    expect(result.markdown).toContain('# Тест');
    expect(result.markdown).toContain('Контент лекции.');
    expect(result.lang).toBe('ru');
    expect(result.fallbackUsed).toBe(false);
  });

  it('reads EN README.md when the requested translation exists', async () => {
    const result = await getLessonContent(
      '02-producer',
      '02-05-bilingual',
      'en',
      { lecturesRoot: fixtureRoot },
    );
    expect(result.markdown).toContain('# EN');
    expect(result.lang).toBe('en');
    expect(result.fallbackUsed).toBe(false);
  });

  it('falls back to RU when EN translation is missing', async () => {
    const result = await getLessonContent(
      '02-producer',
      '02-04-batching',
      'en',
      { lecturesRoot: fixtureRoot },
    );
    expect(result.markdown).toContain('# Тест');
    expect(result.lang).toBe('ru');
    expect(result.fallbackUsed).toBe(true);
  });

  it('throws a descriptive error when README is missing', async () => {
    await expect(
      getLessonContent('02-producer', 'no-such', 'ru', { lecturesRoot: fixtureRoot }),
    ).rejects.toThrow(/lesson README not found.*02-producer\/no-such/);
  });

  it('throws a descriptive error when module is missing', async () => {
    await expect(
      getLessonContent('99-missing', '99-01-x', 'ru', { lecturesRoot: fixtureRoot }),
    ).rejects.toThrow(/lesson README not found.*99-missing\/99-01-x/);
  });
});

describe('getLessonReadmePath', () => {
  it('returns the absolute path to the RU README file', () => {
    const p = getLessonReadmePath('02-producer', '02-04-batching', 'ru', {
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

  it('returns the EN path when lang is "en"', () => {
    const p = getLessonReadmePath('02-producer', '02-04-batching', 'en', {
      lecturesRoot: fixtureRoot,
    });
    expect(p).toBe(
      path.join(
        fixtureRoot,
        '02-producer',
        '02-04-batching',
        'i18n',
        'en',
        'README.md',
      ),
    );
  });
});
