import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { LessonAwareLink } from './LessonAwareLink';

(globalThis as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;

let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
});

afterEach(() => {
  act(() => {
    root.unmount();
  });
  container.remove();
});

function renderLink(href: string, basePath = '') {
  act(() => {
    root.render(
      <LessonAwareLink href={href} basePath={basePath}>
        lesson
      </LessonAwareLink>,
    );
  });
  const anchor = container.querySelector<HTMLAnchorElement>('a');
  if (!anchor) throw new Error('anchor not rendered');
  return anchor;
}

describe('LessonAwareLink', () => {
  it('extracts moduleId/slug from a pre-i18n /moduleId/slug URL', () => {
    const a = renderLink('/01-foundations/01-01-architecture-and-kraft');
    expect(a.getAttribute('data-lesson-key')).toBe(
      '01-foundations/01-01-architecture-and-kraft',
    );
  });

  it('strips the /ru/ prefix before extracting moduleId/slug', () => {
    const a = renderLink('/ru/01-foundations/01-01-architecture-and-kraft');
    expect(a.getAttribute('data-lesson-key')).toBe(
      '01-foundations/01-01-architecture-and-kraft',
    );
  });

  it('strips the /en/ prefix before extracting moduleId/slug', () => {
    const a = renderLink('/en/04-reliability/04-03-outbox-pattern');
    expect(a.getAttribute('data-lesson-key')).toBe(
      '04-reliability/04-03-outbox-pattern',
    );
  });

  it('handles basePath plus lang prefix together', () => {
    const a = renderLink(
      '/kafka-cookbook/ru/01-foundations/01-01-architecture-and-kraft',
      '/kafka-cookbook',
    );
    expect(a.getAttribute('data-lesson-key')).toBe(
      '01-foundations/01-01-architecture-and-kraft',
    );
  });

  it('returns no key for hash-only links', () => {
    const a = renderLink('#section');
    expect(a.hasAttribute('data-lesson-key')).toBe(false);
  });

  it('returns no key for external links', () => {
    const a = renderLink('https://example.com/foo/bar');
    expect(a.hasAttribute('data-lesson-key')).toBe(false);
  });

  it('returns no key for a lang-only path with no module/slug', () => {
    const a = renderLink('/ru/');
    expect(a.hasAttribute('data-lesson-key')).toBe(false);
  });

  it('returns no key for the site root', () => {
    const a = renderLink('/');
    expect(a.hasAttribute('data-lesson-key')).toBe(false);
  });

  it('ignores partial-prefix paths like /enfoo', () => {
    const a = renderLink('/enfoo/01-something');
    // /enfoo is not a lang prefix; segments = ['enfoo', '01-something']
    expect(a.getAttribute('data-lesson-key')).toBe('enfoo/01-something');
  });

  it('preserves query and hash when parsing', () => {
    const a = renderLink('/ru/01-foundations/01-01-architecture-and-kraft#intro');
    expect(a.getAttribute('data-lesson-key')).toBe(
      '01-foundations/01-01-architecture-and-kraft',
    );
  });
});
