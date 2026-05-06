import { describe, expect, it } from 'vitest';
import { isValidSlug, normalizeSlug } from './slug';

describe('normalizeSlug', () => {
  it('lowercases input', () => {
    expect(normalizeSlug('Hello-World')).toBe('hello-world');
  });

  it('replaces spaces and underscores with dashes', () => {
    expect(normalizeSlug('hello world')).toBe('hello-world');
    expect(normalizeSlug('hello_world')).toBe('hello-world');
    expect(normalizeSlug('hello   world')).toBe('hello-world');
  });

  it('drops disallowed characters', () => {
    expect(normalizeSlug('hello, world!')).toBe('hello-world');
    expect(normalizeSlug('foo/bar')).toBe('foobar');
  });

  it('collapses repeated dashes and trims edges', () => {
    expect(normalizeSlug('--foo---bar--')).toBe('foo-bar');
  });

  it('returns empty string for empty input', () => {
    expect(normalizeSlug('')).toBe('');
    expect(normalizeSlug('   ')).toBe('');
  });
});

describe('isValidSlug', () => {
  it('accepts kebab-case lowercase alphanumeric', () => {
    expect(isValidSlug('foo')).toBe(true);
    expect(isValidSlug('foo-bar')).toBe(true);
    expect(isValidSlug('01-foundations')).toBe(true);
    expect(isValidSlug('01-01-architecture-and-kraft')).toBe(true);
  });

  it('rejects empty, uppercase, spaces, and special characters', () => {
    expect(isValidSlug('')).toBe(false);
    expect(isValidSlug('Foo')).toBe(false);
    expect(isValidSlug('foo bar')).toBe(false);
    expect(isValidSlug('foo_bar')).toBe(false);
    expect(isValidSlug('foo!')).toBe(false);
  });

  it('rejects leading, trailing, and consecutive dashes', () => {
    expect(isValidSlug('-foo')).toBe(false);
    expect(isValidSlug('foo-')).toBe(false);
    expect(isValidSlug('foo--bar')).toBe(false);
  });
});
