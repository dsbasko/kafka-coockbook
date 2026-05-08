import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { buildAssetUrl, buildSiteUrl, getSiteUrl } from './site-url';

const ENV_KEY = 'NEXT_PUBLIC_SITE_URL';

describe('site-url', () => {
  let original: string | undefined;

  beforeEach(() => {
    original = process.env[ENV_KEY];
  });

  afterEach(() => {
    if (original === undefined) {
      delete process.env[ENV_KEY];
    } else {
      process.env[ENV_KEY] = original;
    }
  });

  describe('getSiteUrl', () => {
    it('returns default GitHub Pages host when env is unset', () => {
      delete process.env[ENV_KEY];
      expect(getSiteUrl()).toBe('https://dsbasko.github.io');
    });

    it('returns default when env is empty or whitespace', () => {
      process.env[ENV_KEY] = '   ';
      expect(getSiteUrl()).toBe('https://dsbasko.github.io');
    });

    it('strips trailing slashes from env value', () => {
      process.env[ENV_KEY] = 'https://example.com///';
      expect(getSiteUrl()).toBe('https://example.com');
    });

    it('honors custom domain without basePath', () => {
      process.env[ENV_KEY] = 'https://kafka.dsbasko.dev';
      expect(getSiteUrl()).toBe('https://kafka.dsbasko.dev');
    });
  });

  describe('buildSiteUrl', () => {
    it('builds root URL with basePath', () => {
      delete process.env[ENV_KEY];
      expect(buildSiteUrl('/kafka-cookbook')).toBe(
        'https://dsbasko.github.io/kafka-cookbook/',
      );
    });

    it('appends segments and trailing slash', () => {
      delete process.env[ENV_KEY];
      expect(buildSiteUrl('/kafka-cookbook', ['02-producer'])).toBe(
        'https://dsbasko.github.io/kafka-cookbook/02-producer/',
      );
      expect(
        buildSiteUrl('/kafka-cookbook', ['02-producer', '02-03-idempotent-producer']),
      ).toBe(
        'https://dsbasko.github.io/kafka-cookbook/02-producer/02-03-idempotent-producer/',
      );
    });

    it('handles empty basePath (custom domain at root)', () => {
      process.env[ENV_KEY] = 'https://kafka.dsbasko.dev';
      expect(buildSiteUrl('', ['01-foundations'])).toBe(
        'https://kafka.dsbasko.dev/01-foundations/',
      );
      expect(buildSiteUrl('')).toBe('https://kafka.dsbasko.dev/');
    });

    it('skips empty segments', () => {
      delete process.env[ENV_KEY];
      expect(buildSiteUrl('/kafka-cookbook', ['', 'mod', ''])).toBe(
        'https://dsbasko.github.io/kafka-cookbook/mod/',
      );
    });

    it('strips trailing slashes from basePath consistently', () => {
      delete process.env[ENV_KEY];
      expect(buildSiteUrl('/kafka-cookbook/')).toBe(
        'https://dsbasko.github.io/kafka-cookbook/',
      );
    });
  });

  describe('buildAssetUrl', () => {
    it('builds absolute URL for an asset path under basePath', () => {
      delete process.env[ENV_KEY];
      expect(buildAssetUrl('/kafka-cookbook', '/opengraph-image')).toBe(
        'https://dsbasko.github.io/kafka-cookbook/opengraph-image',
      );
    });

    it('accepts asset without leading slash', () => {
      delete process.env[ENV_KEY];
      expect(buildAssetUrl('/kafka-cookbook', 'icon')).toBe(
        'https://dsbasko.github.io/kafka-cookbook/icon',
      );
    });

    it('handles empty basePath (custom domain at root)', () => {
      process.env[ENV_KEY] = 'https://kafka.dsbasko.dev';
      expect(buildAssetUrl('', '/opengraph-image')).toBe(
        'https://kafka.dsbasko.dev/opengraph-image',
      );
    });
  });
});
