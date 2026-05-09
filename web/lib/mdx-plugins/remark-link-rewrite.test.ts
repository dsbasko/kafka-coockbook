import { describe, expect, it } from 'vitest';
import remarkLinkRewrite, {
  rewriteLessonLink,
  type RemarkLinkRewriteOptions,
} from './remark-link-rewrite';
import type { Course } from '../course';

const COURSE: Course = {
  title: 'Kafka Cookbook',
  description: 'desc',
  basePath: '/kafka-cookbook',
  repoUrl: 'https://github.com/dsbasko/kafka-cookbook',
  modules: [
    {
      id: '02-producer',
      title: 'Producer',
      description: 'desc',
      lessons: [
        { slug: '02-03-idempotent-producer', title: 'Idempotent', duration: '30m', tags: [] },
        { slug: '02-04-batching-and-throughput', title: 'Batching', duration: '30m', tags: [] },
      ],
    },
    {
      id: '03-consumer',
      title: 'Consumer',
      description: 'desc',
      lessons: [
        { slug: '03-01-groups-and-rebalance', title: 'Groups', duration: '30m', tags: [] },
        { slug: '03-03-processing-guarantees', title: 'Guarantees', duration: '30m', tags: [] },
      ],
    },
  ],
};

const OPTIONS: RemarkLinkRewriteOptions = {
  moduleId: '02-producer',
  slug: '02-03-idempotent-producer',
  basePath: '/kafka-cookbook',
  course: COURSE,
  lecturesRoot: '/lectures',
};

describe('rewriteLessonLink', () => {
  it('rewrites sibling lesson README.md to site URL with trailing slash', () => {
    const out = rewriteLessonLink(
      '../02-04-batching-and-throughput/README.md',
      OPTIONS,
    );
    expect(out).toEqual({
      url: '/kafka-cookbook/02-producer/02-04-batching-and-throughput/',
      external: false,
    });
  });

  it('rewrites cross-module lesson README.md to site URL', () => {
    const out = rewriteLessonLink(
      '../../03-consumer/03-01-groups-and-rebalance/README.md',
      OPTIONS,
    );
    expect(out).toEqual({
      url: '/kafka-cookbook/03-consumer/03-01-groups-and-rebalance/',
      external: false,
    });
  });

  it('preserves anchor when rewriting README.md links', () => {
    const out = rewriteLessonLink(
      '../../03-consumer/03-03-processing-guarantees/README.md#идемпотентность',
      OPTIONS,
    );
    expect(out.url).toBe(
      '/kafka-cookbook/03-consumer/03-03-processing-guarantees/#идемпотентность',
    );
    expect(out.external).toBe(false);
  });

  it('rewrites links pointing to a lesson directory (trailing slash, no README.md)', () => {
    const out = rewriteLessonLink(
      '../02-04-batching-and-throughput/',
      OPTIONS,
    );
    expect(out).toEqual({
      url: '/kafka-cookbook/02-producer/02-04-batching-and-throughput/',
      external: false,
    });
  });

  it('strips trailing slash from basePath', () => {
    const out = rewriteLessonLink(
      '../02-04-batching-and-throughput/README.md',
      { ...OPTIONS, basePath: '/kafka-cookbook/' },
    );
    expect(out.url).toBe(
      '/kafka-cookbook/02-producer/02-04-batching-and-throughput/',
    );
  });

  it('marks https:// links as external and leaves them unchanged', () => {
    const out = rewriteLessonLink('https://kafka.apache.org/', OPTIONS);
    expect(out).toEqual({ url: 'https://kafka.apache.org/', external: true });
  });

  it('marks http:// links as external too', () => {
    const out = rewriteLessonLink('http://example.com/x', OPTIONS);
    expect(out).toEqual({ url: 'http://example.com/x', external: true });
  });

  it('passes through mailto:, tel:, data: without marking external', () => {
    for (const url of ['mailto:nobody@example.com', 'tel:+123', 'data:text/plain,foo']) {
      expect(rewriteLessonLink(url, OPTIONS)).toEqual({ url, external: false });
    }
  });

  it('passes through pure anchors (#section)', () => {
    expect(rewriteLessonLink('#section', OPTIONS)).toEqual({
      url: '#section',
      external: false,
    });
  });

  it('throws on links to a lesson missing from course.yaml (unknown module)', () => {
    expect(() =>
      rewriteLessonLink('../../99-mystery/lesson/README.md', OPTIONS),
    ).toThrow(/unknown module "99-mystery"/);
  });

  it('throws on links to a lesson missing from course.yaml (unknown slug)', () => {
    expect(() =>
      rewriteLessonLink('../../03-consumer/03-99-not-real/README.md', OPTIONS),
    ).toThrow(/unknown lesson "03-consumer\/03-99-not-real"/);
  });

  it('throws on non-README markdown links', () => {
    expect(() =>
      rewriteLessonLink('../02-04-batching-and-throughput/notes.md', OPTIONS),
    ).toThrow(/Only "<module>\/<slug>\/README\.md"/);
  });

  it('throws when relative path escapes lectures root', () => {
    expect(() =>
      rewriteLessonLink('../../../outside/file.md', OPTIONS),
    ).toThrow(/escapes the lectures root/);
  });

  it('throws on absolute local paths', () => {
    expect(() =>
      rewriteLessonLink('/abs/path', OPTIONS),
    ).toThrow(/is not allowed/);
  });

  it('throws on bare relative paths without "./" or "../" (clear error vs CommonMark misinterpretation)', () => {
    expect(() =>
      rewriteLessonLink('02-04-batching-and-throughput/README.md', OPTIONS),
    ).toThrow(/Only relative links/);
  });

  it('passes through image links unchanged (image plugin owns them)', () => {
    const out = rewriteLessonLink('./images/diagram.png', OPTIONS);
    expect(out).toEqual({ url: './images/diagram.png', external: false });
  });

  it('throws on non-markdown, non-image relative resources', () => {
    expect(() =>
      rewriteLessonLink('../02-04-batching-and-throughput/script.sh', OPTIONS),
    ).toThrow(/non-markdown, non-lesson resource/);
  });

  it('reports lesson id in error messages', () => {
    expect(() =>
      rewriteLessonLink('../../99-mystery/x/README.md', OPTIONS),
    ).toThrow(/02-producer\/02-03-idempotent-producer/);
  });
});

describe('remarkLinkRewrite plugin', () => {
  it('rewrites link nodes anywhere in the tree and tags external links via hProperties', () => {
    const tree = {
      type: 'root',
      children: [
        {
          type: 'paragraph',
          children: [
            {
              type: 'link',
              url: '../02-04-batching-and-throughput/README.md',
              children: [{ type: 'text', value: 'next lesson' }],
            },
          ],
        },
        {
          type: 'paragraph',
          children: [
            {
              type: 'link',
              url: 'https://kafka.apache.org/',
              children: [{ type: 'text', value: 'docs' }],
            },
          ],
        },
        {
          type: 'paragraph',
          children: [
            {
              type: 'link',
              url: '#локально',
              children: [{ type: 'text', value: 'якорь' }],
            },
          ],
        },
      ],
    };

    const transform = remarkLinkRewrite(OPTIONS);
    transform(tree as never);

    const internal = (
      tree.children[0] as { children: Array<{ url: string; data?: unknown }> }
    ).children[0];
    expect(internal.url).toBe(
      '/kafka-cookbook/02-producer/02-04-batching-and-throughput/',
    );
    expect(internal.data).toBeUndefined();

    const external = (
      tree.children[1] as { children: Array<{ url: string; data?: { hProperties?: Record<string, unknown> } }> }
    ).children[0];
    expect(external.url).toBe('https://kafka.apache.org/');
    expect(external.data?.hProperties?.['data-external']).toBe('true');

    const anchor = (
      tree.children[2] as { children: Array<{ url: string; data?: unknown }> }
    ).children[0];
    expect(anchor.url).toBe('#локально');
    expect(anchor.data).toBeUndefined();
  });

  it('rewrites definition nodes (reference-style links)', () => {
    const tree = {
      type: 'root',
      children: [
        {
          type: 'definition',
          identifier: 'next',
          url: '../02-04-batching-and-throughput/README.md',
        },
      ],
    };

    const transform = remarkLinkRewrite(OPTIONS);
    transform(tree as never);

    const def = tree.children[0] as { url: string };
    expect(def.url).toBe(
      '/kafka-cookbook/02-producer/02-04-batching-and-throughput/',
    );
  });

  it('throws when a link points to a missing lesson', () => {
    const tree = {
      type: 'root',
      children: [
        {
          type: 'paragraph',
          children: [
            {
              type: 'link',
              url: '../../99-mystery/x/README.md',
              children: [{ type: 'text', value: 'broken' }],
            },
          ],
        },
      ],
    };

    const transform = remarkLinkRewrite(OPTIONS);
    expect(() => transform(tree as never)).toThrow(/unknown module/);
  });

  it('throws when required options are missing', () => {
    expect(() =>
      remarkLinkRewrite({
        moduleId: '',
        slug: 's',
        basePath: '/x',
        course: COURSE,
      } as never),
    ).toThrow(/required/);
    expect(() =>
      remarkLinkRewrite({
        moduleId: 'm',
        slug: 's',
        basePath: '/x',
      } as never),
    ).toThrow(/required/);
  });
});
