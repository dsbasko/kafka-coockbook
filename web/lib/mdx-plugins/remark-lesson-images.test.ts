import { describe, expect, it } from 'vitest';
import remarkLessonImages, {
  rewriteLessonImageUrl,
  type RemarkLessonImagesOptions,
} from './remark-lesson-images';

const OPTIONS: RemarkLessonImagesOptions = {
  moduleId: '02-producer',
  slug: '02-04-batching-and-throughput',
  basePath: '/kafka-cookbook',
};

describe('rewriteLessonImageUrl', () => {
  it('rewrites ./images/ paths with the module/slug prefix', () => {
    const out = rewriteLessonImageUrl('./images/diagram.png', OPTIONS, {
      nodeKind: 'image',
    });
    expect(out).toBe(
      '/kafka-cookbook/static/lectures/02-producer/02-04-batching-and-throughput/images/diagram.png',
    );
  });

  it('rewrites ../../images/ paths (post-i18n-migration) the same way', () => {
    const out = rewriteLessonImageUrl('../../images/diagram.png', OPTIONS, {
      nodeKind: 'image',
    });
    expect(out).toBe(
      '/kafka-cookbook/static/lectures/02-producer/02-04-batching-and-throughput/images/diagram.png',
    );
  });

  it('preserves nested paths under ./images/', () => {
    const out = rewriteLessonImageUrl('./images/sub/dir/file.svg', OPTIONS, {
      nodeKind: 'image',
    });
    expect(out).toBe(
      '/kafka-cookbook/static/lectures/02-producer/02-04-batching-and-throughput/images/sub/dir/file.svg',
    );
  });

  it('preserves nested paths under ../../images/', () => {
    const out = rewriteLessonImageUrl('../../images/sub/dir/file.svg', OPTIONS, {
      nodeKind: 'image',
    });
    expect(out).toBe(
      '/kafka-cookbook/static/lectures/02-producer/02-04-batching-and-throughput/images/sub/dir/file.svg',
    );
  });

  it('strips trailing slash from basePath', () => {
    const out = rewriteLessonImageUrl(
      './images/x.png',
      { ...OPTIONS, basePath: '/kafka-cookbook/' },
      { nodeKind: 'image' },
    );
    expect(out).toBe(
      '/kafka-cookbook/static/lectures/02-producer/02-04-batching-and-throughput/images/x.png',
    );
  });

  it('passes through https://, http://, data:, mailto:, tel:', () => {
    for (const url of [
      'https://example.com/x.png',
      'http://example.com/x.png',
      'data:image/png;base64,iVBORw0KGgo=',
      'mailto:nobody@example.com',
      'tel:+1234567890',
    ]) {
      expect(
        rewriteLessonImageUrl(url, OPTIONS, { nodeKind: 'image' }),
      ).toBe(url);
    }
  });

  it('throws on single parent-relative paths (../...)', () => {
    expect(() =>
      rewriteLessonImageUrl('../shared/x.png', OPTIONS, { nodeKind: 'image' }),
    ).toThrow(/not allowed/);
  });

  it('throws on triple parent-relative paths (../../../...) — extra .. outside prefix', () => {
    expect(() =>
      rewriteLessonImageUrl('../../../images/x.png', OPTIONS, {
        nodeKind: 'image',
      }),
    ).toThrow(/not allowed/);
  });

  it('throws on absolute local paths (/...)', () => {
    expect(() =>
      rewriteLessonImageUrl('/abs/foo.png', OPTIONS, { nodeKind: 'image' }),
    ).toThrow(/not allowed/);
  });

  it('mentions both supported patterns in the not-allowed error', () => {
    expect(() =>
      rewriteLessonImageUrl('weird.png', OPTIONS, { nodeKind: 'image' }),
    ).toThrow(/\.\/images\/.+\.\.\/\.\.\/images\//s);
  });

  it('throws when image file name is missing (./images/ form)', () => {
    expect(() =>
      rewriteLessonImageUrl('./images/', OPTIONS, { nodeKind: 'image' }),
    ).toThrow(/empty image filename/);
  });

  it('throws when image file name is missing (../../images/ form)', () => {
    expect(() =>
      rewriteLessonImageUrl('../../images/', OPTIONS, { nodeKind: 'image' }),
    ).toThrow(/empty image filename/);
  });

  it('throws when path contains .. segment after ./images/', () => {
    expect(() =>
      rewriteLessonImageUrl('./images/../escape.png', OPTIONS, {
        nodeKind: 'image',
      }),
    ).toThrow(/".."/);
  });

  it('throws when path contains .. segment after ../../images/', () => {
    expect(() =>
      rewriteLessonImageUrl('../../images/../escape.png', OPTIONS, {
        nodeKind: 'image',
      }),
    ).toThrow(/".."/);
  });

  it('reports lesson id and node kind in error messages', () => {
    expect(() =>
      rewriteLessonImageUrl('weird.png', OPTIONS, { nodeKind: 'image' }),
    ).toThrow(/02-producer\/02-04-batching-and-throughput/);
  });
});

describe('remarkLessonImages plugin', () => {
  it('rewrites image nodes anywhere in the tree', () => {
    const tree = {
      type: 'root',
      children: [
        {
          type: 'paragraph',
          children: [
            {
              type: 'image',
              url: './images/foo.png',
              alt: 'Foo',
            },
          ],
        },
        {
          type: 'paragraph',
          children: [
            {
              type: 'image',
              url: 'https://kafka.apache.org/logo.png',
              alt: 'External',
            },
          ],
        },
      ],
    };

    const transform = remarkLessonImages(OPTIONS);
    transform(tree as never);

    const firstImage = (
      (tree.children[0] as { children: Array<{ url: string }> }).children[0]
    );
    expect(firstImage.url).toBe(
      '/kafka-cookbook/static/lectures/02-producer/02-04-batching-and-throughput/images/foo.png',
    );

    const secondImage = (
      (tree.children[1] as { children: Array<{ url: string }> }).children[0]
    );
    expect(secondImage.url).toBe('https://kafka.apache.org/logo.png');
  });

  it('rewrites definition nodes (reference-style images)', () => {
    const tree = {
      type: 'root',
      children: [
        {
          type: 'definition',
          identifier: 'fig1',
          url: './images/fig1.png',
        },
        {
          type: 'definition',
          identifier: 'fig2',
          url: '../../images/fig2.png',
        },
      ],
    };

    const transform = remarkLessonImages(OPTIONS);
    transform(tree as never);

    const legacy = tree.children[0] as { url: string };
    const migrated = tree.children[1] as { url: string };
    expect(legacy.url).toBe(
      '/kafka-cookbook/static/lectures/02-producer/02-04-batching-and-throughput/images/fig1.png',
    );
    expect(migrated.url).toBe(
      '/kafka-cookbook/static/lectures/02-producer/02-04-batching-and-throughput/images/fig2.png',
    );
  });

  it('leaves non-image link reference definitions untouched (link plugin owns them)', () => {
    const tree = {
      type: 'root',
      children: [
        {
          type: 'definition',
          identifier: 'next',
          url: '../02-04-batching-and-throughput/README.md',
        },
        {
          type: 'definition',
          identifier: 'gh',
          url: 'https://github.com/dsbasko/kafka-cookbook',
        },
      ],
    };

    const transform = remarkLessonImages(OPTIONS);
    expect(() => transform(tree as never)).not.toThrow();

    const internal = tree.children[0] as { url: string };
    const external = tree.children[1] as { url: string };
    expect(internal.url).toBe('../02-04-batching-and-throughput/README.md');
    expect(external.url).toBe('https://github.com/dsbasko/kafka-cookbook');
  });

  it('throws when an image node has a forbidden url', () => {
    const tree = {
      type: 'root',
      children: [
        {
          type: 'paragraph',
          children: [
            {
              type: 'image',
              url: '../escape/x.png',
            },
          ],
        },
      ],
    };

    const transform = remarkLessonImages(OPTIONS);
    expect(() => transform(tree as never)).toThrow(/not allowed/);
  });

  it('throws when required options are missing', () => {
    expect(() =>
      remarkLessonImages({ moduleId: '', slug: 's', basePath: '/x' } as never),
    ).toThrow(/options/);
    expect(() =>
      remarkLessonImages({ moduleId: 'm', slug: '', basePath: '/x' } as never),
    ).toThrow(/options/);
  });
});
