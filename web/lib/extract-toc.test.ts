import { describe, expect, it } from 'vitest';
import { unified } from 'unified';
import remarkParse from 'remark-parse';
import remarkRehype from 'remark-rehype';
import rehypeSlug from 'rehype-slug';
import rehypeAutolinkHeadings from 'rehype-autolink-headings';
import type { Root as HastRoot } from 'hast';
import { extractToc } from './extract-toc';

async function toHast(source: string): Promise<HastRoot> {
  const processor = unified()
    .use(remarkParse)
    .use(remarkRehype)
    .use(rehypeSlug)
    .use(rehypeAutolinkHeadings, {
      behavior: 'wrap',
      properties: { className: ['heading-anchor'] },
    });
  const mdast = processor.parse(source);
  return (await processor.run(mdast)) as HastRoot;
}

describe('extractToc', () => {
  it('collects h2 and h3 in document order', async () => {
    const tree = await toHast(
      [
        '# Заголовок страницы',
        '',
        '## Раздел A',
        '',
        '### Подраздел A1',
        '',
        '## Раздел B',
        '',
        '### Подраздел B1',
        '',
        '### Подраздел B2',
      ].join('\n'),
    );
    expect(extractToc(tree)).toEqual([
      { depth: 2, slug: 'раздел-a', text: 'Раздел A' },
      { depth: 3, slug: 'подраздел-a1', text: 'Подраздел A1' },
      { depth: 2, slug: 'раздел-b', text: 'Раздел B' },
      { depth: 3, slug: 'подраздел-b1', text: 'Подраздел B1' },
      { depth: 3, slug: 'подраздел-b2', text: 'Подраздел B2' },
    ]);
  });

  it('skips h1 and h4+ headings', async () => {
    const tree = await toHast(
      ['# Заголовок', '## Видим', '#### Скрыто', '##### Скрыто'].join('\n'),
    );
    expect(extractToc(tree)).toEqual([
      { depth: 2, slug: 'видим', text: 'Видим' },
    ]);
  });

  it('reads heading text through the autolink-headings <a class="heading-anchor"> wrapper', async () => {
    const tree = await toHast('## Заголовок с `inline code`');
    const toc = extractToc(tree);
    expect(toc).toHaveLength(1);
    expect(toc[0].text).toBe('Заголовок с inline code');
    expect(toc[0].slug).toBe('заголовок-с-inline-code');
  });

  it('uses slugs that match rehype-slug ids on the heading element', async () => {
    const tree = await toHast(['## acks=all и идемпотентность'].join('\n'));
    const toc = extractToc(tree);
    expect(toc).toHaveLength(1);
    // rehype-slug normalizes to lowercase, hyphenated, removes punctuation
    expect(toc[0].slug).toMatch(/^acksall-и-идемпотентность$/);
  });

  it('returns empty array when no h2/h3 are present', async () => {
    const tree = await toHast(['# Только заголовок страницы', '', 'Параграф.'].join('\n'));
    expect(extractToc(tree)).toEqual([]);
  });
});
