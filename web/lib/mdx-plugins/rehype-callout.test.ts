import { describe, expect, it } from 'vitest';
import { unified } from 'unified';
import remarkParse from 'remark-parse';
import remarkRehype from 'remark-rehype';
import { remarkAlert } from 'remark-github-blockquote-alert';
import type { Element, Root as HastRoot } from 'hast';
import rehypeCallout from './rehype-callout';

async function buildTree(markdown: string): Promise<HastRoot> {
  const processor = unified()
    .use(remarkParse)
    .use(remarkAlert)
    .use(remarkRehype, { allowDangerousHtml: false })
    .use(rehypeCallout);
  const mdast = processor.parse(markdown);
  return (await processor.run(mdast)) as HastRoot;
}

function findFirst(tree: HastRoot, predicate: (el: Element) => boolean): Element | null {
  function walk(node: HastRoot | Element): Element | null {
    for (const child of node.children) {
      if ((child as Element).type !== 'element') continue;
      const el = child as Element;
      if (predicate(el)) return el;
      const inner = walk(el);
      if (inner) return inner;
    }
    return null;
  }
  return walk(tree);
}

function findAll(tree: HastRoot, predicate: (el: Element) => boolean): Element[] {
  const out: Element[] = [];
  function walk(node: HastRoot | Element) {
    for (const child of node.children) {
      if ((child as Element).type !== 'element') continue;
      const el = child as Element;
      if (predicate(el)) out.push(el);
      walk(el);
    }
  }
  walk(tree);
  return out;
}

const isCallout = (el: Element) =>
  el.tagName === 'aside' && typeof el.properties?.['data-callout-type'] === 'string';

describe('rehypeCallout', () => {
  it('rewrites NOTE alert into <aside data-callout-type="note">', async () => {
    const tree = await buildTree('> [!NOTE]\n> Хорошая заметка.\n');
    const aside = findFirst(tree, isCallout);
    expect(aside).not.toBeNull();
    expect(aside!.properties?.['data-callout-type']).toBe('note');
    expect(aside!.properties?.className).toBeUndefined();
  });

  it('strips the auto-generated alert title paragraph', async () => {
    const tree = await buildTree('> [!TIP]\n> Полезный совет.\n');
    const aside = findFirst(tree, isCallout);
    expect(aside).not.toBeNull();
    const titleParagraph = aside!.children.find(
      (child) =>
        child.type === 'element' &&
        Array.isArray((child as Element).properties?.className) &&
        ((child as Element).properties!.className as unknown[]).includes('markdown-alert-title'),
    );
    expect(titleParagraph).toBeUndefined();
  });

  it('preserves body content (paragraphs, formatting) inside the callout', async () => {
    const tree = await buildTree(
      '> [!WARNING]\n> Это **важно**.\n>\n> Ещё абзац.\n',
    );
    const aside = findFirst(tree, isCallout);
    expect(aside).not.toBeNull();
    const paragraphs = aside!.children.filter(
      (c) => c.type === 'element' && (c as Element).tagName === 'p',
    ) as Element[];
    expect(paragraphs.length).toBeGreaterThanOrEqual(2);
    const flatten = (el: Element): string =>
      el.children
        .map((c) => {
          if (c.type === 'text') return c.value;
          if (c.type === 'element') return flatten(c as Element);
          return '';
        })
        .join('');
    expect(flatten(paragraphs[0])).toContain('важно');
    expect(flatten(paragraphs[1])).toContain('Ещё абзац');
  });

  it('handles every supported callout type', async () => {
    const md = [
      '> [!NOTE]\n> n',
      '> [!TIP]\n> t',
      '> [!WARNING]\n> w',
      '> [!IMPORTANT]\n> i',
      '> [!CAUTION]\n> c',
    ].join('\n\n');
    const tree = await buildTree(md);
    const callouts = findAll(tree, isCallout);
    expect(callouts.map((el) => el.properties?.['data-callout-type'])).toEqual([
      'note',
      'tip',
      'warning',
      'important',
      'caution',
    ]);
  });

  it('leaves regular blockquotes untouched', async () => {
    const tree = await buildTree('> просто цитата без alert-маркера.\n');
    const aside = findFirst(tree, isCallout);
    expect(aside).toBeNull();
    const blockquote = findFirst(tree, (el) => el.tagName === 'blockquote');
    expect(blockquote).not.toBeNull();
  });

  it('does not throw on a tree with no alerts', async () => {
    const tree = await buildTree('# Heading\n\nText only.\n');
    expect(findFirst(tree, isCallout)).toBeNull();
  });
});
