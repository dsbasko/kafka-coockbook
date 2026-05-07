import type { Element, ElementContent, Root as HastRoot } from 'hast';

export interface TocEntry {
  depth: 2 | 3;
  slug: string;
  text: string;
}

const HEADING_TAGS: Readonly<Record<string, 2 | 3>> = {
  h2: 2,
  h3: 3,
};

/**
 * Walk a HAST tree and collect h2/h3 headings produced by rehype-slug.
 * Headings without an `id` are skipped (they cannot anchor a TOC link).
 *
 * `rehype-autolink-headings` wraps heading text in an `<a class="heading-anchor">`,
 * so we extract text recursively rather than reading direct children only.
 */
export function extractToc(tree: HastRoot): TocEntry[] {
  const out: TocEntry[] = [];
  walk(tree, (node) => {
    const depth = HEADING_TAGS[node.tagName];
    if (!depth) return;
    const slug = readId(node);
    if (!slug) return;
    const text = collectText(node).trim();
    if (!text) return;
    out.push({ depth, slug, text });
  });
  return out;
}

function readId(node: Element): string | null {
  const id = node.properties?.id;
  return typeof id === 'string' && id.length > 0 ? id : null;
}

function collectText(node: ElementContent | HastRoot): string {
  if (node.type === 'text') return node.value;
  if (node.type === 'element' || node.type === 'root') {
    let acc = '';
    for (const child of node.children) {
      acc += collectText(child as ElementContent);
    }
    return acc;
  }
  return '';
}

function walk(node: HastRoot | Element, visit: (el: Element) => void) {
  for (const child of node.children) {
    if ((child as Element).type === 'element') {
      visit(child as Element);
      walk(child as Element, visit);
    }
  }
}
