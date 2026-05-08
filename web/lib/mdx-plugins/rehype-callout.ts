import type { Plugin } from 'unified';
import type { Element, Root as HastRoot } from 'hast';

export type CalloutType = 'note' | 'tip' | 'warning' | 'important' | 'caution';

const CLASS_PREFIX = 'markdown-alert-';
const TITLE_CLASS = 'markdown-alert-title';

const CALLOUT_TYPES: ReadonlySet<CalloutType> = new Set([
  'note',
  'tip',
  'warning',
  'important',
  'caution',
]);

/**
 * Normalises GitHub-style alert markup produced by `remark-github-blockquote-alert`
 * into our own React-friendly shape: `<aside data-callout-type="note">…</aside>`.
 *
 * The upstream plugin emits `<div class="markdown-alert markdown-alert-note">`
 * with a leading title paragraph (`<p class="markdown-alert-title"><svg/>NOTE</p>`).
 * We strip the auto-generated title and let the React `<Callout>` render its own
 * header (with Russian label and icon), keeping the body content intact.
 */
const rehypeCallout: Plugin<[], HastRoot> = () => {
  return (tree) => {
    walk(tree, (node) => {
      const type = readCalloutType(node);
      if (!type) return;

      node.tagName = 'aside';
      const props = node.properties ?? {};
      delete props.className;
      (props as Record<string, unknown>)['data-callout-type'] = type;
      node.properties = props;

      node.children = node.children.filter((child) => {
        if (child.type !== 'element') return true;
        return !hasClass(child, TITLE_CLASS);
      });
    });
  };
};

export default rehypeCallout;

function readCalloutType(node: Element): CalloutType | null {
  const classes = readClassList(node);
  if (!classes.includes('markdown-alert')) return null;
  for (const cls of classes) {
    if (!cls.startsWith(CLASS_PREFIX)) continue;
    const candidate = cls.slice(CLASS_PREFIX.length);
    if (CALLOUT_TYPES.has(candidate as CalloutType)) {
      return candidate as CalloutType;
    }
  }
  return null;
}

function hasClass(node: Element, target: string): boolean {
  return readClassList(node).includes(target);
}

function readClassList(node: Element): string[] {
  const raw = node.properties?.className;
  if (!raw) return [];
  if (Array.isArray(raw)) return raw.filter((v): v is string => typeof v === 'string');
  if (typeof raw === 'string') return raw.split(/\s+/).filter(Boolean);
  return [];
}

function walk(node: HastRoot | Element, visit: (el: Element) => void) {
  for (const child of node.children) {
    if ((child as Element).type === 'element') {
      visit(child as Element);
      walk(child as Element, visit);
    }
  }
}
