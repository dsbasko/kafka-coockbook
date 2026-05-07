import type { ReactElement } from 'react';
import { Fragment, jsx, jsxs } from 'react/jsx-runtime';
import { unified, type Plugin } from 'unified';
import remarkParse from 'remark-parse';
import remarkGfm from 'remark-gfm';
import remarkRehype from 'remark-rehype';
import rehypeSlug from 'rehype-slug';
import rehypeAutolinkHeadings from 'rehype-autolink-headings';
import rehypePrettyCode from 'rehype-pretty-code';
import { toJsxRuntime } from 'hast-util-to-jsx-runtime';
import type { Element, Root as HastRoot } from 'hast';
import remarkLessonImages from './mdx-plugins/remark-lesson-images';
import { MarkdownFigure } from './markdown-components';

export interface RenderLessonMarkdownOptions {
  moduleId: string;
  slug: string;
  basePath: string;
}

export interface RenderLessonMarkdownResult {
  content: ReactElement;
}

const PRETTY_CODE_OPTIONS = {
  theme: { light: 'github-light', dark: 'night-owl' },
  keepBackground: false,
  defaultLang: 'plaintext',
} as const;

/**
 * rehype-pretty-code stamps `data-language` on the inner <pre>/<code>, but our
 * CodeBlock wrapper renders at the <figure> level. Lift the language up so the
 * figure component can show it without inspecting children.
 */
const rehypeLiftCodeBlockLanguage: Plugin<[], HastRoot> = () => {
  return (tree) => {
    walk(tree, (node) => {
      if (node.tagName !== 'figure') return;
      const props = node.properties;
      if (!props || !('data-rehype-pretty-code-figure' in props)) return;
      for (const child of node.children) {
        if (child.type !== 'element' || child.tagName !== 'pre') continue;
        const lang = (child.properties as Record<string, unknown> | undefined)?.['data-language'];
        if (typeof lang === 'string') {
          (props as Record<string, unknown>)['data-language'] = lang;
        }
        return;
      }
    });
  };
};

function walk(node: HastRoot | Element, visit: (el: Element) => void) {
  for (const child of node.children) {
    if ((child as Element).type === 'element') {
      visit(child as Element);
      walk(child as Element, visit);
    }
  }
}

export async function renderLessonMarkdown(
  source: string,
  options: RenderLessonMarkdownOptions,
): Promise<RenderLessonMarkdownResult> {
  const processor = unified()
    .use(remarkParse)
    .use(remarkGfm)
    .use(remarkLessonImages, {
      moduleId: options.moduleId,
      slug: options.slug,
      basePath: options.basePath,
    })
    .use(remarkRehype, { allowDangerousHtml: false })
    .use(rehypeSlug)
    .use(rehypeAutolinkHeadings, {
      behavior: 'wrap',
      properties: { className: ['heading-anchor'] },
    })
    .use(rehypePrettyCode, PRETTY_CODE_OPTIONS)
    .use(rehypeLiftCodeBlockLanguage);

  const mdast = processor.parse(source);
  const hast = (await processor.run(mdast)) as HastRoot;

  const content = toJsxRuntime(hast, {
    Fragment,
    jsx: jsx as never,
    jsxs: jsxs as never,
    components: { figure: MarkdownFigure as never },
  }) as ReactElement;

  return { content };
}
