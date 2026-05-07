import type { ReactElement } from 'react';
import { Fragment, jsx, jsxs } from 'react/jsx-runtime';
import { unified } from 'unified';
import remarkParse from 'remark-parse';
import remarkGfm from 'remark-gfm';
import remarkRehype from 'remark-rehype';
import rehypeSlug from 'rehype-slug';
import rehypeAutolinkHeadings from 'rehype-autolink-headings';
import rehypePrettyCode from 'rehype-pretty-code';
import { toJsxRuntime } from 'hast-util-to-jsx-runtime';
import type { Root as HastRoot } from 'hast';
import remarkLessonImages from './mdx-plugins/remark-lesson-images';

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
    .use(rehypePrettyCode, PRETTY_CODE_OPTIONS);

  const mdast = processor.parse(source);
  const hast = (await processor.run(mdast)) as HastRoot;

  const content = toJsxRuntime(hast, {
    Fragment,
    jsx: jsx as never,
    jsxs: jsxs as never,
  }) as ReactElement;

  return { content };
}
