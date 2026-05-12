import path from 'node:path';
import type { Course } from '../course';
import { DEFAULT_LANG, isLang, type Lang } from '../lang';

export interface RemarkLinkRewriteOptions {
  moduleId: string;
  slug: string;
  basePath: string;
  course: Course;
  /**
   * Active route language. Used both to pick the source `i18n/<lang>/README.md`
   * file when re-relativizing markdown sibling links AND to prefix the emitted
   * site URL with `/<lang>/`. Defaults to {@link DEFAULT_LANG}.
   */
  lang?: Lang;
  /**
   * Absolute path to the lectures root (the directory that contains
   * `<moduleId>/<slug>/README.md`). Optional — defaults to the path that
   * `course-loader` uses (`<repo-root>/lectures`). Tests pass a synthetic root.
   */
  lecturesRoot?: string;
}

interface LinkNode {
  type: 'link' | 'linkReference' | 'definition';
  url?: string;
  data?: {
    hProperties?: Record<string, unknown>;
  } & Record<string, unknown>;
  children?: AstNode[];
}

interface ParentNode {
  type: string;
  children?: AstNode[];
}

type AstNode = LinkNode | ParentNode;

const EXTERNAL_PROTOCOL_RE = /^[a-z][a-z0-9+.-]*:\/\//i;
const PASS_THROUGH_PROTOCOL_RE = /^(?:mailto:|tel:|data:)/i;
const IMAGE_EXT_RE = /\.(?:png|jpe?g|gif|svg|webp|avif|ico)(?:[?#]|$)/i;

/**
 * Pure rewrite logic, exported for unit tests. Returns the new url plus a flag
 * indicating whether the link should be marked external in HAST.
 */
export function rewriteLessonLink(
  rawUrl: string,
  options: RemarkLinkRewriteOptions,
): { url: string; external: boolean } {
  const url = rawUrl.trim();

  if (url.length === 0) {
    return { url: rawUrl, external: false };
  }

  if (url.startsWith('#')) {
    return { url, external: false };
  }

  if (EXTERNAL_PROTOCOL_RE.test(url)) {
    return { url, external: true };
  }

  if (PASS_THROUGH_PROTOCOL_RE.test(url)) {
    return { url, external: false };
  }

  if (!url.startsWith('./') && !url.startsWith('../')) {
    throw new Error(
      `remark-link-rewrite: link "${rawUrl}" in ${options.moduleId}/${options.slug} ` +
        `is not allowed. Only relative links ("./", "../"), absolute URLs ` +
        `(http(s)://, mailto:, tel:, data:) and "#anchor" are supported.`,
    );
  }

  const [pathPart, hashPart = ''] = splitHash(url);
  const lecturesRoot = options.lecturesRoot ?? '/lectures';
  const lang = options.lang && isLang(options.lang) ? options.lang : DEFAULT_LANG;
  const lessonDir = path.posix.join(
    lecturesRoot,
    options.moduleId,
    options.slug,
    'i18n',
    lang,
  );
  const resolved = path.posix.normalize(path.posix.join(lessonDir, pathPart));

  if (!isInside(resolved, lecturesRoot)) {
    throw new Error(
      `remark-link-rewrite: link "${rawUrl}" in ${options.moduleId}/${options.slug} ` +
        `escapes the lectures root (resolved to "${resolved}")`,
    );
  }

  const relative = resolved.slice(lecturesRoot.length).replace(/^\/+/, '');
  const segments = relative.split('/').filter(Boolean);

  // Image / static asset under another lesson — leave as-is. The image plugin
  // handles its own `image` nodes; <a href="…image">…</a> is a manual link and
  // we don't want to break it.
  if (IMAGE_EXT_RE.test(resolved)) {
    return { url: rawUrl, external: false };
  }

  const isMarkdown = resolved.endsWith('.md');
  const looksLikeLessonDir =
    segments.length === 2 && pathPart.endsWith('/');
  const looksLikeLessonReadme =
    segments.length === 5 &&
    segments[2] === 'i18n' &&
    isLang(segments[3]) &&
    segments[4] === 'README.md';

  if (!isMarkdown && !looksLikeLessonDir) {
    // Non-markdown file (e.g. "../foo.txt") — outside the supported subset.
    throw new Error(
      `remark-link-rewrite: link "${rawUrl}" in ${options.moduleId}/${options.slug} ` +
        `points to a non-markdown, non-lesson resource ("${resolved}"). Only ` +
        `lesson README.md and lesson directory links are supported.`,
    );
  }

  if (isMarkdown && !looksLikeLessonReadme) {
    throw new Error(
      `remark-link-rewrite: link "${rawUrl}" in ${options.moduleId}/${options.slug} ` +
        `points to "${segments.join('/')}". Only ` +
        `"<module>/<slug>/i18n/<ru|en>/README.md" markdown links are supported.`,
    );
  }

  const targetModuleId = segments[0];
  const targetSlug = segments[1];

  const moduleEntry = options.course.modules.find((m) => m.id === targetModuleId);
  if (!moduleEntry) {
    throw new Error(
      `remark-link-rewrite: link "${rawUrl}" in ${options.moduleId}/${options.slug} ` +
        `targets unknown module "${targetModuleId}" (not present in course.yaml)`,
    );
  }

  const lessonEntry = moduleEntry.lessons.find((l) => l.slug === targetSlug);
  if (!lessonEntry) {
    throw new Error(
      `remark-link-rewrite: link "${rawUrl}" in ${options.moduleId}/${options.slug} ` +
        `targets unknown lesson "${targetModuleId}/${targetSlug}" ` +
        `(not present in course.yaml)`,
    );
  }

  const basePath = options.basePath.replace(/\/+$/, '');
  const siteUrl = `${basePath}/${lang}/${targetModuleId}/${targetSlug}/${hashPart}`;
  return { url: siteUrl, external: false };
}

export default function remarkLinkRewrite(options: RemarkLinkRewriteOptions) {
  if (
    !options ||
    !options.moduleId ||
    !options.slug ||
    !options.course ||
    typeof options.basePath !== 'string'
  ) {
    throw new Error(
      'remark-link-rewrite: options.moduleId, options.slug, options.basePath and options.course are required',
    );
  }

  return function transformer(tree: AstNode): void {
    walk(tree, (node) => {
      if (node.type !== 'link' && node.type !== 'definition') return;
      const link = node as LinkNode;
      if (typeof link.url !== 'string') return;

      // mdast `definition` nodes are shared by image and link references.
      // remark-lesson-images already rewrites image-targeted definitions to
      // absolute asset paths (e.g. `/kafka-cookbook/static/lectures/.../foo.png`);
      // those would fail rewriteLessonLink's relative-only check, so skip them.
      // Link nodes still go through full validation — rewriteLessonLink handles
      // relative image URLs internally and preserves protocol/allowlist checks.
      if (
        node.type === 'definition' &&
        link.url.startsWith('/') &&
        IMAGE_EXT_RE.test(link.url)
      ) {
        return;
      }

      const { url, external } = rewriteLessonLink(link.url, options);
      link.url = url;

      if (external) {
        link.data = link.data ?? {};
        link.data.hProperties = link.data.hProperties ?? {};
        link.data.hProperties['data-external'] = 'true';
      }
    });
  };
}

function walk(node: AstNode, visitor: (node: AstNode) => void): void {
  visitor(node);
  const children = (node as ParentNode).children;
  if (Array.isArray(children)) {
    for (const child of children) {
      walk(child, visitor);
    }
  }
}

function splitHash(input: string): [string, string] {
  const i = input.indexOf('#');
  if (i === -1) return [input, ''];
  return [input.slice(0, i), input.slice(i)];
}

function isInside(candidate: string, root: string): boolean {
  const r = root.endsWith('/') ? root : `${root}/`;
  return candidate === root || candidate.startsWith(r);
}
