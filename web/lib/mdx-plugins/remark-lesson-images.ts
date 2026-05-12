export interface RemarkLessonImagesOptions {
  moduleId: string;
  slug: string;
  basePath: string;
}

interface ImageLikeNode {
  type: 'image' | 'definition';
  url: string;
  alt?: string | null;
  identifier?: string;
}

interface ParentNode {
  type: string;
  children?: AstNode[];
}

type AstNode = ImageLikeNode | ParentNode;

const PROTOCOL_RE = /^(?:[a-z][a-z0-9+.-]*:|\/\/|data:|mailto:|tel:|#)/i;

const IMAGE_PREFIXES = ['./images/', '../../images/'] as const;

function matchImagePrefix(url: string): string | null {
  for (const prefix of IMAGE_PREFIXES) {
    if (url.startsWith(prefix)) {
      return prefix;
    }
  }
  return null;
}

export function rewriteLessonImageUrl(
  url: string,
  options: RemarkLessonImagesOptions,
  context: { nodeKind: 'image' | 'definition' },
): string {
  if (PROTOCOL_RE.test(url)) {
    return url;
  }

  const prefix = matchImagePrefix(url);
  if (prefix === null) {
    throw new Error(
      `remark-lesson-images: ${context.nodeKind} url "${url}" in ` +
        `${options.moduleId}/${options.slug} is not allowed. ` +
        `Only "./images/<file>" (legacy) or "../../images/<file>" (post-migration) ` +
        `relative paths and absolute URLs (http(s)://, data:, mailto:, tel:) are supported.`,
    );
  }

  const relative = url.slice(prefix.length);
  if (relative.length === 0) {
    throw new Error(
      `remark-lesson-images: empty image filename in ${options.moduleId}/${options.slug}`,
    );
  }
  if (relative.includes('..')) {
    throw new Error(
      `remark-lesson-images: image url "${url}" in ${options.moduleId}/${options.slug} ` +
        `must not contain ".." segments after the images/ prefix`,
    );
  }

  const basePath = options.basePath.replace(/\/+$/, '');
  return `${basePath}/static/lectures/${options.moduleId}/${options.slug}/images/${relative}`;
}

export default function remarkLessonImages(options: RemarkLessonImagesOptions) {
  if (!options || !options.moduleId || !options.slug) {
    throw new Error(
      'remark-lesson-images: options.moduleId and options.slug are required',
    );
  }

  return function transformer(tree: AstNode): void {
    walk(tree, (node) => {
      if (node.type === 'image') {
        const img = node as ImageLikeNode;
        img.url = rewriteLessonImageUrl(img.url, options, {
          nodeKind: 'image',
        });
        return;
      }
      // mdast `definition` nodes are shared between image and link references.
      // Only claim ones that clearly point to a lesson image; let
      // remark-link-rewrite handle the rest.
      if (node.type === 'definition') {
        const def = node as ImageLikeNode;
        if (typeof def.url === 'string' && matchImagePrefix(def.url) !== null) {
          def.url = rewriteLessonImageUrl(def.url, options, {
            nodeKind: 'definition',
          });
        }
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
