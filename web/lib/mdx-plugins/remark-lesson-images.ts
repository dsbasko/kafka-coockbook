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

export function rewriteLessonImageUrl(
  url: string,
  options: RemarkLessonImagesOptions,
  context: { nodeKind: 'image' | 'definition' },
): string {
  if (PROTOCOL_RE.test(url)) {
    return url;
  }

  if (!url.startsWith('./images/')) {
    throw new Error(
      `remark-lesson-images: ${context.nodeKind} url "${url}" in ` +
        `${options.moduleId}/${options.slug} is not allowed. ` +
        `Only "./images/<file>" relative paths and absolute URLs (http(s)://, data:, mailto:, tel:) are supported.`,
    );
  }

  const relative = url.slice('./images/'.length);
  if (relative.length === 0) {
    throw new Error(
      `remark-lesson-images: empty image filename in ${options.moduleId}/${options.slug}`,
    );
  }
  if (relative.includes('..')) {
    throw new Error(
      `remark-lesson-images: image url "${url}" in ${options.moduleId}/${options.slug} ` +
        `must not contain ".." segments`,
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
      if (node.type === 'image' || node.type === 'definition') {
        const img = node as ImageLikeNode;
        img.url = rewriteLessonImageUrl(img.url, options, {
          nodeKind: node.type,
        });
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
