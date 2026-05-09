const DEFAULT_SITE_URL = 'https://dsbasko.github.io';

/**
 * Returns the basePath that matches Next.js routing in the current runtime.
 * In production (and any non-development NODE_ENV) this is the configured
 * `course.basePath` (e.g. `/kafka-cookbook`). In dev `next.config.mjs`
 * disables basePath, so internal links and assets must point at the root —
 * otherwise rewritten markdown URLs 404 against the dev server.
 */
export function getRuntimeBasePath(coursePath: string): string {
  return process.env.NODE_ENV === 'development' ? '' : coursePath;
}

export function getSiteUrl(): string {
  const raw = process.env.NEXT_PUBLIC_SITE_URL;
  const value = raw && raw.trim().length > 0 ? raw.trim() : DEFAULT_SITE_URL;
  return value.replace(/\/+$/, '');
}

export function buildSiteUrl(basePath: string, segments: string[] = []): string {
  const root = getSiteUrl();
  const normalizedBase = basePath.replace(/\/+$/, '');
  const path = segments.filter(Boolean).join('/');
  const tail = path.length > 0 ? `/${path}/` : '/';
  return `${root}${normalizedBase}${tail}`;
}

export function buildAssetUrl(basePath: string, asset: string): string {
  const root = getSiteUrl();
  const normalizedBase = basePath.replace(/\/+$/, '');
  const normalizedAsset = asset.startsWith('/') ? asset : `/${asset}`;
  return `${root}${normalizedBase}${normalizedAsset}`;
}
