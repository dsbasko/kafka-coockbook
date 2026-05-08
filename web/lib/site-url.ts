const DEFAULT_SITE_URL = 'https://dsbasko.github.io';

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
