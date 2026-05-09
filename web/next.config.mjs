import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import yaml from 'js-yaml';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const courseYamlPath = path.resolve(__dirname, '..', 'course.yaml');
const course = yaml.load(readFileSync(courseYamlPath, 'utf8'));
if (!course || typeof course.basePath !== 'string' || !course.basePath.startsWith('/')) {
  throw new Error(
    `next.config: course.yaml at ${courseYamlPath} must define a basePath string starting with '/'`,
  );
}
const coursePath = course.basePath;

const isProd = process.env.NODE_ENV === 'production';

/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'export',
  basePath: isProd ? coursePath : '',
  assetPrefix: isProd ? `${coursePath}/` : undefined,
  trailingSlash: true,
  images: { unoptimized: true },
  reactStrictMode: true,
  experimental: {
    typedRoutes: true,
  },
};

export default nextConfig;
