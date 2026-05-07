#!/usr/bin/env node
import { promises as fs, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, '..', '..');
const LECTURES_DIR = path.join(REPO_ROOT, 'lectures');
const PUBLIC_DIR = path.join(REPO_ROOT, 'web', 'public', 'static', 'lectures');

const ALLOWED_EXT = new Set([
  '.png',
  '.jpg',
  '.jpeg',
  '.svg',
  '.webp',
  '.gif',
]);

async function* walkImages(dir, relativeFromRoot = '') {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  for (const entry of entries) {
    const abs = path.join(dir, entry.name);
    const rel = path.join(relativeFromRoot, entry.name);
    if (entry.isDirectory()) {
      yield* walkImages(abs, rel);
    } else if (entry.isFile()) {
      const ext = path.extname(entry.name).toLowerCase();
      if (ALLOWED_EXT.has(ext)) {
        yield { abs, rel };
      }
    }
  }
}

async function main() {
  if (!existsSync(LECTURES_DIR)) {
    console.error(`sync-images: lectures dir not found at ${LECTURES_DIR}`);
    process.exit(1);
  }

  await fs.rm(PUBLIC_DIR, { recursive: true, force: true });
  await fs.mkdir(PUBLIC_DIR, { recursive: true });

  let copied = 0;
  const moduleEntries = await fs.readdir(LECTURES_DIR, { withFileTypes: true });

  for (const moduleEntry of moduleEntries) {
    if (!moduleEntry.isDirectory()) continue;
    if (!/^\d{2}-/.test(moduleEntry.name)) continue;

    const moduleDir = path.join(LECTURES_DIR, moduleEntry.name);
    const lessonEntries = await fs.readdir(moduleDir, { withFileTypes: true });

    for (const lessonEntry of lessonEntries) {
      if (!lessonEntry.isDirectory()) continue;

      const imagesDir = path.join(moduleDir, lessonEntry.name, 'images');
      if (!existsSync(imagesDir)) continue;

      const destDir = path.join(
        PUBLIC_DIR,
        moduleEntry.name,
        lessonEntry.name,
        'images',
      );

      for await (const file of walkImages(imagesDir)) {
        const dest = path.join(destDir, file.rel);
        await fs.mkdir(path.dirname(dest), { recursive: true });
        await fs.copyFile(file.abs, dest);
        copied += 1;
      }
    }
  }

  console.log(`sync-images: copied ${copied} image${copied === 1 ? '' : 's'}`);
}

main().catch((err) => {
  console.error('sync-images: failed');
  console.error(err);
  process.exit(1);
});
