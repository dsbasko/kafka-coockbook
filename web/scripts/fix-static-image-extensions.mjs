#!/usr/bin/env node
// Next.js App Router materializes app/opengraph-image.tsx and app/icon.tsx as
// extension-less files (out/opengraph-image, out/icon). GitHub Pages serves
// such files as application/octet-stream, which breaks social-card crawlers
// and the favicon. Rename the files to .png and patch every reference in the
// generated HTML.
import { promises as fs, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.resolve(__dirname, '..', 'out');

const RENAMES = [
  { from: 'opengraph-image', to: 'opengraph-image.png' },
  { from: 'icon', to: 'icon.png' },
];

async function* walkHtml(dir) {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  for (const entry of entries) {
    const abs = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      yield* walkHtml(abs);
    } else if (entry.isFile() && entry.name.endsWith('.html')) {
      yield abs;
    }
  }
}

function patchContent(content) {
  let next = content;
  for (const { from, to } of RENAMES) {
    // Match /<name> when followed by ?, ", or backslash (for JSON-escaped
    // strings inside <script>). Lookahead keeps existing query/fragment.
    const re = new RegExp(`/${from}(?=[?"\\\\])`, 'g');
    next = next.replace(re, `/${to}`);
  }
  return next;
}

async function main() {
  if (!existsSync(OUT_DIR)) {
    console.error(`fix-static-image-extensions: ${OUT_DIR} not found`);
    process.exit(1);
  }

  for (const { from, to } of RENAMES) {
    const src = path.join(OUT_DIR, from);
    const dst = path.join(OUT_DIR, to);
    if (!existsSync(src)) {
      console.error(`fix-static-image-extensions: missing ${src}`);
      process.exit(1);
    }
    await fs.rename(src, dst);
  }

  let patched = 0;
  for await (const file of walkHtml(OUT_DIR)) {
    const original = await fs.readFile(file, 'utf8');
    const updated = patchContent(original);
    if (updated !== original) {
      await fs.writeFile(file, updated);
      patched += 1;
    }
  }

  console.log(
    `fix-static-image-extensions: renamed ${RENAMES.length} files, patched ${patched} html files`,
  );
}

main().catch((err) => {
  console.error('fix-static-image-extensions: failed');
  console.error(err);
  process.exit(1);
});
