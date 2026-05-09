# web/

Next.js 14 (App Router, `output: 'export'`) — статический сайт курса. Деплой на GitHub Pages по push в `main` через `.github/workflows/deploy.yml`.

Прод: **https://dsbasko.github.io/kafka-cookbook/**

## Локальный запуск

Требования: Node ≥ 20, pnpm 9.15.0 (`corepack enable && corepack prepare pnpm@9.15.0 --activate`).

```sh
pnpm install
pnpm dev          # http://localhost:3000
```

Из корня репозитория то же самое доступно через `make web-dev` и т.д. — корневой `Makefile` проксирует все web-цели.

## Скрипты

| Скрипт                    | Что делает                                                              |
| ------------------------- | ----------------------------------------------------------------------- |
| `pnpm dev`                | Next.js dev-сервер с HMR.                                               |
| `pnpm build`              | Прогон `prebuild` (sync-images) → `next build` → статика в `out/`.      |
| `pnpm start`              | Запуск собранного бандла (для smoke-теста статики локально).            |
| `pnpm lint`               | `eslint` через `next lint`.                                             |
| `pnpm typecheck`          | `tsc --noEmit`.                                                         |
| `pnpm test`               | Один прогон vitest (`vitest run`).                                      |
| `pnpm test:watch`         | vitest в watch-режиме.                                                  |
| `pnpm exec tsx scripts/check-course-coverage.ts` | Сверка `course.yaml` ↔ `lectures/<module>/<slug>/`. |

## Структура каталога

```
web/
├── app/                # Next.js App Router
│   ├── layout.tsx      # ThemeProvider, шрифты, AppShell
│   ├── page.tsx        # /
│   ├── [module]/
│   │   ├── page.tsx                # /<module>
│   │   └── [lesson]/page.tsx       # /<module>/<lesson>
│   ├── not-found.tsx
│   ├── sitemap.ts                  # /sitemap.xml
│   ├── robots.ts                   # /robots.txt
│   ├── opengraph-image.tsx         # дефолтная og:image
│   └── icon.tsx                    # favicon
├── components/         # AppShell, Sidebar, Header, ProgramDrawer, LessonLayout,
│                       # Toc, ProgressBar, CodeBlock, Callout, LessonNav,
│                       # LessonMeta, ThemeToggle, ThemeProvider
├── lib/                # course.ts (парсер course.yaml), lesson.ts (fs-helpers),
│                       # markdown.ts (unified-pipeline), progress.ts, theme.ts,
│                       # site-url.ts, extract-toc.ts, slug.ts,
│                       # mdx-plugins/ (remark-lesson-images, remark-link-rewrite,
│                       # rehype-callout, rehype-lift-codeblock-language)
├── styles/             # tokens.css, reset.css, globals.css, markdown.css
├── scripts/
│   ├── sync-images.mjs                # prebuild — копирует lectures/**/images/* в public/static/lectures/
│   └── check-course-coverage.ts       # сверка course.yaml ↔ lectures/
├── public/
│   ├── static/lectures/               # output sync-images.mjs (gitignored)
│   └── ...
├── next.config.mjs                    # output: 'export', basePath: '/kafka-cookbook' в prod
├── tsconfig.json                      # strict, paths: { "@/*": ["./*"] }
└── vitest.config.ts
```

## Контентный пайплайн

Lesson README — обычный markdown (не MDX). Цепочка `unified` в `lib/markdown.ts`:

1. `remark-parse` → mdast.
2. `remark-gfm` — таблицы, чек-листы.
3. `remark-github-blockquote-alert` — `> [!NOTE]` блоки.
4. `remark-lesson-images` (custom) — переписывает `./images/foo.png` в site-URL.
5. `remark-link-rewrite` (custom) — переписывает `../module/slug/README.md` в site-URL, валидирует против `course.yaml`.
6. `remark-rehype` → hast.
7. `rehype-slug` + `rehype-autolink-headings`.
8. `rehype-pretty-code` (Shiki dual-theme `github-light` + `night-owl`).
9. `rehypeLiftCodeBlockLanguage` (custom) — поднимает `data-language` с `<pre>` на `<figure>`.
10. `rehype-callout` (custom) — нормализует callouts в `<aside data-callout-type=…>`.
11. `hast-util-to-jsx-runtime` с `components` map (`figure`, `aside`).

Картинки лекций синхронизируются из `lectures/**/images/*` в `web/public/static/lectures/...` скриптом `scripts/sync-images.mjs` (запускается в `prebuild`).

`course.yaml` в корне репо — единственный источник истины про порядок модулей и человеко-читаемые заголовки. Парсер — `lib/course-loader.ts` (server-only) + чистые helper'ы и типы в `lib/course.ts` (можно импортить в client-компонентах). Сверку с `lectures/` руками делает `make web-check-coverage`.

## Темы оформления

`data-theme="light"|"dark"` на `<html>`. Anti-FOUC inline-скрипт в `<head>` ставит значение из `localStorage` ДО гидрации. `ThemeProvider` (client) хранит `theme: 'light'|'dark'|'system'`, слушает `prefers-color-scheme` пока выбран `system`. Shiki генерирует обе темы через `--shiki-light` / `--shiki-dark` CSS-переменные, селекторы в `markdown.css` показывают актуальную.

## Прогресс

`localStorage` ключ `kafka-cookbook-progress`: `{ "<module>/<slug>": { completed: true, at: ISO } }`. Кнопка «Отметить пройденным» в `LessonNav`, чекмарки в `ProgramDrawer`, счётчик в `ProgressBar` (Header). До mount всё рендерится placeholder'ами фиксированной ширины — никакого CLS при гидрации.

## Тесты

Vitest. Юниты лежат рядом с реализацией (`*.test.ts`):

- `lib/course.test.ts`, `lib/slug.test.ts` — парсер манифеста, нормализация slug'а.
- `lib/lesson.test.ts` — fs-чтение README.
- `lib/markdown.test.ts` — интеграционные кейсы пайплайна (ASCII-стрелки, generic-плейсхолдеры, GFM, code blocks, callouts).
- `lib/progress.test.ts`, `lib/theme.test.ts` — клиентская логика (jsdom + in-memory Storage shim).
- `lib/site-url.test.ts`, `lib/extract-toc.test.ts` — pure-helpers.
- `lib/mdx-plugins/*.test.ts` — image rewriter, link rewriter, callout normalizer.

Запуск:

```sh
pnpm test                       # все тесты один раз
pnpm exec vitest run lib/       # только модули lib/
pnpm test:watch                 # watch
```

## Деплой

GitHub Action `.github/workflows/deploy.yml` на push в `main`:

1. Checkout, pnpm 9.15.0, Node 22.
2. `pnpm install --frozen-lockfile` в `web/`.
3. `make web-build` (prebuild sync-images + `next build`).
4. Guard: `test -f web/out/404.html`.
5. `actions/upload-pages-artifact@v3` (`path: web/out`).
6. `actions/deploy-pages@v4`.

Включить деплой: Settings → Pages → Source: «GitHub Actions». Дальше push в `main` поднимает прод.
