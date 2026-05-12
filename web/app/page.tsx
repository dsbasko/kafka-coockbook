import { AppShell } from '@/components/AppShell';
import { GateProvider } from '@/components/GateProvider';
import { HomePage } from '@/components/HomePage';
import { loadCourse } from '@/lib/course-loader';
import { DEFAULT_LANG, LANG_INIT_SCRIPT, LANGS } from '@/lib/lang';
import { buildSiteUrl, getRuntimeBasePath } from '@/lib/site-url';

export default function RootPage() {
  // Static `/index.html` always serves the default-language HomePage. Visitors
  // whose stored or browser language differs are redirected client-side by
  // LANG_INIT_SCRIPT to `/{lang}/`. <noscript> ships explicit per-lang links so
  // users without JS can still pick a language.
  const course = loadCourse(DEFAULT_LANG);
  const basePath = getRuntimeBasePath(course.basePath);

  return (
    <>
      <script
        id="lang-init"
        // Runs synchronously before the body renders so a redirect, if needed,
        // happens before the user sees default-language content.
        dangerouslySetInnerHTML={{ __html: LANG_INIT_SCRIPT }}
      />
      <GateProvider course={course} basePath={basePath}>
        <AppShell course={course}>
          <HomePage course={course} level="Go" />
        </AppShell>
      </GateProvider>
      <noscript>
        <ul>
          {LANGS.map((lang) => (
            <li key={lang}>
              <a href={buildSiteUrl(course.basePath, [lang])}>{lang.toUpperCase()}</a>
            </li>
          ))}
        </ul>
      </noscript>
    </>
  );
}
