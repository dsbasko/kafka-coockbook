import { flattenLessons, type Course } from './course';
import { FURTHEST_STORAGE_KEY, PROGRESS_STORAGE_KEY, lessonKey } from './progress';

export const GATE_LOCKED_ATTR = 'data-lesson-locked';

/**
 * Inline script that runs synchronously in <head>, before <body> is parsed,
 * to decide whether the current URL points at a locked lesson. If so it
 * stamps `data-lesson-locked="true"` on <html>; CSS then hides the lesson
 * body and reveals the interstitial — no flash of content while React
 * hydrates. After hydration GateProvider keeps the attribute in sync.
 *
 * The script is intentionally pre-compiled rather than emitted via JSX so
 * the linear lesson order from course.yaml is baked into the bundle at
 * build time (no runtime fetch of the same data we already know).
 */
export function buildGateInitScript(course: Course, basePath: string): string {
  const linearKeys = flattenLessons(course).map((e) => lessonKey(e.moduleId, e.lesson.slug));
  const data = JSON.stringify({
    keys: linearKeys,
    basePath: basePath ?? '',
    progressKey: PROGRESS_STORAGE_KEY,
    furthestKey: FURTHEST_STORAGE_KEY,
    attr: GATE_LOCKED_ATTR,
  }).replace(/</g, '\\u003c');
  // Inline lang-strip mirrors `stripLangFromPath` from lib/lang.ts: removes a
  // leading `/ru/` or `/en/` segment so the parser sees `[moduleId, slug]` as
  // its first two segments regardless of the active language route. basePath
  // strip is segment-aware (mirrors LessonAwareLink) so `/foo` does not match
  // `/foobar/...`.
  return `(function(){try{var D=${data};var p=window.location.pathname;var base=D.basePath;if(base&&(p===base||p.indexOf(base+'/')===0)){p=p.slice(base.length)||'/'}var lm=p.match(/^\\/(ru|en)(\\/.*)?$/);if(lm){p=lm[2]&&lm[2].length>0?lm[2]:'/'}var m=p.replace(/^\\/+|\\/+$/g,'').split('/');if(m.length<2)return;var key=m[0]+'/'+m[1];var idx=D.keys.indexOf(key);if(idx<0)return;var fkey=null;try{fkey=window.localStorage.getItem(D.furthestKey)}catch(e){}var fidx=fkey?D.keys.indexOf(fkey):-1;if(fidx<0){try{var raw=window.localStorage.getItem(D.progressKey);if(raw){var pr=JSON.parse(raw);for(var i=0;i<D.keys.length;i++){var v=pr[D.keys[i]];if(v&&v.completed===true&&i>fidx){fidx=i}}}}catch(e){}}if(idx>fidx+1){document.documentElement.setAttribute(D.attr,'true')}}catch(e){}})();`;
}
