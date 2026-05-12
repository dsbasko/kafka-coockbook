import { flattenLessons, type Course } from './course';
import { getDict } from './i18n';
import type { Lang } from './lang';
import { FURTHEST_STORAGE_KEY, PROGRESS_STORAGE_KEY, lessonKey, type LessonKey } from './progress';

export const GATE_ITEM_LOCKED_ATTR = 'data-locked';
export const GATE_ITEM_KEY_ATTR = 'data-lesson-key';

/**
 * Single inline script that "paints" the SSR-rendered page against the user's
 * actual progress *synchronously, before first paint*. JSX outputs a stable
 * pre-hydrated baseline (everything 0%, nothing completed, default CTA copy);
 * this script reads localStorage, computes furthest/completed/percent, and
 * mutates the DOM directly — setting attributes on lesson rows and rewriting
 * text/href in dedicated slots. Avoids the React-driven re-render flash that
 * useEffect-based hydration causes.
 *
 * The same logic is mirrored in {@link applyGatePainting} for post-hydration
 * updates (SPA route changes, cross-tab progress sync, drawer expand). React
 * never sees these attributes in JSX, so reconciliation can't clobber them.
 */
export function buildGateMarkScript(
  course: Course,
  basePath: string,
  defaultLang: Lang,
): string {
  const flat = flattenLessons(course);
  // moduleTitles[i] = the title of the module containing lesson i. Pre-
  // computed here so the inline script doesn't need to scan modules at
  // runtime — it just indexes by linear lesson index.
  const moduleTitleByIndex: string[] = [];
  for (const entry of flat) {
    const mod = course.modules.find((m) => m.id === entry.moduleId);
    moduleTitleByIndex.push(mod?.title ?? '');
  }
  const data = JSON.stringify({
    keys: flat.map((e) => lessonKey(e.moduleId, e.lesson.slug)),
    titles: flat.map((e) => e.lesson.title),
    moduleTitles: moduleTitleByIndex,
    basePath: basePath ?? '',
    progressKey: PROGRESS_STORAGE_KEY,
    furthestKey: FURTHEST_STORAGE_KEY,
    defaultLang,
    // Per-lang connector word for aria-valuetext ("X из Y" / "X of Y").
    // Lookup at runtime by the lang derived from URL prefix.
    progressConnector: {
      ru: getDict('ru').progressAriaConnector,
      en: getDict('en').progressAriaConnector,
    },
  });
  // Body kept in one IIFE so it can ship as inline <script> verbatim.
  return `(function(){try{var D=${data};${GATE_PAINT_BODY}}catch(e){}})();`;
}

// Plain JS body. Extracted as a constant so the imperative `applyGatePainting`
// stays in lockstep with the inline script — same algorithm, same attribute
// names, same edge cases.
const GATE_PAINT_BODY = `
// Derive active lang from URL so the inline CTA href stays in /<lang>/.../ —
// the root layout (which renders this script) has no lang context, but the
// browser does. Mirrors stripLangFromPath in lib/lang.ts.
var __p=window.location.pathname;if(D.basePath&&__p.indexOf(D.basePath)===0)__p=__p.slice(D.basePath.length)||'/';var __lm=__p.match(/^\\/(ru|en)(\\/.*)?$/);var lang=__lm?__lm[1]:D.defaultLang;var connector=(D.progressConnector&&D.progressConnector[lang])||'of';
var fkey=null;try{fkey=window.localStorage.getItem(D.furthestKey)}catch(e){}
var fidx=fkey?D.keys.indexOf(fkey):-1;
var completed={};var completedCount=0;
try{var raw=window.localStorage.getItem(D.progressKey);if(raw){var pr=JSON.parse(raw);for(var i=0;i<D.keys.length;i++){var k=D.keys[i];if(pr[k]&&pr[k].completed===true){completed[k]=true;completedCount++;if(i>fidx)fidx=i}}}}catch(e){}
var hasProgress=completedCount>0||!!fkey;
var frontierIdx=Math.max(0,Math.min(fidx+1,D.keys.length-1));

// 1. Lesson rows: data-locked + data-completed.
var rows=document.querySelectorAll('[data-lesson-key]');
for(var j=0;j<rows.length;j++){var el=rows[j];var k2=el.getAttribute('data-lesson-key');var idx=D.keys.indexOf(k2);el.removeAttribute('data-locked');el.removeAttribute('data-completed');el.removeAttribute('data-next');el.removeAttribute('aria-disabled');if(idx>=0&&idx>fidx+1){el.setAttribute('data-locked','true');el.setAttribute('aria-disabled','true');el.setAttribute('tabindex','-1')}else{el.removeAttribute('tabindex')}if(completed[k2]){el.setAttribute('data-completed','true')}}

// 2. "Next" marker within each group (first non-completed in DOM order).
var groups=document.querySelectorAll('[data-lesson-group]');
for(var g=0;g<groups.length;g++){var gr=groups[g].querySelectorAll('[data-lesson-key]');for(var x=0;x<gr.length;x++){gr[x].removeAttribute('data-next')}for(var x2=0;x2<gr.length;x2++){if(!gr[x2].hasAttribute('data-completed')){gr[x2].setAttribute('data-next','true');break}}}

// 3. Progress slots: numbers, percent, bar fill. Scope = 'global' uses the
//    whole course; scope = 'module' uses data-progress-keys (csv) for in-page
//    per-module cards.
var slots=document.querySelectorAll('[data-progress-scope]');
for(var p=0;p<slots.length;p++){var s=slots[p];var scope=s.getAttribute('data-progress-scope');var sKeys;if(scope==='module'){var csv=s.getAttribute('data-progress-keys')||'';sKeys=csv?csv.split(','):[]}else{sKeys=D.keys}var sDone=0;for(var i2=0;i2<sKeys.length;i2++){if(completed[sKeys[i2]])sDone++}var sTotal=sKeys.length;var sPct=sTotal>0?Math.round((sDone/sTotal)*100):0;var qN=s.querySelectorAll('[data-progress-count]');for(var i3=0;i3<qN.length;i3++)qN[i3].textContent=sDone;var qP=s.querySelectorAll('[data-progress-pct]');for(var i4=0;i4<qP.length;i4++)qP[i4].textContent=sPct;var qB=s.querySelectorAll('[data-progress-bar]');for(var i5=0;i5<qB.length;i5++)qB[i5].style.width=sPct+'%';var state=sTotal===0?'empty':sDone===0?'not-started':sDone===sTotal?'complete':'in-progress';s.setAttribute('data-progress-state',state);if(s.getAttribute('role')==='progressbar'){s.setAttribute('aria-valuenow',String(sDone));s.setAttribute('aria-valuemax',String(sTotal));s.setAttribute('aria-valuetext',sDone+' '+connector+' '+sTotal+' ('+sPct+'%)')}}

// 4. Module rows on HomePage: each row carries data-progress-keys + slot for
//    its own count/pct/state. Re-uses the same logic above by being matched
//    via data-progress-scope='module'.

// 5. CTA frontier rows: scope = 'global' picks the global frontier; scope =
//    'module' picks the first non-completed lesson in the module, falling
//    back to the global frontier if that's locked. The row carries variants
//    (not-started, in-progress, complete) and we set data-cta-state on it
//    so CSS hides the unused variants.
var ctas=document.querySelectorAll('[data-cta-frontier]');
for(var c=0;c<ctas.length;c++){var row=ctas[c];var ctaScope=row.getAttribute('data-cta-frontier');var targetIdx=-1;var ctaState='not-started';if(ctaScope==='module'){var mcsv=row.getAttribute('data-progress-keys')||'';var mKeys=mcsv?mcsv.split(','):[];var mDone=0;var mNext=null;for(var i6=0;i6<mKeys.length;i6++){if(completed[mKeys[i6]]){mDone++}else if(mNext===null){mNext=mKeys[i6]}}var mTotal=mKeys.length;if(mTotal>0&&mDone===mTotal){ctaState='complete';targetIdx=D.keys.indexOf(mKeys[0])}else if(mNext!==null){var nextIdx=D.keys.indexOf(mNext);if(nextIdx>fidx+1){targetIdx=frontierIdx}else{targetIdx=nextIdx}ctaState=mDone>0?'in-progress':hasProgress?'in-progress':'not-started'}}else{targetIdx=frontierIdx;ctaState=hasProgress?'in-progress':'not-started'}if(targetIdx<0||targetIdx>=D.keys.length){row.setAttribute('data-cta-state',ctaState);continue}var tKey=D.keys[targetIdx];var tTitle=D.titles[targetIdx];var hrefVal=(D.basePath||'')+'/'+lang+'/'+tKey+'/';var links=row.querySelectorAll('[data-cta-frontier-link]');for(var i7=0;i7<links.length;i7++)links[i7].setAttribute('href',hrefVal);var tTitleEls=row.querySelectorAll('[data-cta-frontier-title]');for(var i8=0;i8<tTitleEls.length;i8++)tTitleEls[i8].textContent=tTitle;var tNumEls=row.querySelectorAll('[data-cta-frontier-num]');for(var i9=0;i9<tNumEls.length;i9++)tNumEls[i9].textContent=String(targetIdx+1);row.setAttribute('data-cta-state',ctaState)}

// 6. Continue hints (HomePage frontier line under CTA, interstitial side card).
var hints=document.querySelectorAll('[data-frontier-hint]');
if(hints.length>0){var hKey=D.keys[frontierIdx];var hTitle=D.titles[frontierIdx];var hMod=D.moduleTitles[frontierIdx];for(var i10=0;i10<hints.length;i10++){var h=hints[i10];var hLes=h.querySelectorAll('[data-frontier-hint-lesson]');var hMo=h.querySelectorAll('[data-frontier-hint-module]');if(hKey){h.setAttribute('data-hint-state',hasProgress?'visible':'hidden')}else{h.setAttribute('data-hint-state','hidden')}for(var i11=0;i11<hLes.length;i11++)hLes[i11].textContent=hTitle;for(var i12=0;i12<hMo.length;i12++)hMo[i12].textContent=hMod}}

// 7. Steps-until counters (interstitial "до этого урока N шагов"). The slot
//    carries data-steps-target-index = linear index of the locked target;
//    the slot text becomes max(0, target - frontier) and data-steps-state
//    is flipped so CSS hides the row when there's effectively no gap.
var steps=document.querySelectorAll('[data-steps-until]');
for(var sx=0;sx<steps.length;sx++){var sel=steps[sx];var tgt=parseInt(sel.getAttribute('data-steps-target-index')||'-1',10);var diff=tgt>=0?Math.max(0,tgt-frontierIdx):0;sel.textContent=String(diff);sel.setAttribute('data-steps-state',diff>0?'visible':'hidden')}

document.documentElement.setAttribute('data-has-progress',hasProgress?'true':'false');
`;

export function applyGatePainting(
  course: Course,
  furthestIndex: number,
  basePath: string,
  lang: Lang,
): void {
  if (typeof document === 'undefined') return;
  const flat = flattenLessons(course);
  const keys = flat.map((e) => lessonKey(e.moduleId, e.lesson.slug));
  const titles = flat.map((e) => e.lesson.title);

  // Pull live progress from localStorage so this mirrors what the inline
  // script saw. Using the same source on both paths (initial paint and
  // subsequent updates) keeps state consistent.
  const moduleTitleByIndex: string[] = [];
  for (const entry of flat) {
    const mod = course.modules.find((m) => m.id === entry.moduleId);
    moduleTitleByIndex.push(mod?.title ?? '');
  }
  const completed: Record<string, boolean> = {};
  let completedCount = 0;
  let fidx = furthestIndex;
  try {
    const raw = window.localStorage.getItem(PROGRESS_STORAGE_KEY);
    if (raw) {
      const pr = JSON.parse(raw) as Record<string, { completed?: boolean } | undefined>;
      for (let i = 0; i < keys.length; i += 1) {
        const k = keys[i];
        if (pr[k]?.completed === true) {
          completed[k] = true;
          completedCount += 1;
          if (i > fidx) fidx = i;
        }
      }
    }
  } catch {
    /* ignore — leave completed empty */
  }
  let furthestKeyExists: string | null = null;
  try {
    furthestKeyExists = window.localStorage.getItem(FURTHEST_STORAGE_KEY);
  } catch {
    /* ignore */
  }
  const hasProgress = completedCount > 0 || !!furthestKeyExists;
  const frontierIdx = Math.max(0, Math.min(fidx + 1, keys.length - 1));

  // 1. Lesson rows.
  document.querySelectorAll<HTMLElement>('[data-lesson-key]').forEach((el) => {
    const k = (el.getAttribute('data-lesson-key') ?? '') as LessonKey;
    const idx = keys.indexOf(k);
    el.removeAttribute('data-locked');
    el.removeAttribute('data-completed');
    el.removeAttribute('data-next');
    el.removeAttribute('aria-disabled');
    if (idx >= 0 && idx > fidx + 1) {
      el.setAttribute('data-locked', 'true');
      el.setAttribute('aria-disabled', 'true');
      el.setAttribute('tabindex', '-1');
    } else {
      el.removeAttribute('tabindex');
    }
    if (completed[k]) {
      el.setAttribute('data-completed', 'true');
    }
  });

  // 2. Next markers per group.
  document.querySelectorAll<HTMLElement>('[data-lesson-group]').forEach((group) => {
    const rows = group.querySelectorAll<HTMLElement>('[data-lesson-key]');
    rows.forEach((r) => r.removeAttribute('data-next'));
    for (let i = 0; i < rows.length; i += 1) {
      if (!rows[i].hasAttribute('data-completed')) {
        rows[i].setAttribute('data-next', 'true');
        break;
      }
    }
  });

  // 3. Progress slots.
  document.querySelectorAll<HTMLElement>('[data-progress-scope]').forEach((slot) => {
    const scope = slot.getAttribute('data-progress-scope');
    let slotKeys: string[];
    if (scope === 'module') {
      const csv = slot.getAttribute('data-progress-keys') ?? '';
      slotKeys = csv ? csv.split(',') : [];
    } else {
      slotKeys = keys;
    }
    let slotDone = 0;
    for (const k of slotKeys) if (completed[k]) slotDone += 1;
    const slotTotal = slotKeys.length;
    const slotPct = slotTotal > 0 ? Math.round((slotDone / slotTotal) * 100) : 0;
    slot.querySelectorAll<HTMLElement>('[data-progress-count]').forEach((n) => {
      n.textContent = String(slotDone);
    });
    slot.querySelectorAll<HTMLElement>('[data-progress-pct]').forEach((n) => {
      n.textContent = String(slotPct);
    });
    slot.querySelectorAll<HTMLElement>('[data-progress-bar]').forEach((bar) => {
      bar.style.width = `${slotPct}%`;
    });
    const state =
      slotTotal === 0
        ? 'empty'
        : slotDone === 0
          ? 'not-started'
          : slotDone === slotTotal
            ? 'complete'
            : 'in-progress';
    slot.setAttribute('data-progress-state', state);
    // Mirror values onto ARIA attributes when this slot is also a
    // progressbar (Header ProgressBar uses role=progressbar). Other slots
    // (HomePage stats card, ModulePage side card) carry the same data but
    // present it as plain text — no role, so we don't pollute them with
    // misleading ARIA values.
    if (slot.getAttribute('role') === 'progressbar') {
      slot.setAttribute('aria-valuenow', String(slotDone));
      slot.setAttribute('aria-valuemax', String(slotTotal));
      slot.setAttribute(
        'aria-valuetext',
        `${slotDone} ${getDict(lang).progressAriaConnector} ${slotTotal} (${slotPct}%)`,
      );
    }
  });

  // 4. CTA frontier.
  document.querySelectorAll<HTMLElement>('[data-cta-frontier]').forEach((row) => {
    const ctaScope = row.getAttribute('data-cta-frontier');
    let targetIdx = -1;
    let ctaState = 'not-started';
    if (ctaScope === 'module') {
      const csv = row.getAttribute('data-progress-keys') ?? '';
      const mKeys = csv ? csv.split(',') : [];
      let mDone = 0;
      let mNext: string | null = null;
      for (const k of mKeys) {
        if (completed[k]) {
          mDone += 1;
        } else if (mNext === null) {
          mNext = k;
        }
      }
      const mTotal = mKeys.length;
      if (mTotal > 0 && mDone === mTotal) {
        ctaState = 'complete';
        targetIdx = keys.indexOf(mKeys[0] as LessonKey);
      } else if (mNext !== null) {
        const nextIdx = keys.indexOf(mNext as LessonKey);
        targetIdx = nextIdx > fidx + 1 ? frontierIdx : nextIdx;
        ctaState = mDone > 0 || hasProgress ? 'in-progress' : 'not-started';
      }
    } else {
      targetIdx = frontierIdx;
      ctaState = hasProgress ? 'in-progress' : 'not-started';
    }
    if (targetIdx >= 0 && targetIdx < keys.length) {
      const tKey = keys[targetIdx];
      const tTitle = titles[targetIdx];
      const hrefVal = `${basePath || ''}/${lang}/${tKey}/`;
      row.querySelectorAll<HTMLElement>('[data-cta-frontier-link]').forEach((link) => {
        link.setAttribute('href', hrefVal);
      });
      row.querySelectorAll<HTMLElement>('[data-cta-frontier-title]').forEach((n) => {
        n.textContent = tTitle;
      });
      row.querySelectorAll<HTMLElement>('[data-cta-frontier-num]').forEach((n) => {
        n.textContent = String(targetIdx + 1);
      });
    }
    row.setAttribute('data-cta-state', ctaState);
  });

  // 5. Frontier hint (HomePage line + interstitial side card).
  document.querySelectorAll<HTMLElement>('[data-frontier-hint]').forEach((h) => {
    const tTitle = titles[frontierIdx] ?? '';
    const tModule = moduleTitleByIndex[frontierIdx] ?? '';
    h.querySelectorAll<HTMLElement>('[data-frontier-hint-lesson]').forEach((n) => {
      n.textContent = tTitle;
    });
    h.querySelectorAll<HTMLElement>('[data-frontier-hint-module]').forEach((n) => {
      n.textContent = tModule;
    });
    h.setAttribute('data-hint-state', hasProgress ? 'visible' : 'hidden');
  });

  // 6. Steps-until counters.
  document.querySelectorAll<HTMLElement>('[data-steps-until]').forEach((slot) => {
    const tgt = Number.parseInt(
      slot.getAttribute('data-steps-target-index') ?? '-1',
      10,
    );
    const diff = tgt >= 0 ? Math.max(0, tgt - frontierIdx) : 0;
    slot.textContent = String(diff);
    slot.setAttribute('data-steps-state', diff > 0 ? 'visible' : 'hidden');
  });

  document.documentElement.setAttribute(
    'data-has-progress',
    hasProgress ? 'true' : 'false',
  );
}

// Kept as a thin alias so existing imports (`applyGateMarking`) continue to
// work — the gate-mark module historically only handled lock state, while
// this new implementation also paints progress/CTA/hints. New code should
// import applyGatePainting directly.
export function applyGateMarking(course: Course, furthestIndex: number): void {
  // basePath here is best-effort — when called without basePath via the old
  // signature, fall back to '' which makes hrefs site-root-relative. The
  // initial inline script always knows the real basePath. Lang defaults to
  // 'en' which matches DEFAULT_LANG; real callers should use the language-
  // aware applyGatePainting directly.
  applyGatePainting(course, furthestIndex, '', 'en');
}
