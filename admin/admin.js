/**
 * Panel admin v1 — consume app.main FastAPI (puerto 8002 por defecto).
 */

const $ = (id) => document.getElementById(id);

let rawArticles = [];
let filterOptions = { score_buckets: [], topic_tags: [], narrative_candidates: [] };

const state = {
  surgeMap: new Map(),
};

/** Ficheros alerts_*.json a fusionar (debe coincidir con el default del API). */
const ALERTS_RECENT_FILES = 5;

const EVOLUTION_MAX_NEW = 12;
const EVOLUTION_MAX_RISING = 8;
const EVOLUTION_MAX_FADING = 8;

function apiBase() {
  return $("apiBase").value.replace(/\/$/, "");
}

function showError(msg) {
  const el = $("error");
  el.textContent = msg;
  el.classList.toggle("hidden", !msg);
}

function scoreNum(a, key) {
  const v = a[key];
  if (typeof v === "number" && !Number.isNaN(v)) return v;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function populateSelect(selectEl, values, includeAll = true) {
  selectEl.innerHTML = "";
  if (includeAll) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "(todos)";
    selectEl.appendChild(opt);
  }
  for (const v of values) {
    const opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    selectEl.appendChild(opt);
  }
}

async function fetchJson(path) {
  const res = await fetch(`${apiBase()}${path}`);
  const text = await res.text();
  let data;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    throw new Error(`Respuesta no JSON (${res.status}): ${text.slice(0, 200)}`);
  }
  if (!res.ok) {
    const detail = data?.detail ?? text;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return data;
}

async function loadFilters() {
  return fetchJson("/articles/filters");
}

async function loadLatest() {
  return fetchJson("/articles/latest");
}

async function loadNarrativesLatest() {
  return fetchJson("/narratives/latest");
}

/** Artefactos pipeline: último lifecycle + meta del diff (no lanza si el endpoint falta). */
async function loadNarrativeHistoryLatest() {
  try {
    const res = await fetch(`${apiBase()}/narrative-history/latest`);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

/** No lanza: si no hay alerts o falla la red, devuelve null. Usa últimos N JSON fusionados. */
async function loadAlertsRecent() {
  try {
    const q = new URLSearchParams({ limit: String(ALERTS_RECENT_FILES) });
    const res = await fetch(`${apiBase()}/alerts/recent?${q}`);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

function inferSurgeBucketFromGrowth(growth) {
  const g = Number(growth);
  if (!Number.isFinite(g)) return null;
  if (g >= 2.0) return "surge_200";
  if (g >= 1.0) return "surge_100";
  if (g >= 0.5) return "surge_50";
  return null;
}

function buildSurgeMap(alertsPayload) {
  const map = new Map();

  if (!alertsPayload || !Array.isArray(alertsPayload.alerts)) {
    return map;
  }

  for (const a of alertsPayload.alerts) {
    if (!a || a.type !== "surge") continue;

    const name = (a.narrative || "").trim();
    const raw = a.surge_bucket;
    const bucket =
      (typeof raw === "string" && raw.trim()) || inferSurgeBucketFromGrowth(a.growth);

    if (name && bucket) {
      map.set(name, bucket);
    }
  }

  return map;
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function isRenderableAlert(a) {
  return (
    a &&
    (a.type === "surge" ||
      a.type === "early_opportunity" ||
      a.type === "confirmed_momentum")
  );
}

function alertStripKind(a) {
  if (a.type === "surge") return "surge";
  if (a.type === "early_opportunity") return "early";
  if (a.type === "confirmed_momentum") return "momentum";
  return "surge";
}

function alertStripLeftText(a) {
  if (a.type === "surge") return "🔥 SURGE";
  if (a.type === "early_opportunity") return "🚀 EARLY";
  if (a.type === "confirmed_momentum") return "⚠️ MOMENTUM";
  return "·";
}

function alertStripRightText(a) {
  if (a.type === "surge") {
    return `+${Math.round((a.growth || 0) * 100)}%`;
  }
  const s = a.narrative_strength;
  if (s != null && s !== "") return String(s);
  return "—";
}

function renderAlertStripHtml(a) {
  const nar = escapeHtml(a.narrative || "");
  const kind = alertStripKind(a);
  const left = escapeHtml(alertStripLeftText(a));
  const right = escapeHtml(alertStripRightText(a));
  return `
    <div class="alert-strip alert-strip--${kind}" role="row">
      <div class="alert-strip__left">${left}</div>
      <div class="alert-strip__center">${nar}</div>
      <div class="alert-strip__right">${right}</div>
    </div>
  `;
}

function renderNarrativeEvolution(payload) {
  const summary = $("evolutionSummary");
  const diffLine = $("evolutionDiffLine");
  const ulN = $("evolutionNew");
  const ulR = $("evolutionRising");
  const ulF = $("evolutionFading");
  if (!summary || !diffLine || !ulN || !ulR || !ulF) return;

  const emptyCols = () => {
    for (const ul of [ulN, ulR, ulF]) {
      ul.replaceChildren();
      const li = document.createElement("li");
      li.className = "evolution-list__empty";
      li.textContent = "—";
      ul.appendChild(li);
    }
  };

  if (
    !payload ||
    (payload.lifecycle == null &&
      payload.diff_meta == null &&
      !payload.lifecycle_source &&
      !payload.diff_source)
  ) {
    summary.textContent =
      "Sin artefactos de historial. Ejecuta el pipeline (persist_narrative_history + classify_narrative_lifecycle) y / o arranca la API.";
    diffLine.textContent = "";
    emptyCols();
    return;
  }

  const lc =
    payload.lifecycle && typeof payload.lifecycle === "object"
      ? payload.lifecycle
      : null;
  const dm =
    payload.diff_meta && typeof payload.diff_meta === "object"
      ? payload.diff_meta
      : null;

  if (lc) {
    const run = lc.run_id ?? "—";
    const when = lc.classified_at ?? "—";
    const th =
      lc.threshold_delta_strength != null ? String(lc.threshold_delta_strength) : "—";
    const first = lc.is_first_snapshot ? " · primer snapshot (sin baseline previo)" : "";
    const n = Array.isArray(lc.new) ? lc.new.length : 0;
    const r = Array.isArray(lc.rising) ? lc.rising.length : 0;
    const f = Array.isArray(lc.fading) ? lc.fading.length : 0;
    summary.textContent = `Run ${run} · clasificado ${when} · umbral Δstrength ±${th}${first} · NEW ${n} · RISING ${r} · FADING ${f}`;
  } else {
    summary.textContent =
      "Lifecycle aún no generado; puedes ver el resumen del diff bajo la línea siguiente.";
  }

  if (dm && dm.counts) {
    const c = dm.counts;
    const src = payload.diff_source ? ` · ${payload.diff_source}` : "";
    diffLine.textContent = `Último diff: +${c.added ?? 0} añadidas / −${c.removed ?? 0} quitadas / ${c.changed ?? 0} con cambio de métricas${src}`;
  } else {
    diffLine.textContent = "";
  }

  const fill = (ul, items, makeRow) => {
    ul.replaceChildren();
    if (!items?.length) {
      const li = document.createElement("li");
      li.className = "evolution-list__empty";
      li.textContent = "—";
      ul.appendChild(li);
      return;
    }
    for (const it of items) {
      ul.appendChild(makeRow(it));
    }
  };

  const news = lc && Array.isArray(lc.new) ? lc.new.slice(0, EVOLUTION_MAX_NEW) : [];
  fill(ulN, news, (it) => {
    const li = document.createElement("li");
    li.className = "evolution-item";
    const name = document.createElement("span");
    name.className = "evolution-item__name";
    name.textContent = it.narrative || it.narrative_key || "—";
    li.appendChild(name);
    if (it.narrative_strength != null && it.narrative_strength !== "") {
      const meta = document.createElement("span");
      meta.className = "evolution-item__meta";
      meta.textContent = `str ${it.narrative_strength}`;
      li.appendChild(meta);
    }
    return li;
  });

  let rising = lc && Array.isArray(lc.rising) ? [...lc.rising] : [];
  rising.sort(
    (a, b) => (Number(b.delta_strength) || 0) - (Number(a.delta_strength) || 0),
  );
  rising = rising.slice(0, EVOLUTION_MAX_RISING);
  fill(ulR, rising, (it) => {
    const li = document.createElement("li");
    li.className = "evolution-item";
    const name = document.createElement("span");
    name.className = "evolution-item__name";
    name.textContent = it.narrative || it.narrative_key || "—";
    const d = document.createElement("span");
    d.className = "evolution-item__delta evolution-item__delta--up";
    const v = Number(it.delta_strength);
    d.textContent = Number.isFinite(v)
      ? v > 0
        ? `Δ +${v}`
        : `Δ ${v}`
      : "—";
    li.appendChild(name);
    li.appendChild(d);
    return li;
  });

  let fading = lc && Array.isArray(lc.fading) ? [...lc.fading] : [];
  fading.sort(
    (a, b) => (Number(a.delta_strength) || 0) - (Number(b.delta_strength) || 0),
  );
  fading = fading.slice(0, EVOLUTION_MAX_FADING);
  fill(ulF, fading, (it) => {
    const li = document.createElement("li");
    li.className = "evolution-item";
    const name = document.createElement("span");
    name.className = "evolution-item__name";
    name.textContent = it.narrative || it.narrative_key || "—";
    const d = document.createElement("span");
    d.className = "evolution-item__delta evolution-item__delta--down";
    const v = Number(it.delta_strength);
    d.textContent = Number.isFinite(v) ? `Δ ${v}` : "—";
    li.appendChild(name);
    li.appendChild(d);
    return li;
  });
}

function renderAlertsList(alertsPayload) {
  const el = $("alertsList");
  if (!el) return;

  const alerts = alertsPayload?.alerts || [];
  if (!alerts.length) {
    el.innerHTML = `<div class="alerts-strips alerts-strips--empty"><p class="alerts-empty">No alerts</p></div>`;
    return;
  }

  const newestFirst = [...alerts].reverse().filter(isRenderableAlert);
  const maxStrips = 20;
  const strips = newestFirst.slice(0, maxStrips).map(renderAlertStripHtml).join("");
  el.innerHTML = `<div class="alerts-strips">${strips}</div>`;
}

/** Opcional: comprobar que /articles/top responde (no es obligatorio para la UI). */
async function pingTop() {
  try {
    await fetchJson("/articles/top?sort_by=priority_score&limit=1");
  } catch {
    /* ignorar: latest ya es suficiente */
  }
}

function applyFiltersAndSort() {
  const sortBy = $("sortBy").value;
  const limit = Math.min(200, Math.max(1, Number($("limit").value) || 8));
  const bucket = $("filterBucket").value;
  const tag = $("filterTag").value;

  let list = rawArticles.filter((a) => {
    if (bucket && (a.score_bucket || "") !== bucket) return false;
    if (tag) {
      const tags = Array.isArray(a.topic_tags) ? a.topic_tags : [];
      if (!tags.includes(tag)) return false;
    }
    return true;
  });

  list = [...list].sort((a, b) => scoreNum(b, sortBy) - scoreNum(a, sortBy));
  list = list.slice(0, limit);
  return list;
}

function renderChips(items, className) {
  if (!items?.length) return "—";
  const wrap = document.createElement("div");
  wrap.className = "chips";
  for (const t of items) {
    const s = document.createElement("span");
    s.className = `chip ${className}`;
    s.textContent = t;
    wrap.appendChild(s);
  }
  return wrap;
}

function renderRow(a) {
  const tr = document.createElement("tr");

  const tdTitle = document.createElement("td");
  tdTitle.className = "title-cell";
  const strong = document.createElement("strong");
  strong.textContent = a.title || "(sin título)";
  tdTitle.appendChild(strong);
  const src = document.createElement("div");
  src.className = "source";
  src.textContent = a.source || "—";
  tdTitle.appendChild(src);
  if (a.url) {
    const link = document.createElement("a");
    link.href = a.url;
    link.target = "_blank";
    link.rel = "noreferrer";
    link.textContent = "Abrir URL";
    tdTitle.appendChild(link);
  }
  tr.appendChild(tdTitle);

  const tdPub = document.createElement("td");
  tdPub.className = "num";
  tdPub.textContent = a.published_at || "—";
  tr.appendChild(tdPub);

  for (const key of ["priority_score", "signal_score", "risk_score"]) {
    const td = document.createElement("td");
    td.className = "num";
    td.textContent = String(a[key] ?? "—");
    tr.appendChild(td);
  }

  const tdB = document.createElement("td");
  const span = document.createElement("span");
  span.className = "bucket";
  span.textContent = a.score_bucket || "—";
  tdB.appendChild(span);
  tr.appendChild(tdB);

  const tdTags = document.createElement("td");
  const tagsEl = renderChips(a.topic_tags, "");
  const narrEl = renderChips(a.narrative_candidates, "chip--narr");
  const stack = document.createElement("div");
  stack.style.display = "flex";
  stack.style.flexDirection = "column";
  stack.style.gap = "0.35rem";
  if (typeof tagsEl === "string") {
    stack.appendChild(document.createTextNode(tagsEl));
  } else {
    stack.appendChild(tagsEl);
  }
  if (typeof narrEl === "string") {
    const d = document.createElement("div");
    d.textContent = narrEl;
    stack.appendChild(d);
  } else {
    stack.appendChild(narrEl);
  }
  tdTags.appendChild(stack);
  tr.appendChild(tdTags);

  const tdDet = document.createElement("td");
  const det = document.createElement("details");
  det.className = "breakdown";
  const sum = document.createElement("summary");
  sum.textContent = "scoring_breakdown";
  det.appendChild(sum);
  const pre = document.createElement("pre");
  pre.textContent = JSON.stringify(a.scoring_breakdown ?? {}, null, 2);
  det.appendChild(pre);
  tdDet.appendChild(det);
  tr.appendChild(tdDet);

  return tr;
}

function render() {
  const tbody = $("tbody");
  tbody.innerHTML = "";
  const rows = applyFiltersAndSort();
  for (const a of rows) {
    tbody.appendChild(renderRow(a));
  }
}

function narrativeTypeClass(type) {
  const t = (type || "").toLowerCase();
  if (t === "early") return "early";
  if (t === "confirmed") return "confirmed";
  if (t === "institutional") return "institutional";
  return "unknown";
}

function narrativeTypeLabel(type) {
  const t = (type || "").toLowerCase();
  if (t === "early") return "🔥 early";
  if (t === "confirmed") return "⚠️ confirmed";
  if (t === "institutional") return "institutional";
  return type || "—";
}

/** Umbrales para overlay de señal (narrativas accionables). */
const SIGNAL_STRENGTH_EARLY = 20;
const SIGNAL_STRENGTH_CONFIRMED = 40;

/** Devuelve badge de señal accionable o null si no aplica. */
function narrativeSignalOverlay(n) {
  const type = (n.type || "").toLowerCase();
  const strength = scoreNum(n, "narrative_strength");

  if (type === "institutional") {
    return { label: "🧱 priced-in", cls: "narr-signal narr-signal--priced-in" };
  }
  if (type === "early" && strength > SIGNAL_STRENGTH_EARLY) {
    return { label: "🚀 opportunity", cls: "narr-signal narr-signal--opportunity" };
  }
  if (type === "confirmed" && strength > SIGNAL_STRENGTH_CONFIRMED) {
    return { label: "⚠️ momentum", cls: "narr-signal narr-signal--momentum" };
  }
  return null;
}

function narrativeSurgeOverlay(n) {
  if (!n || !state.surgeMap) return null;

  const name = (n.narrative || "").trim();
  return state.surgeMap.get(name) || null;
}

function decisionBadgeForNarrative(n) {
  const bucket = narrativeSurgeOverlay(n);
  if (bucket === "surge_200" || bucket === "surge_100") {
    return { label: "ACTIONABLE NOW", cls: "decision-badge decision-badge--action" };
  }
  if (bucket === "surge_50") {
    return { label: "WATCH CLOSELY", cls: "decision-badge decision-badge--watch" };
  }
  const overlay = narrativeSignalOverlay(n);
  const type = (n.type || "").toLowerCase();
  if (type === "early" && overlay?.cls?.includes("opportunity")) {
    return { label: "ACTIONABLE NOW", cls: "decision-badge decision-badge--action" };
  }
  if (type === "confirmed" && overlay?.cls?.includes("momentum")) {
    return { label: "WATCH CLOSELY", cls: "decision-badge decision-badge--watch" };
  }
  if (type === "institutional") {
    return { label: "PRICED IN", cls: "decision-badge decision-badge--priced" };
  }
  return { label: "PRICED IN", cls: "decision-badge decision-badge--priced" };
}

// surge_bucket desde /alerts/recent (últimos N ficheros fusionados)
function renderSurgeBadge(bucket) {
  if (!bucket) return "—";

  const map = {
    surge_50: { label: "🔥 +50%", cls: "narr-surge narr-surge--50" },
    surge_100: { label: "🔥🔥 +100%", cls: "narr-surge narr-surge--100" },
    surge_200: { label: "💣 +200%", cls: "narr-surge narr-surge--200" },
  };

  const cfg = map[bucket];
  if (!cfg) return "—";
  return `<span class="${cfg.cls}">${cfg.label}</span>`;
}

function sourcesLabel(n) {
  const rss = Number(n.rss_count) || 0;
  const red = Number(n.reddit_count) || 0;
  if (rss > 0 && red > 0) return "RSS + Reddit";
  if (rss > 0) return "RSS only";
  if (red > 0) return "Reddit only";
  return "—";
}

function narrativeInterpretation(n) {
  const t = (n.type || "").toLowerCase();
  const src = sourcesLabel(n);
  if (t === "early" && src === "Reddit only") {
    return "Early social-led narrative. Not yet confirmed by media.";
  }
  if (t === "confirmed" && src === "RSS + Reddit") {
    return "Cross-source confirmed narrative with active momentum.";
  }
  if (t === "institutional" && src === "RSS only") {
    return "Institutional narrative. Likely closer to priced-in.";
  }
  if (t === "early" && src === "RSS only") {
    return "Early narrative emerging via traditional media. Social validation still pending.";
  }
  if (t === "early" && src === "RSS + Reddit") {
    return "Early cross-source interest — watch for acceleration into confirmed territory.";
  }
  if (t === "confirmed" && src === "RSS only") {
    return "Media-led narrative gaining structure. Monitor for broader flow confirmation.";
  }
  if (t === "confirmed" && src === "Reddit only") {
    return "Community-led momentum in a confirmed bucket — validate against headlines.";
  }
  if (t === "institutional") {
    return "Institutional-grade framing — market may already reflect much of this story.";
  }
  return "Narrative under active scoring. Use strength, sources, and signals to size conviction.";
}

function renderNarrativeHero(list) {
  const host = $("narrativeHero");
  if (!host) return;

  host.replaceChildren();
  const arr = Array.isArray(list) ? list : [];
  const sorted = [...arr].sort(
    (a, b) => scoreNum(b, "narrative_strength") - scoreNum(a, "narrative_strength"),
  );
  const top = sorted[0];
  if (!top || typeof top !== "object") {
    const empty = document.createElement("div");
    empty.className = "narrative-hero narrative-hero--empty";
    const p = document.createElement("p");
    p.className = "narrative-hero__empty-text";
    p.textContent = "Sin narrativa destacada (carga datos o ejecuta detect_narratives).";
    empty.appendChild(p);
    host.appendChild(empty);
    return;
  }

  const card = document.createElement("div");
  card.className = "narrative-hero__card";

  const kicker = document.createElement("p");
  kicker.className = "narrative-hero__kicker";
  kicker.textContent = "Top narrative right now";
  card.appendChild(kicker);

  const decision = decisionBadgeForNarrative(top);
  const decEl = document.createElement("div");
  decEl.className = decision.cls;
  decEl.textContent = decision.label;
  card.appendChild(decEl);

  const name = document.createElement("h3");
  name.className = "narrative-hero__name";
  name.textContent = top.narrative || "—";
  card.appendChild(name);

  const interpret = document.createElement("p");
  interpret.className = "narrative-hero__interpret";
  interpret.textContent = narrativeInterpretation(top);
  card.appendChild(interpret);

  const strengthRow = document.createElement("div");
  strengthRow.className = "narrative-hero__strength-row";
  const strengthLabel = document.createElement("span");
  strengthLabel.className = "narrative-hero__strength-label";
  strengthLabel.textContent = "Strength";
  const strength = document.createElement("span");
  strength.className = "narrative-hero__strength";
  strength.textContent =
    top.narrative_strength != null ? String(top.narrative_strength) : "—";
  strengthRow.appendChild(strengthLabel);
  strengthRow.appendChild(strength);
  card.appendChild(strengthRow);

  const metaRow = document.createElement("div");
  metaRow.className = "narrative-hero__row";
  const typeSpan = document.createElement("span");
  typeSpan.className = `narrative-type narrative-type--${narrativeTypeClass(top.type)}`;
  typeSpan.textContent = narrativeTypeLabel(top.type);
  metaRow.appendChild(typeSpan);
  card.appendChild(metaRow);

  const sigRow = document.createElement("div");
  sigRow.className = "narrative-hero__signals";
  const overlay = narrativeSignalOverlay(top);
  if (overlay) {
    const sig = document.createElement("span");
    sig.className = overlay.cls;
    sig.textContent = overlay.label;
    sigRow.appendChild(sig);
  } else {
    const ph = document.createElement("span");
    ph.className = "narrative-hero__muted";
    ph.textContent = "Sin señal accionable";
    sigRow.appendChild(ph);
  }
  const surgeSlot = document.createElement("div");
  surgeSlot.className = "narrative-hero__surge-slot";
  const surgeHtml = renderSurgeBadge(narrativeSurgeOverlay(top));
  if (surgeHtml === "—") {
    const s = document.createElement("span");
    s.className = "narrative-hero__muted";
    s.textContent = "Surge: —";
    surgeSlot.appendChild(s);
  } else {
    surgeSlot.innerHTML = surgeHtml;
  }
  sigRow.appendChild(surgeSlot);
  card.appendChild(sigRow);

  const foot = document.createElement("div");
  foot.className = "narrative-hero__foot";
  const rss = Number(top.rss_count) || 0;
  const red = Number(top.reddit_count) || 0;
  foot.textContent = `Fuentes · RSS ${rss} · Reddit ${red} · ${sourcesLabel(top)}`;
  card.appendChild(foot);

  host.appendChild(card);
}

function renderNarrativesRows(list) {
  const tbody = $("narrativesTbody");
  tbody.innerHTML = "";
  const sorted = [...list].sort(
    (a, b) => scoreNum(b, "narrative_strength") - scoreNum(a, "narrative_strength"),
  );
  const top = sorted.slice(0, 10);
  top.forEach((n, i) => {
    const tr = document.createElement("tr");
    const tdRank = document.createElement("td");
    tdRank.className = "num";
    tdRank.textContent = String(i + 1);
    tr.appendChild(tdRank);

    const tdName = document.createElement("td");
    tdName.className = "narr-col-narrative";
    tdName.textContent = n.narrative || "—";
    tr.appendChild(tdName);

    const tdType = document.createElement("td");
    const span = document.createElement("span");
    const cls = narrativeTypeClass(n.type);
    span.className = `narrative-type narrative-type--${cls}`;
    span.textContent = narrativeTypeLabel(n.type);
    tdType.appendChild(span);
    tr.appendChild(tdType);

    const tdStr = document.createElement("td");
    tdStr.className = "num";
    tdStr.textContent =
      n.narrative_strength != null ? String(n.narrative_strength) : "—";
    tr.appendChild(tdStr);

    const tdSignal = document.createElement("td");
    const overlay = narrativeSignalOverlay(n);
    if (overlay) {
      const sig = document.createElement("span");
      sig.className = overlay.cls;
      sig.textContent = overlay.label;
      tdSignal.appendChild(sig);
    } else {
      tdSignal.className = "num";
      tdSignal.style.color = "var(--muted)";
      tdSignal.textContent = "—";
    }
    tr.appendChild(tdSignal);

    const tdSurge = document.createElement("td");
    tdSurge.innerHTML = renderSurgeBadge(narrativeSurgeOverlay(n));
    tr.appendChild(tdSurge);

    const tdSources = document.createElement("td");
    tdSources.className = "narrative-sources";
    tdSources.textContent = sourcesLabel(n);
    tr.appendChild(tdSources);

    tbody.appendChild(tr);
  });
}

async function loadAndRenderNarratives(preloadedData) {
  const meta = $("narrativesMeta");
  const tbody = $("narrativesTbody");
  try {
    const data =
      preloadedData !== undefined ? preloadedData : await loadNarrativesLatest();
    const list = Array.isArray(data.narratives) ? data.narratives : [];
    meta.textContent = `Fuente: ${data.source_file || "—"} · saved_at: ${data.saved_at ?? "—"} · total narrativas: ${data.narrative_count ?? list.length} · mostrando top 10`;
    renderNarrativeHero(list);
    renderNarrativesRows(list);
  } catch (e) {
    meta.textContent =
      e instanceof Error
        ? `Narrativas: ${e.message}`
        : "Narrativas: error al cargar.";
    renderNarrativeHero([]);
    tbody.innerHTML = "";
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 7;
    td.style.color = "var(--muted)";
    td.style.fontSize = "0.8rem";
    td.textContent =
      "Sin datos de narrativas. ¿API en marcha y ejecutaste detect_narratives.py?";
    tr.appendChild(td);
    tbody.appendChild(tr);
  }
}

async function reload() {
  showError("");
  $("meta").textContent = "Cargando…";
  $("narrativesMeta").textContent = "Cargando narrativas…";

  const alertsPayload = await loadAlertsRecent();
  state.surgeMap = buildSurgeMap(alertsPayload);
  renderAlertsList(alertsPayload);

  try {
    const [filters, latest, narrData, evolutionPayload] = await Promise.all([
      loadFilters(),
      loadLatest(),
      loadNarrativesLatest(),
      loadNarrativeHistoryLatest(),
    ]);
    filterOptions = filters;
    rawArticles = Array.isArray(latest.articles) ? latest.articles : [];
    populateSelect($("filterBucket"), filters.score_buckets ?? []);
    populateSelect($("filterTag"), filters.topic_tags ?? []);

    const src = latest.source_file || "";
    $("meta").textContent = `Fuente: ${src} · saved_at: ${latest.saved_at ?? "—"} · artículos: ${rawArticles.length}`;
    await pingTop();
    render();

    await loadAndRenderNarratives(narrData);
    renderNarrativeEvolution(evolutionPayload);
  } catch (e) {
    $("meta").textContent = "";
    showError(e instanceof Error ? e.message : String(e));
    await loadAndRenderNarratives();
    renderNarrativeEvolution(null);
  }
}

function updateDocsLink() {
  const a = $("docsLink");
  if (a) {
    a.href = `${apiBase()}/docs`;
  }
}

function wire() {
  updateDocsLink();
  $("apiBase").addEventListener("change", () => updateDocsLink());
  $("btnReload").addEventListener("click", () => reload());
  ["sortBy", "limit", "filterBucket", "filterTag"].forEach((id) => {
    $(id).addEventListener("change", () => render());
  });
  $("limit").addEventListener("input", () => render());
}

wire();
reload();
