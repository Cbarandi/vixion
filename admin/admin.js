/**
 * Panel admin v1 — consume app.main FastAPI (puerto 8002 por defecto).
 */

const $ = (id) => document.getElementById(id);

let rawArticles = [];
let filterOptions = { score_buckets: [], topic_tags: [], narrative_candidates: [] };

const state = {
  surgeMap: new Map(),
  /** Datos crudos para re-aplicar filtros sin nuevo fetch. */
  narrativePanel: {
    evolutionPayload: null,
    outcomesPayload: null,
    edgePayload: null,
    narrData: null,
    timelinesPayload: null,
  },
};

/** Ficheros alerts_*.json a fusionar (debe coincidir con el default del API). */
const ALERTS_RECENT_FILES = 5;

const EVOLUTION_MAX_NEW = 12;
const EVOLUTION_MAX_RISING = 8;
const EVOLUTION_MAX_FADING = 8;
const TOP_MOVERS_LIMIT = 5;

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

/** Top movers desde el último diff (no lanza si falla la API). */
async function loadNarrativeDiffMoversLatest() {
  try {
    const res = await fetch(`${apiBase()}/narrative-history/diff-movers/latest`);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

/** Series de strength por narrativa (snapshots indexados; no lanza si falla). */
async function loadSnapshotTimelinesLatest() {
  try {
    const q = new URLSearchParams({ max_runs: "8", max_narratives: "6" });
    const res = await fetch(`${apiBase()}/narrative-history/snapshot-timelines/latest?${q}`);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

function normalizeNarrativeKeyLabel(label) {
  if (typeof label !== "string") return "";
  return label.trim().replace(/\s+/g, " ");
}

function lifecycleItemKey(it) {
  if (!it) return "";
  if (typeof it.narrative_key === "string" && it.narrative_key.trim()) return it.narrative_key.trim();
  return normalizeNarrativeKeyLabel(it.narrative);
}

function buildLifecycleKeySets(lc) {
  const out = { newSet: new Set(), risingSet: new Set(), fadingSet: new Set() };
  if (!lc) return out;
  const ingest = (arr, set) => {
    if (!Array.isArray(arr)) return;
    for (const it of arr) {
      const k = lifecycleItemKey(it);
      if (k) set.add(k);
    }
  };
  ingest(lc.new, out.newSet);
  ingest(lc.rising, out.risingSet);
  ingest(lc.fading, out.fadingSet);
  return out;
}

function readNarrativeFiltersFromDom() {
  const lcRaw = ($("narFilterLifecycle") && $("narFilterLifecycle").value) || "ALL";
  const lifecycle = ["ALL", "NEW", "RISING", "FADING"].includes(lcRaw) ? lcRaw : "ALL";
  const minOcc = Number($("narFilterMinOcc") && $("narFilterMinOcc").value);
  const minEdge = Number($("narFilterMinEdge") && $("narFilterMinEdge").value);
  const maxRows = Number($("narFilterMaxRows") && $("narFilterMaxRows").value);
  const sortRaw = ($("narFilterSort") && $("narFilterSort").value) || "occurrences";
  const sort = sortRaw === "edge" || sortRaw === "strength" ? sortRaw : "occurrences";
  return {
    lifecycle,
    minOccurrences: Number.isFinite(minOcc) ? Math.max(0, minOcc) : 0,
    minEdgeScore: Number.isFinite(minEdge) ? Math.max(0, minEdge) : 0,
    maxRows: Number.isFinite(maxRows) ? Math.min(50, Math.max(1, maxRows)) : 12,
    sort,
  };
}

function buildEdgeScoreByKey(edgePayload) {
  const map = new Map();
  const rk = edgePayload && edgePayload.ranking;
  const ranked = rk && Array.isArray(rk.ranked) ? rk.ranked : [];
  const all = rk && Array.isArray(rk.all_narratives) ? rk.all_narratives : [];
  for (const r of ranked) {
    if (!r || typeof r.narrative_key !== "string" || !r.narrative_key.trim()) continue;
    const s = Number(r.edge_score);
    if (Number.isFinite(s)) map.set(r.narrative_key.trim(), s);
  }
  for (const r of all) {
    if (!r || typeof r.narrative_key !== "string" || !r.narrative_key.trim()) continue;
    const kk = r.narrative_key.trim();
    if (map.has(kk)) continue;
    const s = Number(r.edge_score);
    if (Number.isFinite(s)) map.set(kk, s);
  }
  return map;
}

function buildStrengthByKeyFromNarratives(narrData) {
  const map = new Map();
  const list = narrData && Array.isArray(narrData.narratives) ? narrData.narratives : [];
  for (const row of list) {
    if (!row || typeof row.narrative !== "string") continue;
    const k = normalizeNarrativeKeyLabel(row.narrative);
    if (!k) continue;
    const s = Number(row.narrative_strength);
    if (Number.isFinite(s)) map.set(k, s);
  }
  return map;
}

function passesLifecycleFilter(narrativeKey, sets, mode, lc) {
  if (mode === "ALL" || !lc) return true;
  const k = narrativeKey || "";
  if (!k) return false;
  if (mode === "NEW") return sets.newSet.has(k);
  if (mode === "RISING") return sets.risingSet.has(k);
  if (mode === "FADING") return sets.fadingSet.has(k);
  return true;
}

function passesMinEdge(narrativeKey, minEdge, edgeByKey) {
  if (!(minEdge > 0)) return true;
  if (!edgeByKey || edgeByKey.size === 0) return true;
  const s = edgeByKey.get(narrativeKey);
  return typeof s === "number" && Number.isFinite(s) && s >= minEdge;
}

function sortNarrativeRows(rows, sort, edgeByKey, strengthByKey) {
  const keyOf = (r) => String(r.narrative_key || "");
  rows.sort((a, b) => {
    const ka = keyOf(a);
    const kb = keyOf(b);
    if (sort === "edge") {
      const va = edgeByKey.get(ka);
      const vb = edgeByKey.get(kb);
      const na = Number.isFinite(va) ? va : -Infinity;
      const nb = Number.isFinite(vb) ? vb : -Infinity;
      if (nb !== na) return nb - na;
    } else if (sort === "strength") {
      const va = strengthByKey.get(ka);
      const vb = strengthByKey.get(kb);
      const na = Number.isFinite(va) ? va : -Infinity;
      const nb = Number.isFinite(vb) ? vb : -Infinity;
      if (nb !== na) return nb - na;
    } else {
      const na = Number(a.occurrences) || 0;
      const nb = Number(b.occurrences) || 0;
      if (nb !== na) return nb - na;
    }
    return ka.localeCompare(kb);
  });
}

function applyNarrativePanelFilters() {
  const p = state.narrativePanel;
  if (!p) return;
  const f = readNarrativeFiltersFromDom();
  const lc =
    p.evolutionPayload &&
    p.evolutionPayload.lifecycle &&
    typeof p.evolutionPayload.lifecycle === "object"
      ? p.evolutionPayload.lifecycle
      : null;
  const sets = buildLifecycleKeySets(lc);
  const edgeByKey = buildEdgeScoreByKey(p.edgePayload);
  const strengthByKey = buildStrengthByKeyFromNarratives(p.narrData);
  renderNarrativeEdgeTop(p.edgePayload, lc, sets, f, edgeByKey);
  renderNarrativeOutcomesTable(p.outcomesPayload, p.edgePayload, lc, sets, f, edgeByKey, strengthByKey);
  renderMiniTimelines(p.timelinesPayload, lc, sets, f, edgeByKey, strengthByKey);
}

/** No lanza: devuelve null si no hay agregado o falla la API. */
async function loadNarrativeOutcomesLatest() {
  try {
    const res = await fetch(`${apiBase()}/outcomes/narrative-aggregates/latest`);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

/** No lanza: devuelve null si no hay ranking o falla la API. */
async function loadNarrativeEdgeLatest() {
  try {
    const res = await fetch(`${apiBase()}/outcomes/narrative-edge/latest`);
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

function formatRankPair(prev, curr) {
  const pr = Number(prev);
  const cr = Number(curr);
  const ps = Number.isFinite(pr) ? `#${pr}` : null;
  const cs = Number.isFinite(cr) ? `#${cr}` : null;
  if (!ps && !cs) return "";
  return `${ps ?? "—"}→${cs ?? "—"}`;
}

function fmtStrengthShort(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(2);
}

function renderTopMovers(moversPayload) {
  const block = $("topMoversBlock");
  const metaEl = $("topMoversMeta");
  const ulR = $("topMoversRising");
  const ulF = $("topMoversFalling");
  if (!block || !metaEl || !ulR || !ulF) return;

  const movers = moversPayload && moversPayload.movers;
  if (!movers || typeof movers !== "object") {
    block.hidden = true;
    ulR.replaceChildren();
    ulF.replaceChildren();
    metaEl.textContent = "";
    return;
  }

  const meta = movers.meta && typeof movers.meta === "object" ? movers.meta : {};
  const counts = meta.counts && typeof meta.counts === "object" ? meta.counts : {};
  const nChanged = Number(counts.changed);
  const risingN = Array.isArray(movers.rising) ? movers.rising.length : 0;
  const fallingN = Array.isArray(movers.falling) ? movers.falling.length : 0;
  if (Number.isFinite(nChanged) && nChanged === 0 && risingN === 0 && fallingN === 0) {
    block.hidden = true;
    ulR.replaceChildren();
    ulF.replaceChildren();
    metaEl.textContent = "";
    return;
  }

  block.hidden = false;
  const src = moversPayload.source_file ? ` · ${moversPayload.source_file}` : "";
  const parts = [];
  if (meta.diff_generated_at) parts.push(`diff @ ${meta.diff_generated_at}`);
  if (meta.current_run_id) parts.push(`run ${meta.current_run_id}`);
  parts.push(`changed ${counts.changed ?? 0}`);
  metaEl.textContent = parts.join(" · ") + src;

  const buildLi = (it, dir) => {
    const li = document.createElement("li");
    li.className = "top-movers__item";
    const row = document.createElement("div");
    row.className = "top-movers__row";
    const name = document.createElement("span");
    name.className = "top-movers__name";
    name.textContent = it.narrative || it.narrative_key || "—";
    const delta = document.createElement("span");
    delta.className = `top-movers__delta top-movers__delta--${dir}`;
    const d = Number(it.delta_strength);
    delta.textContent = Number.isFinite(d)
      ? d > 0
        ? `Δ +${d.toFixed(2)}`
        : `Δ ${d.toFixed(2)}`
      : "—";
    row.appendChild(name);
    row.appendChild(delta);
    li.appendChild(row);
    const detail = document.createElement("div");
    detail.className = "top-movers__detail";
    const rk = formatRankPair(it.previous_rank, it.current_rank);
    const bits = [`str ${fmtStrengthShort(it.current_strength)}`];
    if (rk) bits.push(`rnk ${rk}`);
    detail.textContent = bits.join(" · ");
    li.appendChild(detail);
    return li;
  };

  const fill = (ul, items, dir, emptyMsg) => {
    ul.replaceChildren();
    if (!items?.length) {
      const li = document.createElement("li");
      li.className = "top-movers__empty";
      li.textContent = emptyMsg;
      ul.appendChild(li);
      return;
    }
    const slice = items.slice(0, TOP_MOVERS_LIMIT);
    for (const it of slice) {
      ul.appendChild(buildLi(it, dir));
    }
  };

  fill(ulR, movers.rising, "up", "Sin subidas netas (Δ strength > 0).");
  fill(ulF, movers.falling, "down", "Sin bajadas netas (Δ strength < 0).");
}

function lastFiniteStrengthPoint(points) {
  if (!Array.isArray(points)) return null;
  for (let i = points.length - 1; i >= 0; i -= 1) {
    const p = points[i];
    if (p && Number.isFinite(Number(p.strength))) return p;
  }
  return null;
}

/** Sparkline SVG (sin librerías); huecos por run sin dato. */
function miniTimelineSparklineSvg(points, runLen) {
  const ns = "http://www.w3.org/2000/svg";
  const w = 80;
  const h = 24;
  const pad = 3;
  const svg = document.createElementNS(ns, "svg");
  svg.setAttribute("class", "mini-timelines__spark-svg");
  svg.setAttribute("viewBox", `0 0 ${w} ${h}`);
  svg.setAttribute("width", String(w));
  svg.setAttribute("height", String(h));
  svg.setAttribute("role", "img");
  svg.setAttribute("aria-hidden", "true");

  const vals = [];
  for (let i = 0; i < runLen; i += 1) {
    const p = points[i];
    const v = p && p.strength != null ? Number(p.strength) : null;
    vals.push(Number.isFinite(v) ? v : null);
  }
  const finite = vals.filter((x) => x != null);
  if (!finite.length) {
    const t = document.createElementNS(ns, "text");
    t.setAttribute("x", String(pad));
    t.setAttribute("y", String(h / 2 + 3));
    t.setAttribute("fill", "rgba(255,255,255,0.32)");
    t.setAttribute("font-size", "9");
    t.textContent = "—";
    svg.appendChild(t);
    return svg;
  }

  const minV = Math.min(...finite);
  const maxV = Math.max(...finite);
  const span = runLen > 1 ? w - 2 * pad : 0;
  const xAt = (i) => (runLen <= 1 ? w / 2 : pad + (span * i) / (runLen - 1));
  const yAt = (v) => {
    if (maxV === minV) return h / 2;
    const t = (v - minV) / (maxV - minV);
    return pad + (1 - t) * (h - 2 * pad);
  };

  let d = "";
  let pen = false;
  for (let i = 0; i < runLen; i += 1) {
    const v = vals[i];
    if (v == null) {
      pen = false;
      continue;
    }
    const x = xAt(i);
    const y = yAt(v);
    if (!pen) {
      d += `M${x.toFixed(2)},${y.toFixed(2)}`;
      pen = true;
    } else {
      d += `L${x.toFixed(2)},${y.toFixed(2)}`;
    }
  }
  if (d) {
    const path = document.createElementNS(ns, "path");
    path.setAttribute("d", d);
    path.setAttribute("fill", "none");
    path.setAttribute("stroke", "rgba(126, 200, 255, 0.9)");
    path.setAttribute("stroke-width", "1.35");
    path.setAttribute("stroke-linecap", "round");
    path.setAttribute("stroke-linejoin", "round");
    svg.appendChild(path);
  }
  const rDot = runLen <= 3 ? 2.25 : 1.7;
  for (let i = 0; i < runLen; i += 1) {
    const v = vals[i];
    if (v == null) continue;
    const c = document.createElementNS(ns, "circle");
    c.setAttribute("cx", String(xAt(i).toFixed(2)));
    c.setAttribute("cy", String(yAt(v).toFixed(2)));
    c.setAttribute("r", String(rDot));
    c.setAttribute("fill", "rgba(210, 235, 255, 0.95)");
    svg.appendChild(c);
  }
  return svg;
}

function renderMiniTimelines(tlPayload, lc, sets, f, edgeByKey, strengthByKey) {
  const block = $("miniTimelinesBlock");
  const metaEl = $("miniTimelinesMeta");
  const rowsEl = $("miniTimelinesRows");
  if (!block || !metaEl || !rowsEl) return;

  if (!tlPayload || typeof tlPayload !== "object") {
    block.hidden = true;
    rowsEl.replaceChildren();
    metaEl.textContent = "";
    return;
  }

  const runs = tlPayload.runs;
  let timelines = tlPayload.timelines;
  if (!Array.isArray(runs) || runs.length === 0 || !Array.isArray(timelines) || timelines.length === 0) {
    block.hidden = true;
    rowsEl.replaceChildren();
    metaEl.textContent = "";
    return;
  }

  if (f && sets) {
    timelines = timelines.filter((tl) => {
      const key = typeof tl.narrative_key === "string" ? tl.narrative_key.trim() : "";
      if (!key) return false;
      if (!passesLifecycleFilter(key, sets, f.lifecycle, lc)) return false;
      if (edgeByKey && !passesMinEdge(key, f.minEdgeScore, edgeByKey)) return false;
      return true;
    });
    timelines = [...timelines];
    const lastStr = (tl) => {
      const p = lastFiniteStrengthPoint(tl.points);
      const s = p && Number(p.strength);
      return Number.isFinite(s) ? s : -Infinity;
    };
    timelines.sort((a, b) => {
      const ka = String(a.narrative_key || "").trim();
      const kb = String(b.narrative_key || "").trim();
      if (f.sort === "edge") {
        const va = edgeByKey && edgeByKey.get(ka);
        const vb = edgeByKey && edgeByKey.get(kb);
        const na = Number.isFinite(va) ? va : -Infinity;
        const nb = Number.isFinite(vb) ? vb : -Infinity;
        if (nb !== na) return nb - na;
      } else if (f.sort === "strength") {
        const na = lastStr(a);
        const nb = lastStr(b);
        if (nb !== na) return nb - na;
      } else {
        const va = strengthByKey && strengthByKey.get(ka);
        const vb = strengthByKey && strengthByKey.get(kb);
        const na = Number.isFinite(va) ? va : lastStr(a);
        const nb = Number.isFinite(vb) ? vb : lastStr(b);
        if (nb !== na) return nb - na;
      }
      return ka.localeCompare(kb);
    });
    timelines = timelines.slice(0, f.maxRows);
  }

  if (!timelines.length) {
    block.hidden = true;
    rowsEl.replaceChildren();
    metaEl.textContent = "";
    return;
  }

  block.hidden = false;
  const idx = tlPayload.source_runs_index || "—";
  const filt =
    f && (f.lifecycle !== "ALL" || f.minOccurrences > 0 || f.minEdgeScore > 0) ? " · filtros" : "";
  metaEl.textContent = `${runs.length} corridas · ${timelines.length} narrativas${filt} · ${idx}`;

  rowsEl.replaceChildren();
  for (const tl of timelines) {
    const row = document.createElement("div");
    row.className = "mini-timelines__row";
    const lab = document.createElement("div");
    lab.className = "mini-timelines__label";
    lab.textContent = tl.narrative || tl.narrative_key || "—";
    lab.title = tl.narrative_key || "";
    const sparkWrap = document.createElement("div");
    sparkWrap.className = "mini-timelines__spark";
    sparkWrap.appendChild(miniTimelineSparklineSvg(tl.points || [], runs.length));
    const tail = document.createElement("div");
    tail.className = "mini-timelines__tail";
    const last = lastFiniteStrengthPoint(tl.points);
    if (last) {
      const s = Number(last.strength);
      const rk = last.rank != null && Number.isFinite(Number(last.rank)) ? `#${last.rank}` : "—";
      tail.textContent = Number.isFinite(s) ? `${s.toFixed(2)} ${rk}` : "—";
    } else {
      tail.textContent = "—";
    }
    row.appendChild(lab);
    row.appendChild(sparkWrap);
    row.appendChild(tail);
    rowsEl.appendChild(row);
  }
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

function fmtPct(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  return `${(n * 100).toFixed(1)}%`;
}

function fmtRet(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  const pct = n * 100;
  return `${pct >= 0 ? "+" : ""}${pct.toFixed(2)}%`;
}

function renderNarrativeEdgeTop(edgePayload, lc, sets, f, edgeByKey) {
  const el = $("outcomesEdgeStrip");
  if (!el) return;
  el.replaceChildren();
  const rk = edgePayload && edgePayload.ranking;
  const ranked = rk && Array.isArray(rk.ranked) ? [...rk.ranked] : [];
  if (!ranked.length) {
    el.hidden = true;
    return;
  }
  const filtered = ranked.filter((r) => {
    const key = typeof r.narrative_key === "string" ? r.narrative_key.trim() : "";
    if (!key) return false;
    if (!passesLifecycleFilter(key, sets, f.lifecycle, lc)) return false;
    const occ = Number(r.occurrences);
    if (!Number.isFinite(occ) || occ < f.minOccurrences) return false;
    if (!passesMinEdge(key, f.minEdgeScore, edgeByKey)) return false;
    return true;
  });
  sortNarrativeRows(filtered, f.sort, edgeByKey, new Map());
  const cap = Math.min(8, f.maxRows);
  const top = filtered.slice(0, cap);
  if (!top.length) {
    el.hidden = true;
    return;
  }
  el.hidden = false;
  const label = document.createElement("span");
  label.className = "outcomes-edge-strip__label";
  label.textContent = "Narrative edge (v1)";
  el.appendChild(label);
  for (let i = 0; i < top.length; i += 1) {
    const r = top[i];
    el.appendChild(document.createTextNode(i === 0 ? " · " : " · "));
    const score = Number(r.edge_score);
    const sc = Number.isFinite(score) ? score.toFixed(3) : "—";
    const span = document.createElement("span");
    span.textContent = `${r.narrative_key || "—"} (${sc})`;
    el.appendChild(span);
  }
}

function renderNarrativeOutcomesTable(
  payload,
  edgePayload,
  lc,
  sets,
  f,
  edgeByKey,
  strengthByKey,
) {
  const meta = $("outcomesMeta");
  const tbody = $("outcomesTbody");
  if (!meta || !tbody) return;

  tbody.innerHTML = "";
  const agg = payload && typeof payload.aggregate === "object" ? payload.aggregate : null;
  if (!agg || !Array.isArray(agg.narratives)) {
    meta.textContent =
      "Sin agregado de outcomes (ejecuta aggregate_narrative_outcomes en el pipeline).";
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 10;
    td.className = "outcomes-empty";
    td.textContent = "Sin datos.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  const total = agg.narratives.length;
  let rows = agg.narratives.filter((r) => {
    const key = typeof r.narrative_key === "string" ? r.narrative_key.trim() : "";
    if (!key) return false;
    if (!passesLifecycleFilter(key, sets, f.lifecycle, lc)) return false;
    const occ = Number(r.occurrences);
    if (!Number.isFinite(occ) || occ < f.minOccurrences) return false;
    if (!passesMinEdge(key, f.minEdgeScore, edgeByKey)) return false;
    return true;
  });
  rows = [...rows];
  sortNarrativeRows(rows, f.sort, edgeByKey, strengthByKey);
  rows = rows.slice(0, f.maxRows);

  const filterNote =
    f.lifecycle !== "ALL" || f.minOccurrences > 0 || f.minEdgeScore > 0 ? " · filtros activos" : "";
  meta.textContent =
    `Fuente: ${payload.source_file || "—"} · generated_at: ${agg.generated_at ?? "—"} · ` +
    `narrativas (agregado): ${total} · runs con outcomes: ` +
    `${agg.runs_with_forward_returns ?? 0}/${agg.runs_with_snapshots ?? 0} · ` +
    `mostrando ${rows.length}${filterNote}`;

  if (!rows.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 10;
    td.className = "outcomes-empty";
    td.textContent = "Sin filas con estos filtros.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  for (const r of rows) {
    const tr = document.createElement("tr");
    const cells = [
      r.narrative_key || "—",
      String(r.occurrences ?? "—"),
      fmtRet(r.avg_btc_return_1d),
      fmtRet(r.avg_btc_return_3d),
      fmtRet(r.avg_btc_return_7d),
      fmtPct(r.positive_rate_1d),
      fmtPct(r.positive_rate_3d),
      fmtPct(r.positive_rate_7d),
      String(r.runs_tagged_new ?? 0),
      String(r.runs_tagged_rising ?? 0),
    ];
    cells.forEach((text, i) => {
      const td = document.createElement("td");
      td.textContent = text;
      if (i > 0) td.className = "num";
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  }
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
  $("outcomesMeta").textContent = "Cargando outcomes…";

  const alertsPayload = await loadAlertsRecent();
  state.surgeMap = buildSurgeMap(alertsPayload);
  renderAlertsList(alertsPayload);

  try {
    const [
      filters,
      latest,
      narrData,
      evolutionPayload,
      moversPayload,
      timelinesPayload,
      outcomesPayload,
      edgePayload,
    ] = await Promise.all([
      loadFilters(),
      loadLatest(),
      loadNarrativesLatest(),
      loadNarrativeHistoryLatest(),
      loadNarrativeDiffMoversLatest(),
      loadSnapshotTimelinesLatest(),
      loadNarrativeOutcomesLatest(),
      loadNarrativeEdgeLatest(),
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
    renderTopMovers(moversPayload);
    state.narrativePanel = {
      evolutionPayload,
      outcomesPayload,
      edgePayload,
      narrData,
      timelinesPayload,
    };
    applyNarrativePanelFilters();
  } catch (e) {
    $("meta").textContent = "";
    showError(e instanceof Error ? e.message : String(e));
    await loadAndRenderNarratives();
    renderNarrativeEvolution(null);
    renderTopMovers(null);
    state.narrativePanel = {
      evolutionPayload: null,
      outcomesPayload: null,
      edgePayload: null,
      narrData: null,
      timelinesPayload: null,
    };
    applyNarrativePanelFilters();
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
  [
    "narFilterLifecycle",
    "narFilterMinOcc",
    "narFilterMinEdge",
    "narFilterMaxRows",
    "narFilterSort",
  ].forEach((id) => {
    const el = $(id);
    if (!el) return;
    el.addEventListener("change", () => applyNarrativePanelFilters());
    if (el.type === "number") el.addEventListener("input", () => applyNarrativePanelFilters());
  });
}

wire();
reload();
