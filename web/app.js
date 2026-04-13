(function () {
  const DEFAULT_API = "http://127.0.0.1:8001";
  const params = new URLSearchParams(window.location.search);
  const API = (params.get("api") || DEFAULT_API).replace(/\/$/, "");

  function parseRoute() {
    const h = window.location.hash.replace(/^#/, "");
    const m = h.match(/^\/n\/([0-9a-fA-F-]{36})$/);
    if (m) return { view: "detail", id: m[1] };
    return { view: "list" };
  }

  function setHash(path) {
    window.location.hash = path;
  }

  async function fetchJson(url) {
    const r = await fetch(url, { headers: { Accept: "application/json" } });
    if (!r.ok) throw new Error((await r.text()) || r.statusText);
    return r.json();
  }

  function esc(s) {
    const d = document.createElement("div");
    d.textContent = s == null ? "" : String(s);
    return d.innerHTML;
  }

  function isSyntheticTitle(title) {
    return String(title ?? "").trim().startsWith("Muestra CP1");
  }

  function updateSynthToggleLabel() {
    const el = document.getElementById("synthToggleLabel");
    const on = document.getElementById("showSynthetic").checked;
    if (el) el.textContent = on ? "Show synthetic samples: ON" : "Show synthetic samples: OFF";
  }

  async function renderList(root) {
    const sort = document.getElementById("sortMode").value;
    const url =
      sort === "score"
        ? `${API}/narratives/top?limit=50&include_dormant=false`
        : `${API}/narratives?limit=50&offset=0&include_dormant=false`;
    root.innerHTML = '<div class="empty">Loading…</div>';
    let data;
    try {
      data = await fetchJson(url);
    } catch (e) {
      root.innerHTML = `<div class="empty msg err">API error: ${esc(e.message)}</div>`;
      return;
    }
    const raw = data.items || [];
    const showSynthetic = document.getElementById("showSynthetic").checked;
    const items = showSynthetic ? raw : raw.filter((n) => !isSyntheticTitle(n.title));
    if (!items.length) {
      root.innerHTML =
        raw.length && !showSynthetic
          ? '<div class="empty">No narratives in this view. Turn on “Show synthetic samples” to include titles starting with Muestra CP1.</div>'
          : '<div class="empty">No narratives.</div>';
      return;
    }
    const rows = items
      .map(
        (n) => `
      <tr data-id="${esc(n.id)}">
        <td class="title-cell">${esc(n.title || "—")}</td>
        <td class="num">${esc(n.score)}</td>
        <td class="num">${esc(n.state)}</td>
        <td class="num">${esc(n.item_count)}</td>
      </tr>`
      )
      .join("");
    root.innerHTML = `
      <div class="panel">
        <div class="panel__head">
          <h2 class="panel__title">[ NARRATIVES ]</h2>
        </div>
        <table>
          <thead><tr><th>Title</th><th>Score</th><th>State</th><th>Items</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>`;
    root.querySelectorAll("tbody tr").forEach((tr) => {
      tr.addEventListener("click", () => setHash(`/n/${tr.getAttribute("data-id")}`));
    });
  }

  async function postReview(narrativeId, verdict) {
    const body = {
      verdict,
      reason_code: "other",
      notes: `review-ui:${verdict}`,
      reviewer: "cristian",
    };
    const r = await fetch(`${API}/narratives/${narrativeId}/reviews`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify(body),
    });
    const txt = await r.text();
    if (!r.ok) throw new Error(txt || r.statusText);
    return JSON.parse(txt);
  }

  async function renderDetail(root, id) {
    root.innerHTML = '<div class="empty">Loading…</div>';
    let detail, items;
    try {
      [detail, items] = await Promise.all([
        fetchJson(`${API}/narratives/${id}`),
        fetchJson(`${API}/narratives/${id}/items`),
      ]);
    } catch (e) {
      root.innerHTML = `<div class="empty msg err">${esc(e.message)}</div>`;
      return;
    }
    const c = detail.current || {};
    const itemRows = (items.items || [])
      .map(
        (it) => `
      <div class="item-row">
        <div class="t">${esc(it.title)}</div>
        <div class="u">${it.url ? `<a href="${esc(it.url)}" target="_blank" rel="noopener">${esc(it.url)}</a>` : "—"}</div>
        <div class="u">${esc(it.source_name || "")}${it.published_at ? " · " + esc(it.published_at) : ""}</div>
      </div>`
      )
      .join("");
    root.innerHTML = `
      <div class="detail-wrap">
      <a href="#/" class="back">← Back to list</a>
      <div class="detail-head">
        <h2>${esc(c.title || "—")}</h2>
        <div class="pills">
          <span class="pill">score ${esc(c.score)}</span>
          <span class="pill">${esc(c.state)}</span>
          <span class="pill">${esc(c.item_count)} items</span>
          <span class="pill">trend ${esc(c.trend)}</span>
        </div>
      </div>
      <div class="items">
        <h3>Linked items</h3>
        ${itemRows || '<div class="muted">No items.</div>'}
      </div>
      <div class="review-bar">
        <button type="button" class="good" data-v="good">Good</button>
        <button type="button" class="bad" data-v="bad">Bad</button>
        <button type="button" class="unsure" data-v="unsure">Unsure</button>
      </div>
      <div id="reviewMsg" class="msg"></div>
      </div>`;
    const msg = root.querySelector("#reviewMsg");
    root.querySelectorAll(".review-bar button").forEach((btn) => {
      btn.addEventListener("click", async () => {
        msg.textContent = "Submitting…";
        msg.className = "msg";
        try {
          const out = await postReview(id, btn.getAttribute("data-v"));
          msg.textContent = `Review saved (id ${out.review_id}).`;
          msg.className = "msg ok";
        } catch (e) {
          msg.textContent = e.message || String(e);
          msg.className = "msg err";
        }
      });
    });
  }

  async function route() {
    const root = document.getElementById("app");
    const r = parseRoute();
    document.body.className = r.view === "list" ? "is-list" : "is-detail";
    document.getElementById("toolbar").style.display = r.view === "list" ? "flex" : "none";
    if (r.view === "list") await renderList(root);
    else await renderDetail(root, r.id);
  }

  document.getElementById("sortMode").addEventListener("change", () => route());
  document.getElementById("showSynthetic").addEventListener("change", () => {
    updateSynthToggleLabel();
    route();
  });
  window.addEventListener("hashchange", () => route());
  document.getElementById("apiHint").textContent = API;
  document.getElementById("showSynthetic").checked = false;
  updateSynthToggleLabel();
  route();
})();
