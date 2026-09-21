const state = {
  currentTable: "lexis_hotels",
  mappingCache: {},
  polling: null,
};

async function api(path, opts, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs || 25000);
  try {
    const res = await fetch(
      path,
      Object.assign({ headers: { "Content-Type": "application/json" }, signal: controller.signal }, opts)
    );
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.message || res.statusText);
    return data;
  } catch (e) {
    if (e.name === "AbortError") {
      throw new Error("Request timed out — check the server/network and try again.");
    }
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

function el(tag, attrs, children) {
  const node = document.createElement(tag);
  Object.entries(attrs || {}).forEach(([k, v]) => {
    if (k === "text") node.textContent = v;
    else if (k === "class") node.className = v;
    else node.setAttribute(k, v);
  });
  (children || []).forEach((c) => node.appendChild(c));
  return node;
}

async function loadConfig() {
  const input = document.getElementById("apiKeyInput");
  const status = document.getElementById("keyStatus");
  try {
    const cfg = await api("/api/config");
    input.value = cfg.airtable_api_key || "";
    input.placeholder = "Airtable API key";
  } catch (e) {
    status.textContent = "Could not load saved settings: " + e.message;
  }
}

document.getElementById("toggleKeyBtn").addEventListener("click", () => {
  const input = document.getElementById("apiKeyInput");
  const btn = document.getElementById("toggleKeyBtn");
  if (input.type === "password") {
    input.type = "text";
    btn.textContent = "Hide";
  } else {
    input.type = "password";
    btn.textContent = "Show";
  }
});

document.getElementById("saveKeyBtn").addEventListener("click", async () => {
  const value = document.getElementById("apiKeyInput").value.trim();
  const status = document.getElementById("keyStatus");
  if (!value) {
    status.textContent = "Enter an API key first.";
    return;
  }
  try {
    await api("/api/config/api-key", { method: "POST", body: JSON.stringify({ api_key: value }) });
    status.textContent = "Saved.";
    loadConfig();
  } catch (e) {
    status.textContent = "Error: " + e.message;
  }
});

document.getElementById("testBtn").addEventListener("click", async () => {
  const box = document.getElementById("testResult");
  box.innerHTML = "Testing…";
  const typed = document.getElementById("apiKeyInput").value.trim();
  try {
    const body = typed ? { api_key: typed } : {};
    const result = await api("/api/test-airtable", { method: "POST", body: JSON.stringify(body) }, 25000);
    box.innerHTML = "";
    Object.entries(result.tables || {}).forEach(([name, r]) => {
      box.appendChild(el("div", { class: "test-line " + (r.ok ? "ok" : "fail"), text: `${name}: ${r.message}` }));
    });
    if (!Object.keys(result.tables || {}).length) {
      box.appendChild(el("div", { class: "test-line fail", text: result.message || "No response" }));
    }
  } catch (e) {
    box.innerHTML = "";
    box.appendChild(el("div", { class: "test-line fail", text: e.message }));
  }
});

async function loadMapping(rdsTable) {
  const warning = document.getElementById("mappingWarning");
  warning.textContent = "Loading mapping…";
  try {
    const data = await api(`/api/mapping/${rdsTable}`, {}, 35000);
    state.mappingCache[rdsTable] = data;
    warning.textContent = data.warning ? "Warning: " + data.warning : "";
    renderMappingTable(rdsTable, data);
  } catch (e) {
    warning.textContent = "Error loading mapping: " + e.message;
  }
}

function renderMappingTable(rdsTable, data) {
  const tbody = document.querySelector("#mappingTable tbody");
  tbody.innerHTML = "";
  const overrides = data.overrides || {};
  const fields = data.airtable_fields || [];

  data.default_mapping.forEach((row) => {
    const tr = el("tr", {});
    tr.appendChild(el("td", { text: row.rds_column }));

    const mappedCell = el("td", {});
    if (row.editable) {
      const select = el("select", { "data-col": row.rds_column });
      select.appendChild(el("option", { value: "" }, [document.createTextNode("(default: " + (row.airtable_field || "none") + ")")]));
      const currentValue = overrides[row.rds_column] || "";
      fields.forEach((f) => {
        const opt = el("option", { value: f, text: f });
        if (f === currentValue) opt.selected = true;
        select.appendChild(opt);
      });
      if (!currentValue) select.value = "";
      mappedCell.appendChild(select);
    } else {
      mappedCell.appendChild(el("span", { class: "col-locked", text: (row.airtable_field || "—") + " (locked)" }));
    }
    tr.appendChild(mappedCell);
    tr.appendChild(el("td", { class: "note", text: row.note || "" }));
    tbody.appendChild(tr);
  });
}

document.getElementById("tableSelect").addEventListener("change", (e) => {
  state.currentTable = e.target.value;
  document.querySelector("#mappingTable tbody").innerHTML = "";
  document.getElementById("mappingWarning").textContent = "";
});

document.getElementById("fetchColumnsBtn").addEventListener("click", () => {
  loadMapping(state.currentTable);
});

document.getElementById("saveMappingBtn").addEventListener("click", async () => {
  const rdsTable = state.currentTable;
  const overrides = {};
  document.querySelectorAll("#mappingTable select[data-col]").forEach((sel) => {
    if (sel.value) overrides[sel.getAttribute("data-col")] = sel.value;
  });
  const warning = document.getElementById("mappingWarning");
  try {
    await api(`/api/mapping/${rdsTable}`, { method: "POST", body: JSON.stringify({ overrides }) });
    warning.textContent = "Mapping saved.";
  } catch (e) {
    warning.textContent = "Error saving mapping: " + e.message;
  }
});

document.getElementById("resetMappingBtn").addEventListener("click", async () => {
  const rdsTable = state.currentTable;
  try {
    await api(`/api/mapping/${rdsTable}`, { method: "POST", body: JSON.stringify({ overrides: {} }) });
    loadMapping(rdsTable);
  } catch (e) {
    document.getElementById("mappingWarning").textContent = "Error: " + e.message;
  }
});

const TABLE_LABELS = {
  scrapped_hotels: "Hotels",
  room_category: "Rooms",
};

function renderProgress(statusData) {
  const grid = document.getElementById("progressGrid");
  grid.innerHTML = "";
  Object.entries(statusData.tables).forEach(([name, t]) => {
    const card = el("div", { class: "progress-card" + (t.error ? " error" : "") });
    card.appendChild(el("h3", { text: TABLE_LABELS[name] || name }));
    card.appendChild(el("div", { class: "stage", text: t.stage || "idle" }));

    let pct = 0;
    if (t.stage === "done") pct = 100;
    else if (t.stage === "upserting") pct = 85;
    else if (t.stage === "transforming") pct = 65;
    else if (t.stage === "uploading_images") {
      pct = 40 + (t.images_total ? (t.images_done / t.images_total) * 20 : 0);
    } else if (t.stage === "fetched") pct = 35;
    else if (t.stage === "fetching") pct = 15;
    else if (t.stage === "connecting" || t.stage === "queued") pct = 5;

    const barBg = el("div", { class: "progress-bar-bg" });
    barBg.appendChild(el("div", { class: "progress-bar-fill", style: `width:${pct}%` }));
    card.appendChild(barBg);

    const stats = el("div", { class: "progress-stats" });
    stats.appendChild(el("span", { text: `Fetched: ${t.fetched || 0}` }));
    stats.appendChild(el("span", { text: `Images: ${t.images_uploaded || 0}` }));
    stats.appendChild(el("span", { text: `Upserted: ${t.upserted || 0}` }));
    card.appendChild(stats);

    if (t.error) card.appendChild(el("div", { class: "error-text", text: t.error }));

    grid.appendChild(card);
  });
}

async function pollStatus() {
  try {
    const status = await api("/api/migration/status");
    renderProgress(status);
    const logBox = document.getElementById("logBox");
    logBox.textContent = status.logs.join("\n");
    logBox.scrollTop = logBox.scrollHeight;

    const badge = document.getElementById("runningBadge");
    const buttons = [
      document.getElementById("startBtn"),
      document.getElementById("startHotelsBtn"),
      document.getElementById("startRoomsBtn"),
    ];
    if (status.running) {
      badge.classList.remove("hidden");
      buttons.forEach((b) => (b.disabled = true));
    } else {
      badge.classList.add("hidden");
      buttons.forEach((b) => (b.disabled = false));
      if (state.polling) {
        clearInterval(state.polling);
        state.polling = null;
      }
    }
  } catch (e) {
    console.error(e);
  }
}

async function startMigration(tables) {
  try {
    const body = tables ? { tables } : {};
    await api("/api/migration/start", { method: "POST", body: JSON.stringify(body) });
    if (!state.polling) {
      state.polling = setInterval(pollStatus, 1000);
    }
    pollStatus();
  } catch (e) {
    alert("Could not start migration: " + e.message);
  }
}

document.getElementById("startBtn").addEventListener("click", () => startMigration(null));
document.getElementById("startHotelsBtn").addEventListener("click", () => startMigration(["scrapped_hotels"]));
document.getElementById("startRoomsBtn").addEventListener("click", () => startMigration(["room_category"]));

loadConfig();
pollStatus();
