/* Harvest Med Waste — Lead Dashboard */

(function () {
  "use strict";

  const STORAGE_KEY = "hmw_lead_overrides";
  const PAGE_SIZE = 50;

  let allLeads = [];
  let filtered = [];
  let currentPage = 1;
  let sortCol = "name";
  let sortDir = "asc";
  let expandedId = null;

  // ── Bootstrap ──────────────────────────────────────────────

  document.addEventListener("DOMContentLoaded", () => {
    loadLeads();
  });

  async function loadLeads() {
    try {
      const resp = await fetch("data/alabama_leads.json");
      if (!resp.ok) throw new Error(resp.statusText);
      allLeads = await resp.json();
    } catch (err) {
      document.getElementById("results-count").textContent =
        "Failed to load leads. Make sure data/alabama_leads.json exists.";
      console.error("Load error:", err);
      return;
    }

    // Merge localStorage overrides (status + notes)
    const overrides = loadOverrides();
    for (const lead of allLeads) {
      const o = overrides[lead.id];
      if (o) {
        if (o.status) lead.status = o.status;
        if (o.notes !== undefined) lead.notes = o.notes;
      }
    }

    populateFilters();
    bindEvents();
    applyFilters();
  }

  // ── localStorage persistence ───────────────────────────────

  function loadOverrides() {
    try {
      return JSON.parse(localStorage.getItem(STORAGE_KEY)) || {};
    } catch {
      return {};
    }
  }

  function saveOverride(id, field, value) {
    const overrides = loadOverrides();
    if (!overrides[id]) overrides[id] = {};
    overrides[id][field] = value;
    localStorage.setItem(STORAGE_KEY, JSON.stringify(overrides));
  }

  // ── Filters & Search ──────────────────────────────────────

  function populateFilters() {
    const types = [...new Set(allLeads.map((l) => l.facility_type))].sort();
    const cities = [...new Set(allLeads.map((l) => l.city).filter(Boolean))].sort();

    const typeSelect = document.getElementById("filter-type");
    for (const t of types) {
      typeSelect.appendChild(new Option(t, t));
    }

    const citySelect = document.getElementById("filter-city");
    for (const c of cities) {
      // Title-case display
      const display = c.replace(/\b\w/g, (ch) => ch.toUpperCase())
                       .replace(/\B\w+/g, (w) => w.toLowerCase());
      citySelect.appendChild(new Option(display, c));
    }
  }

  function bindEvents() {
    document.getElementById("search").addEventListener("input", debounce(applyFilters, 200));
    document.getElementById("filter-type").addEventListener("change", applyFilters);
    document.getElementById("filter-city").addEventListener("change", applyFilters);
    document.getElementById("filter-status").addEventListener("change", applyFilters);
    document.getElementById("export-btn").addEventListener("click", exportCSV);

    document.querySelectorAll("th.sortable").forEach((th) => {
      th.addEventListener("click", () => {
        const col = th.dataset.sort;
        if (sortCol === col) {
          sortDir = sortDir === "asc" ? "desc" : "asc";
        } else {
          sortCol = col;
          sortDir = "asc";
        }
        applyFilters();
      });
    });
  }

  function applyFilters() {
    const query = document.getElementById("search").value.trim().toLowerCase();
    const typeFilter = document.getElementById("filter-type").value;
    const cityFilter = document.getElementById("filter-city").value;
    const statusFilter = document.getElementById("filter-status").value;

    filtered = allLeads.filter((l) => {
      if (typeFilter && l.facility_type !== typeFilter) return false;
      if (cityFilter && l.city !== cityFilter) return false;
      if (statusFilter && l.status !== statusFilter) return false;
      if (query) {
        const haystack = (l.name + " " + l.city).toLowerCase();
        if (!haystack.includes(query)) return false;
      }
      return true;
    });

    // Sort
    filtered.sort((a, b) => {
      let va = (a[sortCol] || "").toString().toLowerCase();
      let vb = (b[sortCol] || "").toString().toLowerCase();
      if (va < vb) return sortDir === "asc" ? -1 : 1;
      if (va > vb) return sortDir === "asc" ? 1 : -1;
      return 0;
    });

    currentPage = 1;
    expandedId = null;
    renderStats();
    renderTable();
    renderPagination();
    updateSortArrows();
  }

  // ── Stats ──────────────────────────────────────────────────

  function renderStats() {
    const total = allLeads.length;
    const newWeek = allLeads.filter((l) => l.new_this_week).length;
    const contacted = allLeads.filter((l) => l.status === "Contacted").length;
    const interested = allLeads.filter((l) => l.status === "Interested").length;
    const closedWon = allLeads.filter((l) => l.status === "Closed Won").length;
    const worked = contacted + interested +
      allLeads.filter((l) => l.status === "Proposal Sent").length +
      closedWon +
      allLeads.filter((l) => l.status === "Not Interested").length;
    const rate = worked > 0 ? ((closedWon / worked) * 100).toFixed(1) + "%" : "—";

    document.getElementById("stat-total").textContent = total.toLocaleString();
    document.getElementById("stat-new").textContent = newWeek.toLocaleString();
    document.getElementById("stat-contacted").textContent = contacted.toLocaleString();
    document.getElementById("stat-interested").textContent = interested.toLocaleString();
    document.getElementById("stat-conversion").textContent = rate;
  }

  // ── Table ──────────────────────────────────────────────────

  function renderTable() {
    const tbody = document.getElementById("leads-body");
    tbody.innerHTML = "";

    const start = (currentPage - 1) * PAGE_SIZE;
    const page = filtered.slice(start, start + PAGE_SIZE);

    document.getElementById("results-count").textContent =
      `Showing ${start + 1}–${Math.min(start + PAGE_SIZE, filtered.length)} of ${filtered.length.toLocaleString()} leads`;

    for (const lead of page) {
      // Main row
      const tr = document.createElement("tr");
      tr.dataset.id = lead.id;
      if (lead.new_this_week) tr.classList.add("new-this-week");
      tr.addEventListener("click", () => toggleDetail(lead.id));

      tr.innerHTML = `
        <td>
          <div class="lead-name">
            ${escHtml(titleCase(lead.name))}
            ${lead.new_this_week ? '<span class="badge-new">NEW</span>' : ""}
          </div>
        </td>
        <td>${escHtml(lead.facility_type)}</td>
        <td>${escHtml(titleCase(lead.city))}</td>
        <td class="hide-mobile">${escHtml(lead.phone)}</td>
        <td>${statusPill(lead.status)}</td>
      `;
      tbody.appendChild(tr);

      // Detail row (hidden by default)
      if (expandedId === lead.id) {
        tbody.appendChild(buildDetailRow(lead));
      }
    }
  }

  function buildDetailRow(lead) {
    const tr = document.createElement("tr");
    tr.classList.add("detail-row");
    const td = document.createElement("td");
    td.colSpan = 5;

    const fullAddr = [lead.address, lead.city, lead.state, lead.zip]
      .filter(Boolean).join(", ");

    td.innerHTML = `
      <div class="detail-panel">
        <div class="detail-field">
          <label>Full Address</label>
          <span>${escHtml(titleCase(fullAddr))}</span>
        </div>
        <div class="detail-field">
          <label>Phone</label>
          <span>${lead.phone ? `<a href="tel:${lead.phone.replace(/\D/g, "")}">${escHtml(lead.phone)}</a>` : "—"}</span>
        </div>
        <div class="detail-field">
          <label>Fax</label>
          <span>${escHtml(lead.fax || "—")}</span>
        </div>
        <div class="detail-field">
          <label>NPI Number</label>
          <span>${escHtml(lead.npi_number)}</span>
        </div>
        <div class="detail-field">
          <label>Taxonomy Code</label>
          <span>${escHtml(lead.taxonomy_code)}</span>
        </div>
        <div class="detail-field">
          <label>Facility Type</label>
          <span>${escHtml(lead.facility_type)}</span>
        </div>
        <div class="detail-field">
          <label>Date Added</label>
          <span>${escHtml(lead.date_added)}</span>
        </div>
        <div class="detail-field">
          <label>Status</label>
          <select class="detail-status-select" data-id="${lead.id}">
            ${statusOptions(lead.status)}
          </select>
        </div>
        <div class="detail-notes">
          <label>Notes</label>
          <textarea data-id="${lead.id}" placeholder="Add notes about this lead...">${escHtml(lead.notes || "")}</textarea>
          <div class="save-hint">Auto-saves to browser storage</div>
        </div>
      </div>
    `;

    // Bind status change
    td.querySelector(".detail-status-select").addEventListener("change", (e) => {
      e.stopPropagation();
      const newStatus = e.target.value;
      lead.status = newStatus;
      saveOverride(lead.id, "status", newStatus);
      renderStats();
      renderTable();
    });

    // Bind notes change (debounced auto-save)
    const textarea = td.querySelector("textarea");
    textarea.addEventListener("click", (e) => e.stopPropagation());
    textarea.addEventListener("input", debounce((e) => {
      lead.notes = e.target.value;
      saveOverride(lead.id, "notes", e.target.value);
    }, 400));

    // Prevent row collapse when clicking inside detail
    td.addEventListener("click", (e) => e.stopPropagation());

    tr.appendChild(td);
    return tr;
  }

  function toggleDetail(id) {
    expandedId = expandedId === id ? null : id;
    renderTable();
  }

  // ── Pagination ─────────────────────────────────────────────

  function renderPagination() {
    const container = document.getElementById("pagination");
    container.innerHTML = "";
    const totalPages = Math.ceil(filtered.length / PAGE_SIZE);
    if (totalPages <= 1) return;

    const addBtn = (label, page, disabled) => {
      const btn = document.createElement("button");
      btn.textContent = label;
      btn.disabled = disabled;
      if (page === currentPage) btn.classList.add("active");
      btn.addEventListener("click", () => {
        currentPage = page;
        expandedId = null;
        renderTable();
        renderPagination();
        window.scrollTo({ top: 0, behavior: "smooth" });
      });
      container.appendChild(btn);
    };

    addBtn("\u2190", currentPage - 1, currentPage === 1);

    // Show limited page buttons around current page
    let startPage = Math.max(1, currentPage - 2);
    let endPage = Math.min(totalPages, currentPage + 2);

    if (startPage > 1) {
      addBtn("1", 1, false);
      if (startPage > 2) {
        const dots = document.createElement("span");
        dots.textContent = "...";
        dots.style.padding = "0 6px";
        dots.style.color = "#adb5bd";
        container.appendChild(dots);
      }
    }

    for (let p = startPage; p <= endPage; p++) {
      addBtn(String(p), p, false);
    }

    if (endPage < totalPages) {
      if (endPage < totalPages - 1) {
        const dots = document.createElement("span");
        dots.textContent = "...";
        dots.style.padding = "0 6px";
        dots.style.color = "#adb5bd";
        container.appendChild(dots);
      }
      addBtn(String(totalPages), totalPages, false);
    }

    addBtn("\u2192", currentPage + 1, currentPage === totalPages);
  }

  // ── Sort arrows ────────────────────────────────────────────

  function updateSortArrows() {
    document.querySelectorAll("th.sortable").forEach((th) => {
      th.classList.remove("sort-asc", "sort-desc");
      if (th.dataset.sort === sortCol) {
        th.classList.add(sortDir === "asc" ? "sort-asc" : "sort-desc");
      }
    });
  }

  // ── CSV Export ─────────────────────────────────────────────

  function exportCSV() {
    const headers = ["Name", "Facility Type", "Address", "City", "State", "ZIP",
                     "Phone", "Fax", "NPI", "Taxonomy", "Status", "Notes", "Date Added"];
    const rows = filtered.map((l) => [
      l.name, l.facility_type, l.address, l.city, l.state, l.zip,
      l.phone, l.fax, l.npi_number, l.taxonomy_code, l.status, l.notes, l.date_added,
    ]);

    let csv = headers.map(csvEsc).join(",") + "\n";
    for (const row of rows) {
      csv += row.map(csvEsc).join(",") + "\n";
    }

    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "harvest_leads_export.csv";
    a.click();
    URL.revokeObjectURL(url);
  }

  // ── Helpers ────────────────────────────────────────────────

  function escHtml(str) {
    if (!str) return "";
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
  }

  function titleCase(str) {
    if (!str) return "";
    return str.replace(/\b\w+/g, (w) =>
      w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()
    );
  }

  function csvEsc(val) {
    const s = String(val || "");
    if (s.includes(",") || s.includes('"') || s.includes("\n")) {
      return '"' + s.replace(/"/g, '""') + '"';
    }
    return s;
  }

  const STATUS_OPTIONS = [
    "New", "Contacted", "Interested", "Proposal Sent", "Closed Won", "Not Interested",
  ];

  function statusOptions(current) {
    return STATUS_OPTIONS.map(
      (s) => `<option value="${s}"${s === current ? " selected" : ""}>${s}</option>`
    ).join("");
  }

  function statusPill(status) {
    const cls = {
      New: "status-new",
      Contacted: "status-contacted",
      Interested: "status-interested",
      "Proposal Sent": "status-proposal-sent",
      "Closed Won": "status-closed-won",
      "Not Interested": "status-not-interested",
    }[status] || "status-new";
    return `<span class="status-pill ${cls}">${escHtml(status)}</span>`;
  }

  function debounce(fn, ms) {
    let timer;
    return function (...args) {
      clearTimeout(timer);
      timer = setTimeout(() => fn.apply(this, args), ms);
    };
  }
})();
