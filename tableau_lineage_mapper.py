"""
Tableau Lineage Mapper
======================
Parses .twb and .twbx files to extract workbook → worksheet → Snowflake datasource lineage.
Outputs a CSV and a self-contained interactive HTML diagram.

Usage:
    python tableau_lineage_mapper.py --folder /path/to/your/twb/files
    python tableau_lineage_mapper.py --folder /path/to/your/twb/files --output my_report

Requirements:
    pip install pandas
    (All other libraries are built-in: xml, zipfile, os, argparse, json, pathlib)
"""

import os
import sys
import json
import zipfile
import argparse
import xml.etree.ElementTree as ET
from pathlib import Path
from collections import defaultdict

# ── Optional: pandas for CSV output (falls back to csv module if not installed) ──
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    import csv
    HAS_PANDAS = False


# ─────────────────────────────────────────────
#  CORE PARSING
# ─────────────────────────────────────────────

def get_twb_tree(filepath: Path):
    """
    Returns (ET.ElementTree, workbook_name).
    Handles both .twb (plain XML) and .twbx (ZIP archive).
    """
    suffix = filepath.suffix.lower()

    if suffix == ".twbx":
        with zipfile.ZipFile(filepath, "r") as zf:
            # Find the .twb inside the archive
            inner = [n for n in zf.namelist() if n.endswith(".twb")]
            if not inner:
                raise ValueError(f"No .twb found inside {filepath.name}")
            with zf.open(inner[0]) as f:
                tree = ET.parse(f)
    elif suffix == ".twb":
        tree = ET.parse(filepath)
    else:
        raise ValueError(f"Unsupported file type: {filepath.suffix}")

    return tree


def extract_datasource_info(ds_element):
    """
    Given a <datasource> XML element, return a dict with:
      - ds_name       : internal Tableau datasource name
      - ds_caption    : human-readable caption (if set)
      - db_type       : connection dbclass (e.g. 'snowflake')
      - server        : Snowflake account/server
      - database      : database name
      - schema        : schema name
      - table         : table or view name (or '[Custom SQL]')
      - custom_sql    : the SQL text if a custom SQL relation is used
    """
    info = {
        "ds_name": ds_element.get("name", ""),
        "ds_caption": ds_element.get("caption", ""),
        "db_type": "",
        "server": "",
        "database": "",
        "schema": "",
        "table": "",
        "custom_sql": "",
    }

    # Walk into <connection> — may be direct or wrapped in <named-connections>
    conn = ds_element.find(".//connection")
    if conn is not None:
        info["db_type"] = conn.get("class", conn.get("dbclass", "")).lower()
        info["server"]   = conn.get("server", "")
        info["database"] = conn.get("dbname", conn.get("database", ""))
        info["schema"]   = conn.get("schema", "")

    # Look for table relations
    relation = ds_element.find(".//relation[@type='table']")
    if relation is not None:
        info["table"] = relation.get("table", "").strip("[]")

    # Look for custom SQL relations
    custom = ds_element.find(".//relation[@type='text']")
    if custom is not None and custom.text:
        info["table"] = "[Custom SQL]"
        info["custom_sql"] = custom.text.strip()

    # Some workbooks nest relations differently — grab first named relation
    if not info["table"]:
        for rel in ds_element.findall(".//relation"):
            tbl = rel.get("table", "")
            if tbl:
                info["table"] = tbl.strip("[]")
                break

    return info


def parse_workbook(filepath: Path):
    """
    Main parser. Returns a list of row dicts:
      workbook, worksheet, ds_caption/ds_name, db_type,
      server, database, schema, table, custom_sql
    """
    tree = get_twb_tree(filepath)
    root = tree.getroot()
    workbook_name = filepath.stem
    rows = []

    # ── 1. Build datasource lookup: name → info dict ──
    datasources = {}
    for ds in root.findall(".//datasources/datasource"):
        name = ds.get("name", "")
        # Skip Tableau's built-in parameters datasource
        if name.lower() in ("parameters", ""):
            continue
        info = extract_datasource_info(ds)
        datasources[name] = info

    # ── 2. Walk worksheets and link to their datasource dependencies ──
    for ws in root.findall(".//worksheets/worksheet"):
        ws_name = ws.get("name", "Unknown Sheet")

        # Collect all datasource names referenced in this worksheet
        deps = set()
        for dep in ws.findall(".//datasource-dependencies"):
            ds_ref = dep.get("datasource", "")
            if ds_ref and ds_ref.lower() != "parameters":
                deps.add(ds_ref)

        # Also check direct datasource references in the view
        for dep in ws.findall(".//view/datasources/datasource"):
            ds_ref = dep.get("name", "")
            if ds_ref and ds_ref.lower() != "parameters":
                deps.add(ds_ref)

        if not deps:
            # Worksheet with no recognised datasource — still record it
            rows.append({
                "workbook":    workbook_name,
                "worksheet":   ws_name,
                "ds_label":    "(no datasource found)",
                "db_type":     "",
                "server":      "",
                "database":    "",
                "schema":      "",
                "table":       "",
                "custom_sql":  "",
            })
        else:
            for ds_ref in sorted(deps):
                ds_info = datasources.get(ds_ref, {})
                label = ds_info.get("ds_caption") or ds_info.get("ds_name") or ds_ref
                rows.append({
                    "workbook":   workbook_name,
                    "worksheet":  ws_name,
                    "ds_label":   label,
                    "db_type":    ds_info.get("db_type", ""),
                    "server":     ds_info.get("server", ""),
                    "database":   ds_info.get("database", ""),
                    "schema":     ds_info.get("schema", ""),
                    "table":      ds_info.get("table", ""),
                    "custom_sql": ds_info.get("custom_sql", ""),
                })

    return rows


# ─────────────────────────────────────────────
#  OUTPUT: CSV
# ─────────────────────────────────────────────

def write_csv(rows, output_path: Path):
    if HAS_PANDAS:
        pd.DataFrame(rows).to_csv(output_path, index=False)
    else:
        if not rows:
            print("No rows to write.")
            return
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    print(f"  ✓ CSV written → {output_path}")


# ─────────────────────────────────────────────
#  OUTPUT: INTERACTIVE HTML
# ─────────────────────────────────────────────

def build_html(rows, output_path: Path):
    """
    Builds a self-contained HTML file with:
    - A collapsible tree view (Workbook → Worksheet → Table)
    - A summary table with search/filter
    - All CSS/JS inline — no external dependencies
    """

    # ── Organise data into nested structure ──
    tree_data = defaultdict(lambda: defaultdict(list))
    for r in rows:
        tree_data[r["workbook"]][r["worksheet"]].append(r)

    # ── Build tree nodes as JSON for JS rendering ──
    nodes = []
    for wb, sheets in sorted(tree_data.items()):
        ws_nodes = []
        all_tables = set()
        for ws, sources in sorted(sheets.items()):
            src_nodes = []
            for s in sources:
                full_path = ".".join(filter(None, [s["database"], s["schema"], s["table"]]))
                src_nodes.append({
                    "label": s["ds_label"],
                    "table": full_path or s["table"] or "—",
                    "db_type": s["db_type"],
                    "server": s["server"],
                    "custom_sql": s["custom_sql"][:120] + "…" if len(s["custom_sql"]) > 120 else s["custom_sql"],
                })
                if full_path:
                    all_tables.add(full_path)
            ws_nodes.append({"name": ws, "sources": src_nodes})
        nodes.append({
            "workbook": wb,
            "sheets": ws_nodes,
            "table_count": len(all_tables),
            "sheet_count": len(sheets),
        })

    # ── Flat table rows for search view ──
    table_rows = []
    for r in rows:
        full_path = ".".join(filter(None, [r["database"], r["schema"], r["table"]]))
        table_rows.append({
            "workbook":  r["workbook"],
            "worksheet": r["worksheet"],
            "datasource": r["ds_label"],
            "db_type":   r["db_type"].upper() or "—",
            "full_path": full_path or r["table"] or "—",
            "custom_sql": "Yes" if r["custom_sql"] else "No",
        })

    data_json   = json.dumps(nodes, ensure_ascii=False)
    table_json  = json.dumps(table_rows, ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Tableau Lineage Map</title>
<style>
  :root {{
    --bg: #0f1117;
    --surface: #1a1d27;
    --surface2: #22263a;
    --border: #2e3348;
    --accent: #4f8ef7;
    --accent2: #a78bfa;
    --green: #34d399;
    --yellow: #fbbf24;
    --red: #f87171;
    --text: #e2e8f0;
    --text-muted: #8892a4;
    --font: 'Segoe UI', system-ui, sans-serif;
    --mono: 'Cascadia Code', 'Fira Code', monospace;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text); font-family: var(--font); font-size: 14px; min-height: 100vh; }}

  /* ── Header ── */
  header {{
    background: linear-gradient(135deg, #1a1d27 0%, #0f1117 100%);
    border-bottom: 1px solid var(--border);
    padding: 20px 32px;
    display: flex; align-items: center; gap: 16px;
  }}
  header h1 {{ font-size: 20px; font-weight: 700; letter-spacing: -0.3px; }}
  header h1 span {{ color: var(--accent); }}
  .pill {{ background: var(--surface2); border: 1px solid var(--border); border-radius: 99px;
           padding: 3px 12px; font-size: 12px; color: var(--text-muted); }}

  /* ── Tabs ── */
  .tabs {{ display: flex; gap: 0; border-bottom: 1px solid var(--border);
           padding: 0 32px; background: var(--surface); }}
  .tab {{ padding: 12px 20px; cursor: pointer; font-size: 13px; font-weight: 500;
          color: var(--text-muted); border-bottom: 2px solid transparent;
          transition: all .15s; user-select: none; }}
  .tab:hover {{ color: var(--text); }}
  .tab.active {{ color: var(--accent); border-bottom-color: var(--accent); }}

  /* ── Main layout ── */
  main {{ padding: 24px 32px; max-width: 1400px; }}
  .view {{ display: none; }}
  .view.active {{ display: block; }}

  /* ── Tree view ── */
  .workbook-card {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 10px; margin-bottom: 12px; overflow: hidden;
    transition: box-shadow .2s;
  }}
  .workbook-card:hover {{ box-shadow: 0 0 0 1px var(--accent); }}
  .wb-header {{
    display: flex; align-items: center; gap: 10px; padding: 14px 18px;
    cursor: pointer; user-select: none;
  }}
  .wb-icon {{ font-size: 18px; }}
  .wb-name {{ font-weight: 600; font-size: 15px; flex: 1; }}
  .wb-meta {{ color: var(--text-muted); font-size: 12px; display: flex; gap: 8px; }}
  .caret {{ color: var(--text-muted); transition: transform .2s; font-size: 12px; }}
  .wb-body {{ display: none; padding: 0 18px 14px; }}
  .wb-body.open {{ display: block; }}
  .wb-header.open .caret {{ transform: rotate(90deg); }}

  /* worksheets */
  .ws-row {{
    border-left: 2px solid var(--border); margin: 6px 0 6px 8px;
    padding-left: 14px;
  }}
  .ws-label {{
    display: flex; align-items: center; gap: 8px; padding: 5px 0;
    cursor: pointer; user-select: none;
  }}
  .ws-label:hover .ws-name {{ color: var(--accent2); }}
  .ws-name {{ font-size: 13px; font-weight: 500; }}
  .ws-caret {{ color: var(--text-muted); font-size: 11px; transition: transform .2s; }}
  .ws-label.open .ws-caret {{ transform: rotate(90deg); }}
  .ws-sources {{ display: none; padding-left: 18px; }}
  .ws-sources.open {{ display: block; }}

  /* source rows */
  .src-row {{
    display: grid;
    grid-template-columns: 18px 1fr auto;
    gap: 6px; align-items: start;
    padding: 6px 0; border-top: 1px solid var(--border);
  }}
  .src-dot {{ width: 8px; height: 8px; border-radius: 50%; background: var(--green); margin-top: 4px; flex-shrink: 0; }}
  .src-dot.sql {{ background: var(--yellow); }}
  .src-dot.unknown {{ background: var(--text-muted); }}
  .src-info {{ font-size: 12px; }}
  .src-label {{ font-weight: 500; color: var(--text); }}
  .src-path {{ color: var(--text-muted); font-family: var(--mono); font-size: 11px; margin-top: 2px; word-break: break-all; }}
  .src-sql-note {{ color: var(--yellow); font-size: 11px; font-style: italic; margin-top: 2px; }}
  .badge-db {{ background: var(--surface2); border: 1px solid var(--border);
               border-radius: 4px; padding: 1px 6px; font-size: 11px;
               color: var(--accent); white-space: nowrap; }}

  /* ── Table view ── */
  .search-bar {{
    display: flex; gap: 10px; margin-bottom: 16px; align-items: center;
  }}
  .search-bar input {{
    flex: 1; background: var(--surface); border: 1px solid var(--border);
    border-radius: 7px; padding: 9px 14px; color: var(--text);
    font-size: 13px; outline: none; transition: border-color .15s;
  }}
  .search-bar input:focus {{ border-color: var(--accent); }}
  .search-bar input::placeholder {{ color: var(--text-muted); }}
  select.filter {{
    background: var(--surface); border: 1px solid var(--border);
    color: var(--text); border-radius: 7px; padding: 8px 12px;
    font-size: 13px; outline: none; cursor: pointer;
  }}
  table {{ width: 100%; border-collapse: collapse; }}
  thead tr {{ background: var(--surface2); }}
  th {{ text-align: left; padding: 10px 14px; font-size: 12px; font-weight: 600;
        color: var(--text-muted); text-transform: uppercase; letter-spacing: .5px;
        border-bottom: 1px solid var(--border); white-space: nowrap; }}
  td {{ padding: 9px 14px; font-size: 13px; border-bottom: 1px solid var(--border);
        vertical-align: top; }}
  tr:hover td {{ background: var(--surface2); }}
  td.mono {{ font-family: var(--mono); font-size: 12px; color: var(--accent); }}
  td .chip {{
    display: inline-block; background: var(--surface2); border: 1px solid var(--border);
    border-radius: 4px; padding: 1px 7px; font-size: 11px; color: var(--accent2);
  }}
  .count {{ color: var(--text-muted); font-size: 12px; margin-bottom: 8px; }}

  /* ── Stats bar ── */
  .stats {{ display: flex; gap: 12px; margin-bottom: 24px; flex-wrap: wrap; }}
  .stat-card {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 14px 20px; flex: 1; min-width: 140px;
  }}
  .stat-card .num {{ font-size: 28px; font-weight: 700; color: var(--accent); }}
  .stat-card .lbl {{ font-size: 12px; color: var(--text-muted); margin-top: 2px; }}
</style>
</head>
<body>

<header>
  <span style="font-size:24px">📊</span>
  <h1>Tableau <span>Lineage</span> Map</h1>
  <span class="pill" id="wb-count-pill"></span>
</header>

<div class="tabs">
  <div class="tab active" onclick="switchTab('tree')">🌲 Tree View</div>
  <div class="tab" onclick="switchTab('table')">📋 Table View</div>
</div>

<main>

  <!-- Stats bar -->
  <div class="stats" id="stats"></div>

  <!-- Tree view -->
  <div class="view active" id="view-tree">
    <div id="tree-container"></div>
  </div>

  <!-- Table view -->
  <div class="view" id="view-table">
    <div class="search-bar">
      <input type="text" id="search-input" placeholder="Search workbook, worksheet, table…" oninput="filterTable()">
      <select class="filter" id="wb-filter" onchange="filterTable()">
        <option value="">All Workbooks</option>
      </select>
    </div>
    <div class="count" id="row-count"></div>
    <table id="lineage-table">
      <thead>
        <tr>
          <th>Workbook</th>
          <th>Worksheet</th>
          <th>Datasource</th>
          <th>DB Type</th>
          <th>Full Path (DB.Schema.Table)</th>
          <th>Custom SQL</th>
        </tr>
      </thead>
      <tbody id="table-body"></tbody>
    </table>
  </div>

</main>

<script>
const TREE  = {data_json};
const ROWS  = {table_json};

// ── Stats ──
function renderStats() {{
  const wbs     = new Set(ROWS.map(r => r.workbook)).size;
  const sheets  = new Set(ROWS.map(r => r.workbook + '|' + r.worksheet)).size;
  const tables  = new Set(ROWS.map(r => r.full_path).filter(t => t !== '—')).size;
  const sqls    = ROWS.filter(r => r.custom_sql === 'Yes').length;
  document.getElementById('wb-count-pill').textContent = wbs + ' workbooks';
  document.getElementById('stats').innerHTML = `
    <div class="stat-card"><div class="num">${{wbs}}</div><div class="lbl">Workbooks</div></div>
    <div class="stat-card"><div class="num">${{sheets}}</div><div class="lbl">Worksheets</div></div>
    <div class="stat-card"><div class="num">${{tables}}</div><div class="lbl">Unique Tables / Views</div></div>
    <div class="stat-card"><div class="num">${{sqls}}</div><div class="lbl">Custom SQL Sources</div></div>
  `;
}}

// ── Tree ──
function renderTree() {{
  const container = document.getElementById('tree-container');
  container.innerHTML = TREE.map((wb, wi) => `
    <div class="workbook-card">
      <div class="wb-header" onclick="toggleWB(this)" id="wbh-${{wi}}">
        <span class="wb-icon">📁</span>
        <span class="wb-name">${{wb.workbook}}</span>
        <span class="wb-meta">
          <span>${{wb.sheet_count}} sheet${{wb.sheet_count !== 1 ? 's' : ''}}</span>
          <span>·</span>
          <span>${{wb.table_count}} unique table${{wb.table_count !== 1 ? 's' : ''}}</span>
        </span>
        <span class="caret">▶</span>
      </div>
      <div class="wb-body" id="wbb-${{wi}}">
        ${{wb.sheets.map((ws, si) => `
          <div class="ws-row">
            <div class="ws-label" onclick="toggleWS(this)" id="wsl-${{wi}}-${{si}}">
              <span class="ws-caret">▶</span>
              <span class="ws-name">📄 ${{ws.name}}</span>
              <span style="color:var(--text-muted);font-size:11px;margin-left:6px">(${{ws.sources.length}} source${{ws.sources.length !== 1 ? 's' : ''}})</span>
            </div>
            <div class="ws-sources" id="wss-${{wi}}-${{si}}">
              ${{ws.sources.map(src => `
                <div class="src-row">
                  <span class="src-dot${{src.custom_sql ? ' sql' : src.table === '—' || !src.table ? ' unknown' : ''}}"></span>
                  <div class="src-info">
                    <div class="src-label">${{src.label}}</div>
                    <div class="src-path">${{src.table}}</div>
                    ${{src.custom_sql ? `<div class="src-sql-note">⚡ Custom SQL: ${{src.custom_sql}}</div>` : ''}}
                  </div>
                  ${{src.db_type ? `<span class="badge-db">${{src.db_type.toUpperCase()}}</span>` : ''}}
                </div>
              `).join('')}}
            </div>
          </div>
        `).join('')}}
      </div>
    </div>
  `).join('');
}}

function toggleWB(el) {{
  el.classList.toggle('open');
  const idx = el.id.split('-')[1];
  document.getElementById('wbb-' + idx).classList.toggle('open');
}}
function toggleWS(el) {{
  el.classList.toggle('open');
  const [,wi,si] = el.id.split('-');
  document.getElementById('wss-' + wi + '-' + si).classList.toggle('open');
}}

// ── Table ──
function renderTable(rows) {{
  const tbody = document.getElementById('table-body');
  if (!rows.length) {{
    tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--text-muted);padding:32px">No results found</td></tr>';
    document.getElementById('row-count').textContent = '0 rows';
    return;
  }}
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td><strong>${{r.workbook}}</strong></td>
      <td>${{r.worksheet}}</td>
      <td>${{r.datasource}}</td>
      <td><span class="chip">${{r.db_type}}</span></td>
      <td class="mono">${{r.full_path}}</td>
      <td style="color:${{r.custom_sql === 'Yes' ? 'var(--yellow)' : 'var(--text-muted)'}}">${{r.custom_sql}}</td>
    </tr>
  `).join('');
  document.getElementById('row-count').textContent = `${{rows.length}} row${{rows.length !== 1 ? 's' : ''}}`;
}}

function filterTable() {{
  const q  = document.getElementById('search-input').value.toLowerCase();
  const wb = document.getElementById('wb-filter').value;
  const filtered = ROWS.filter(r => {{
    const matchWB   = !wb || r.workbook === wb;
    const matchText = !q  || Object.values(r).some(v => String(v).toLowerCase().includes(q));
    return matchWB && matchText;
  }});
  renderTable(filtered);
}}

function populateWBFilter() {{
  const wbs = [...new Set(ROWS.map(r => r.workbook))].sort();
  const sel = document.getElementById('wb-filter');
  wbs.forEach(wb => {{
    const opt = document.createElement('option');
    opt.value = wb; opt.textContent = wb;
    sel.appendChild(opt);
  }});
}}

// ── Tabs ──
function switchTab(name) {{
  document.querySelectorAll('.tab').forEach((t, i) => {{
    t.classList.toggle('active', ['tree','table'][i] === name);
  }});
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  document.getElementById('view-' + name).classList.add('active');
}}

// ── Init ──
renderStats();
renderTree();
populateWBFilter();
renderTable(ROWS);
</script>
</body>
</html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  ✓ HTML report written → {output_path}")


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Map Tableau workbooks → worksheets → Snowflake datasources."
    )
    parser.add_argument(
        "--folder", required=True,
        help="Path to folder containing .twb / .twbx files"
    )
    parser.add_argument(
        "--output", default="tableau_lineage",
        help="Output filename prefix (default: tableau_lineage)"
    )
    args = parser.parse_args()

    folder = Path(args.folder)
    if not folder.exists():
        print(f"Error: folder '{folder}' does not exist.")
        sys.exit(1)

    files = sorted(list(folder.glob("*.twb")) + list(folder.glob("*.twbx")))
    if not files:
        print(f"No .twb or .twbx files found in '{folder}'.")
        sys.exit(1)

    print(f"\nFound {len(files)} workbook(s) in '{folder}'\n")

    all_rows = []
    for f in files:
        print(f"  Parsing: {f.name}")
        try:
            rows = parse_workbook(f)
            all_rows.extend(rows)
            print(f"    → {len(rows)} source-worksheet links found")
        except Exception as e:
            print(f"    ✗ Error parsing {f.name}: {e}")

    if not all_rows:
        print("\nNo data extracted. Check your workbook files.")
        sys.exit(1)

    print(f"\nTotal rows: {len(all_rows)}")

    out_dir = folder  # write outputs next to source files
    csv_path  = out_dir / f"{args.output}.csv"
    html_path = out_dir / f"{args.output}.html"

    write_csv(all_rows, csv_path)
    build_html(all_rows, html_path)

    print(f"\nDone! Open {html_path.name} in any browser for the visual report.")


if __name__ == "__main__":
    main()
