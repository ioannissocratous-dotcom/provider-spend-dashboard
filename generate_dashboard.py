#!/usr/bin/env python3
"""
Provider Spend Dashboard — Cyprus Bolt Food (GitHub Actions version)
Reads Databricks token from DATABRICKS_TOKEN_INCENTIVES env var.
Generates docs/index.html for GitHub Pages.
"""

import json
import os
import sys
import hashlib
import base64
from datetime import date, timedelta, datetime
from pathlib import Path
from calendar import monthrange

from databricks import sql
import pandas as pd
import re

SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "config.json"
OUTPUT_DIR = SCRIPT_DIR / "docs"
OUTPUT_PATH = OUTPUT_DIR / "index.html"

HOSTNAME = "bolt-incentives.cloud.databricks.com"
HTTP_PATH = "sql/protocolv1/o/2472566184436351/0221-081903-9ag4bh69"
ALLOWED_SQL_PREFIXES = ("select", "describe", "show", "with", "explain")


def get_token():
    token = os.environ.get("DATABRICKS_TOKEN_INCENTIVES")
    if not token:
        env_path = Path.home() / "databricks-setup" / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    if key.strip() == "DATABRICKS_TOKEN_INCENTIVES":
                        token = val.strip()
                        break
    if not token:
        raise RuntimeError("DATABRICKS_TOKEN_INCENTIVES not found in env or ~/.databricks-setup/.env")
    return token


def run_query(sql_text):
    token = get_token()
    stripped = sql_text.strip().lstrip("(")
    if not re.match(r"(?i)^(" + "|".join(ALLOWED_SQL_PREFIXES) + r")\b", stripped):
        raise ValueError(f"Only read queries allowed. Got: {sql_text[:80]}...")
    conn = sql.connect(server_hostname=HOSTNAME, http_path=HTTP_PATH, access_token=token)
    try:
        with conn.cursor() as cur:
            cur.execute(sql_text)
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)
    finally:
        conn.close()


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def get_periods():
    today = date.today()
    month_start = today.replace(day=1)
    days_in_month = monthrange(today.year, today.month)[1]
    week_start = today - timedelta(days=today.weekday())
    wtd_label = "WTD"
    if week_start >= today:
        week_start -= timedelta(days=7)
        wtd_label = "Last Week"
    if week_start < month_start:
        week_start = month_start
    return {
        "today": str(today),
        "month_start": str(month_start),
        "week_start": str(week_start),
        "wtd_label": wtd_label,
        "days_in_month": days_in_month,
        "days_elapsed": (today - month_start).days,
        "days_remaining": days_in_month - (today - month_start).days,
        "month_name": month_start.strftime("%B %Y"),
    }


def query_daily_overview(cfg, month_start, today):
    country = cfg["country"]
    obj_list = ", ".join(f"'{o}'" for o in cfg["objectives"])
    return run_query(f"""
    WITH food_campaigns AS (
        SELECT c.order_created_date, c.spend_objective, c.order_id,
               CAST(c.bolt_spend AS DOUBLE) AS bolt_spend
        FROM ng_public_spark.etl_delivery_campaign_order_metrics c
        JOIN ng_delivery_spark.dim_provider_v2 p ON c.provider_id = p.provider_id
        WHERE c.order_created_date >= '{month_start}' AND c.order_created_date < '{today}'
          AND c.country = '{country}'
          AND p.delivery_vertical IN ('food', 'store_3p_ent', 'store_3p_mm_smb')
          AND c.spend_objective IN ({obj_list})
    ),
    daily_gmv AS (
        SELECT order_created_date, SUM(gmv_eur) AS gmv
        FROM ng_public_spark.etl_delivery_order_monetary_metrics
        WHERE order_created_date >= '{month_start}' AND order_created_date < '{today}'
          AND country = '{country}'
        GROUP BY order_created_date
    ),
    daily_spend AS (
        SELECT order_created_date, spend_objective, SUM(bolt_spend) AS bolt_spend
        FROM food_campaigns GROUP BY order_created_date, spend_objective
    )
    SELECT ds.order_created_date AS dt, ds.spend_objective AS obj,
           ROUND(ds.bolt_spend, 2) AS spend, ROUND(dg.gmv, 2) AS gmv
    FROM daily_spend ds
    JOIN daily_gmv dg ON ds.order_created_date = dg.order_created_date
    ORDER BY ds.order_created_date, ds.spend_objective
    """)


def query_brand_spend(cfg, month_start, today):
    country = cfg["country"]
    obj_list = ", ".join(f"'{o}'" for o in cfg["objectives"])
    return run_query(f"""
    SELECT c.order_created_date AS dt,
           COALESCE(NULLIF(p.brand_name, ''), p.provider_name) AS brand,
           p.city_name AS city, c.spend_objective AS obj,
           ROUND(SUM(CAST(c.bolt_spend AS DOUBLE)), 2) AS spend
    FROM ng_public_spark.etl_delivery_campaign_order_metrics c
    JOIN ng_delivery_spark.dim_provider_v2 p ON c.provider_id = p.provider_id
    WHERE c.order_created_date >= '{month_start}' AND c.order_created_date < '{today}'
      AND c.country = '{country}'
      AND p.delivery_vertical IN ('food', 'store_3p_ent', 'store_3p_mm_smb')
      AND c.spend_objective IN ({obj_list})
    GROUP BY c.order_created_date, COALESCE(NULLIF(p.brand_name, ''), p.provider_name),
             p.city_name, c.spend_objective
    HAVING SUM(CAST(c.bolt_spend AS DOUBLE)) > 0
    ORDER BY spend DESC
    """)


def query_city_gmv(cfg, month_start, today):
    country = cfg["country"]
    return run_query(f"""
    SELECT m.order_created_date AS dt, dc.city_name AS city, ROUND(SUM(m.gmv_eur), 2) AS gmv
    FROM ng_public_spark.etl_delivery_order_monetary_metrics m
    JOIN ng_delivery_spark.dim_delivery_city dc ON m.city_id = dc.city_id
    WHERE m.order_created_date >= '{month_start}' AND m.order_created_date < '{today}'
      AND m.country = '{country}'
    GROUP BY m.order_created_date, dc.city_name
    ORDER BY m.order_created_date, dc.city_name
    """)


def df_to_records(df):
    return df.to_dict(orient="records")


def generate_html(overview_data, brand_data, city_gmv_data, cfg, periods):
    obj_labels = cfg["objective_labels"]
    colors = {
        "provider_campaign_marketing": "#3D7ABF",
        "provider_campaign_manual_churn_prevention": "#5B8C5A",
        "provider_campaign_locations": "#E07A5F",
        "provider_campaign_obligations_commitments": "#F2A93B",
        "provider_campaign_retail_growth": "#81B29A",
        "provider_campaign_commission_increase": "#9B72CF",
    }
    embedded = json.dumps({
        "overview": overview_data, "brands": brand_data, "cityGmv": city_gmv_data,
        "config": {"objectives": cfg["objectives"], "labels": obj_labels, "colors": colors,
                   "budget_pct": cfg["monthly_budget_pct_gmv"], "country": cfg["country"].upper()},
        "periods": periods,
    }, default=str)

    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Provider Spend Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<style>
:root {
  --bg: #f5f6fa; --surface: #ffffff; --surface2: #f0f1f5;
  --border: #e2e4ec; --text: #1a1d2e; --muted: #6b7085;
  --accent: #3D7ABF; --red: #D64545; --green: #2E8B57; --orange: #E07A5F;
}
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: var(--bg); color: var(--text); padding: 24px; }
.tab-bar { display:flex; gap:0; border-bottom:2px solid var(--border); margin-bottom:24px; }
.tab-btn { padding:12px 24px; font-size:14px; font-weight:600; cursor:pointer;
           border:none; background:none; color:var(--muted); border-bottom:2px solid transparent;
           margin-bottom:-2px; transition: all 0.15s; }
.tab-btn.active { color:var(--accent); border-bottom-color:var(--accent); }
.tab-btn:hover { color:var(--text); }
.tab-content { display:none; }
.tab-content.active { display:block; }
.header { display:flex; justify-content:space-between; align-items:center; margin-bottom:20px; }
.header h1 { font-size:20px; font-weight:700; }
.header .meta { color:var(--muted); font-size:12px; }
.cards { display:grid; grid-template-columns:repeat(4,1fr); gap:14px; margin-bottom:24px; }
.card { background:var(--surface); border:1px solid var(--border); border-radius:10px; padding:18px; }
.card .label { font-size:11px; color:var(--muted); text-transform:uppercase; letter-spacing:0.5px; margin-bottom:6px; }
.card .value { font-size:26px; font-weight:700; }
.card .sub-info { font-size:11px; color:var(--muted); margin-top:4px; }
.section { background:var(--surface); border:1px solid var(--border); border-radius:10px; padding:20px; margin-bottom:20px; }
.section h2 { font-size:15px; font-weight:600; margin-bottom:14px; }
.chart-container { position:relative; height:340px; }
.grid-2 { display:grid; grid-template-columns:1fr 1fr; gap:14px; margin-bottom:20px; }
table { width:100%; border-collapse:collapse; font-size:12px; }
th { text-align:left; padding:9px 10px; background:var(--surface2); color:var(--muted);
     font-size:10px; text-transform:uppercase; letter-spacing:0.5px; border-bottom:1px solid var(--border);
     position:sticky; top:0; z-index:1; }
td { padding:9px 10px; border-bottom:1px solid var(--border); }
tr:hover td { background: #f8f9fc; }
.sub { font-size:10px; color:var(--muted); }
.total-cell { font-weight:600; color:var(--accent); }
.text-right { text-align:right; }
.progress-bar { background:var(--surface2); border-radius:6px; height:22px; position:relative; overflow:hidden; margin:10px 0; }
.progress-fill { height:100%; border-radius:6px; transition:width 0.3s; }
.progress-fill.green { background: linear-gradient(90deg, #2E8B57, #81B29A); }
.progress-fill.orange { background: linear-gradient(90deg, #E07A5F, #F2A93B); }
.progress-marker { position:absolute; top:0; height:100%; width:2px; background:var(--text); opacity:0.4; }
.progress-label { position:absolute; top:50%; transform:translateY(-50%); right:8px; font-size:10px; font-weight:600; }
.pace-badge { display:inline-block; padding:2px 8px; border-radius:10px; font-size:10px; font-weight:600; margin-left:6px; }
.pace-over { background:rgba(224,122,95,0.12); color:var(--orange); }
.pace-under { background:rgba(46,139,87,0.12); color:var(--green); }
.filters { display:flex; flex-wrap:wrap; gap:12px; align-items:center; margin-bottom:18px; padding:14px; background:var(--surface2); border-radius:8px; }
.filter-group { display:flex; flex-direction:column; gap:4px; }
.filter-group label { font-size:10px; color:var(--muted); text-transform:uppercase; letter-spacing:0.4px; font-weight:600; }
.filter-group select, .filter-group input[type="number"], .filter-group input[type="date"] {
  padding:6px 10px; border:1px solid var(--border); border-radius:6px; font-size:12px;
  background:var(--surface); color:var(--text); min-width:130px; }
.checkbox-group { display:flex; flex-wrap:wrap; gap:8px; }
.checkbox-group label { font-size:12px; display:flex; align-items:center; gap:4px; cursor:pointer; }
.checkbox-group input { accent-color: var(--accent); }
.budget-input { display:flex; align-items:center; gap:6px; }
.budget-input input { width:70px; padding:6px 8px; border:1px solid var(--border); border-radius:6px;
                      font-size:13px; font-weight:600; text-align:right; }
.budget-input span { font-size:12px; color:var(--muted); }
.budget-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin-top:14px; }
.budget-item .bl { color:var(--muted); font-size:11px; }
.budget-item .bv { font-size:14px; font-weight:600; margin-top:2px; }
.table-scroll { max-height:500px; overflow-y:auto; border:1px solid var(--border); border-radius:8px; }
@media(max-width:900px) { .cards { grid-template-columns:1fr 1fr; } .grid-2 { grid-template-columns:1fr; } }
</style>
</head>
<body>
<div class="header">
  <h1 id="dashTitle">Provider Spend Dashboard</h1>
  <div class="meta" id="dashMeta"></div>
</div>
<div class="tab-bar">
  <button class="tab-btn active" data-tab="overview">Overview</button>
  <button class="tab-btn" data-tab="brands">Brand Breakdown</button>
</div>
<div id="tab-overview" class="tab-content active">
  <div class="filters">
    <div class="filter-group">
      <label>Budget (% of GMV)</label>
      <div class="budget-input">
        <input type="number" id="budgetInput" step="0.01" min="0" max="100">
        <span>%</span>
      </div>
    </div>
    <div class="filter-group">
      <label>Objectives</label>
      <div class="checkbox-group" id="objFilters"></div>
    </div>
  </div>
  <div class="cards" id="overviewCards"></div>
  <div class="section">
    <h2>Budget Progress</h2>
    <div id="budgetProgress"></div>
  </div>
  <div class="section">
    <h2>Daily Bolt Spend % of GMV by Objective</h2>
    <div class="chart-container"><canvas id="dailyChart"></canvas></div>
  </div>
  <div class="grid-2">
    <div class="section"><h2 id="mtdTitle">MTD Breakdown</h2><table id="mtdTable"></table></div>
    <div class="section"><h2 id="wtdTitle">WTD Breakdown</h2><table id="wtdTable"></table></div>
  </div>
  <div class="section">
    <h2>Daily Detail</h2>
    <div class="table-scroll"><table id="dailyTable"></table></div>
  </div>
</div>
<div id="tab-brands" class="tab-content">
  <div class="filters">
    <div class="filter-group">
      <label>Period</label>
      <select id="brandPeriod">
        <option value="mtd">MTD</option>
        <option value="wtd">WTD / Last Week</option>
        <option value="last7">Last 7 Days</option>
        <option value="yesterday">Yesterday</option>
        <option value="custom">Custom Range</option>
      </select>
    </div>
    <div class="filter-group" id="customDateGroup" style="display:none;">
      <label>From</label><input type="date" id="brandDateFrom">
    </div>
    <div class="filter-group" id="customDateGroupTo" style="display:none;">
      <label>To</label><input type="date" id="brandDateTo">
    </div>
    <div class="filter-group">
      <label>City</label>
      <select id="brandCity"><option value="all">All Cities</option></select>
    </div>
    <div class="filter-group">
      <label>Objective</label>
      <select id="brandObjFilter"><option value="all">All Objectives</option></select>
    </div>
  </div>
  <div class="cards" id="brandSummaryCards"></div>
  <div class="section">
    <h2 id="brandTableTitle">Brand Spend Breakdown</h2>
    <div class="table-scroll"><table id="brandTable"></table></div>
  </div>
</div>
<script>
const RAW = """ + embedded + """;
const D = RAW;
const CFG = D.config;
const P = D.periods;
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('tab-' + btn.dataset.tab).classList.add('active');
  });
});
const fmt = n => n.toLocaleString('en', {maximumFractionDigits:0});
const fmtPct = n => n.toFixed(2) + '%';
const fmtEur = n => '\\u20ac' + fmt(n);
document.getElementById('dashTitle').textContent = `Provider Spend Dashboard \\u2014 ${CFG.country} Bolt Food \\u2014 ${P.month_name}`;
document.getElementById('dashMeta').textContent = `Updated: ${P.today} | Data through ${P.today} (excl)`;
let dailyChart = null;
let selectedObjs = new Set(CFG.objectives);
let budgetPct = parseFloat(localStorage.getItem('spendBudgetPct') || CFG.budget_pct);
const objFiltersEl = document.getElementById('objFilters');
CFG.objectives.forEach(obj => {
  const lbl = document.createElement('label');
  const cb = document.createElement('input');
  cb.type = 'checkbox'; cb.checked = true; cb.value = obj;
  cb.addEventListener('change', () => {
    if (cb.checked) selectedObjs.add(obj); else selectedObjs.delete(obj);
    renderOverview();
  });
  lbl.appendChild(cb);
  lbl.appendChild(document.createTextNode(' ' + CFG.labels[obj]));
  objFiltersEl.appendChild(lbl);
});
const budgetInput = document.getElementById('budgetInput');
budgetInput.value = budgetPct;
budgetInput.addEventListener('input', () => {
  budgetPct = parseFloat(budgetInput.value) || 0;
  localStorage.setItem('spendBudgetPct', budgetPct);
  renderOverview();
});
function computeOverview() {
  const overview = D.overview;
  const dates = [...new Set(overview.map(r => r.dt))].sort();
  const gmvByDate = {};
  overview.forEach(r => { gmvByDate[r.dt] = r.gmv; });
  const spendByDateObj = {};
  overview.forEach(r => {
    if (!selectedObjs.has(r.obj)) return;
    if (!spendByDateObj[r.dt]) spendByDateObj[r.dt] = {};
    spendByDateObj[r.dt][r.obj] = (spendByDateObj[r.dt][r.obj] || 0) + r.spend;
  });
  let mtdSpend = 0, mtdGmv = 0;
  const mtdByObj = {}; CFG.objectives.forEach(o => mtdByObj[o] = 0);
  dates.forEach(dt => {
    mtdGmv += gmvByDate[dt] || 0;
    CFG.objectives.forEach(o => {
      if (!selectedObjs.has(o)) return;
      const s = (spendByDateObj[dt] || {})[o] || 0;
      mtdSpend += s; mtdByObj[o] += s;
    });
  });
  let wtdSpend = 0, wtdGmv = 0;
  const wtdByObj = {}; CFG.objectives.forEach(o => wtdByObj[o] = 0);
  dates.filter(dt => dt >= P.week_start).forEach(dt => {
    wtdGmv += gmvByDate[dt] || 0;
    CFG.objectives.forEach(o => {
      if (!selectedObjs.has(o)) return;
      const s = (spendByDateObj[dt] || {})[o] || 0;
      wtdSpend += s; wtdByObj[o] += s;
    });
  });
  const mtdPct = mtdGmv ? 100 * mtdSpend / mtdGmv : 0;
  const wtdPct = wtdGmv ? 100 * wtdSpend / wtdGmv : 0;
  const daysElapsed = P.days_elapsed || 1;
  const avgDailyGmv = mtdGmv / Math.max(daysElapsed, 1);
  const projectedGmv = avgDailyGmv * P.days_in_month;
  const budgetTotalEur = budgetPct / 100 * projectedGmv;
  const remainingEur = budgetTotalEur - mtdSpend;
  const dailyReqEur = P.days_remaining > 0 ? remainingEur / P.days_remaining : 0;
  const dailyReqPct = avgDailyGmv ? 100 * dailyReqEur / avgDailyGmv : 0;
  return { dates, gmvByDate, spendByDateObj, mtdSpend, mtdGmv, mtdPct, mtdByObj,
           wtdSpend, wtdGmv, wtdPct, wtdByObj, projectedGmv, budgetTotalEur,
           remainingEur, dailyReqEur, dailyReqPct, avgDailyGmv };
}
function renderOverview() {
  const c = computeOverview();
  const progressPct = budgetPct ? Math.min(100, c.mtdPct / budgetPct * 100) : 0;
  const timePct = P.days_in_month ? P.days_elapsed / P.days_in_month * 100 : 0;
  const overPacing = (c.mtdPct / Math.max(budgetPct, 0.001)) > (P.days_elapsed / Math.max(P.days_in_month, 1));
  document.getElementById('overviewCards').innerHTML = `
    <div class="card"><div class="label">MTD Bolt Spend % GMV</div>
      <div class="value">${fmtPct(c.mtdPct)}</div>
      <div class="sub-info">${fmtEur(c.mtdSpend)} / ${fmtEur(c.mtdGmv)} GMV \\u00b7 ${P.days_elapsed}d</div></div>
    <div class="card"><div class="label">${P.wtd_label} Bolt Spend % GMV</div>
      <div class="value">${fmtPct(c.wtdPct)}</div>
      <div class="sub-info">${fmtEur(c.wtdSpend)} / ${fmtEur(c.wtdGmv)} GMV</div></div>
    <div class="card"><div class="label">Budget Pace</div>
      <div class="value">${progressPct.toFixed(0)}% <span class="pace-badge ${overPacing?'pace-over':'pace-under'}">${overPacing?'Over-pacing':'Under-pacing'}</span></div>
      <div class="sub-info">${timePct.toFixed(0)}% of month elapsed</div></div>
    <div class="card"><div class="label">Daily Required</div>
      <div class="value">${fmtPct(c.dailyReqPct)}</div>
      <div class="sub-info">\\u2248 ${fmtEur(c.dailyReqEur)}/day for ${P.days_remaining}d left</div></div>`;
  const fillClass = progressPct > 100 ? 'orange' : 'green';
  document.getElementById('budgetProgress').innerHTML = `
    <div class="progress-bar">
      <div class="progress-fill ${fillClass}" style="width:${Math.min(progressPct,100).toFixed(1)}%"></div>
      <div class="progress-marker" style="left:${timePct.toFixed(1)}%"></div>
      <div class="progress-label">${fmtPct(c.mtdPct)} / ${fmtPct(budgetPct)}</div>
    </div>
    <div class="budget-grid">
      <div class="budget-item"><div class="bl">Projected Month GMV</div><div class="bv">${fmtEur(c.projectedGmv)}</div></div>
      <div class="budget-item"><div class="bl">Budget Total (est.)</div><div class="bv">${fmtEur(c.budgetTotalEur)}</div></div>
      <div class="budget-item"><div class="bl">Spent So Far</div><div class="bv">${fmtEur(c.mtdSpend)}</div></div>
      <div class="budget-item"><div class="bl">Remaining</div><div class="bv">${fmtEur(c.remainingEur)}</div></div>
    </div>`;
  const activeObjs = CFG.objectives.filter(o => selectedObjs.has(o));
  const datasets = activeObjs.map(obj => ({
    label: CFG.labels[obj],
    data: c.dates.map(dt => {
      const gmv = c.gmvByDate[dt] || 1;
      const spend = (c.spendByDateObj[dt] || {})[obj] || 0;
      return parseFloat((100 * spend / gmv).toFixed(2));
    }),
    backgroundColor: CFG.colors[obj],
  }));
  if (dailyChart) dailyChart.destroy();
  dailyChart = new Chart(document.getElementById('dailyChart').getContext('2d'), {
    type: 'bar', data: { labels: c.dates, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      scales: {
        x: { stacked:true, grid:{color:'#e2e4ec'}, ticks:{color:'#6b7085', font:{size:10}} },
        y: { stacked:true, grid:{color:'#e2e4ec'}, ticks:{color:'#6b7085', callback:v=>v.toFixed(2)+'%', font:{size:10}},
             title:{display:true, text:'Bolt Spend % of GMV', color:'#6b7085'} }
      },
      plugins: {
        legend: { position:'bottom', labels:{color:'#1a1d2e', padding:14, font:{size:11}} },
        tooltip: { callbacks: {
          label: ctx => ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(2) + '%',
          afterBody: items => { const t = items.reduce((s,i)=>s+i.parsed.y,0); return 'Total: '+t.toFixed(2)+'%'; }
        }}
      }
    },
    plugins: [{
      id:'budgetLine',
      afterDraw(chart) {
        if (!budgetPct) return;
        const y = chart.scales.y.getPixelForValue(budgetPct);
        const ctx2 = chart.ctx;
        ctx2.save(); ctx2.beginPath(); ctx2.setLineDash([6,4]);
        ctx2.strokeStyle='#D64545'; ctx2.lineWidth=2;
        ctx2.moveTo(chart.chartArea.left, y); ctx2.lineTo(chart.chartArea.right, y);
        ctx2.stroke();
        ctx2.fillStyle='#D64545'; ctx2.font='11px -apple-system,sans-serif';
        ctx2.fillText('Budget: '+budgetPct.toFixed(2)+'%', chart.chartArea.right-110, y-6);
        ctx2.restore();
      }
    }]
  });
  function objTable(byObj, totalPct, gmv) {
    let html = '<tr><th>Objective</th><th class="text-right">% of GMV</th><th class="text-right">EUR</th></tr>';
    const activeEntries = activeObjs.map(o => [CFG.labels[o], byObj[o]]).filter(([,v]) => v !== undefined);
    activeEntries.sort((a,b) => b[1]-a[1]);
    activeEntries.forEach(([lbl, spend]) => {
      const pct = gmv ? 100*spend/gmv : 0;
      html += `<tr><td>${lbl}</td><td class="text-right">${fmtPct(pct)}</td><td class="text-right">${fmtEur(spend)}</td></tr>`;
    });
    html += `<tr class="total-cell"><td><strong>Total</strong></td><td class="text-right"><strong>${fmtPct(totalPct)}</strong></td><td class="text-right"><strong>${fmtEur(activeEntries.reduce((s,e)=>s+e[1],0))}</strong></td></tr>`;
    return html;
  }
  document.getElementById('mtdTitle').textContent = `MTD Breakdown \\u2014 ${fmtPct(c.mtdPct)}`;
  document.getElementById('mtdTable').innerHTML = objTable(c.mtdByObj, c.mtdPct, c.mtdGmv);
  document.getElementById('wtdTitle').textContent = `${P.wtd_label} Breakdown \\u2014 ${fmtPct(c.wtdPct)}`;
  document.getElementById('wtdTable').innerHTML = objTable(c.wtdByObj, c.wtdPct, c.wtdGmv);
  const objHeaders = activeObjs.map(o => `<th class="text-right">${CFG.labels[o]}</th>`).join('');
  let dtRows = '';
  c.dates.forEach(dt => {
    const gmv = c.gmvByDate[dt] || 0;
    let totalSpend = 0;
    let cells = `<td>${dt}</td><td class="text-right">${fmtEur(gmv)}</td>`;
    activeObjs.forEach(o => {
      const s = (c.spendByDateObj[dt]||{})[o]||0;
      totalSpend += s;
      const pct = gmv ? 100*s/gmv : 0;
      cells += `<td class="text-right">${fmtPct(pct)}<br><span class="sub">${fmtEur(s)}</span></td>`;
    });
    const totalPct = gmv ? 100*totalSpend/gmv : 0;
    cells += `<td class="text-right total-cell">${fmtPct(totalPct)}<br><span class="sub">${fmtEur(totalSpend)}</span></td>`;
    dtRows += `<tr>${cells}</tr>`;
  });
  document.getElementById('dailyTable').innerHTML =
    `<tr><th>Date</th><th class="text-right">GMV</th>${objHeaders}<th class="text-right total-cell">Total</th></tr>${dtRows}`;
}
const allCities = [...new Set(D.brands.map(r => r.city))].filter(Boolean).sort();
const citySelect = document.getElementById('brandCity');
allCities.forEach(c => { const o = document.createElement('option'); o.value=c; o.textContent=c; citySelect.appendChild(o); });
const brandObjSelect = document.getElementById('brandObjFilter');
CFG.objectives.forEach(o => { const opt = document.createElement('option'); opt.value=o; opt.textContent=CFG.labels[o]; brandObjSelect.appendChild(opt); });
const brandPeriodEl = document.getElementById('brandPeriod');
brandPeriodEl.addEventListener('change', () => {
  const show = brandPeriodEl.value === 'custom';
  document.getElementById('customDateGroup').style.display = show ? '' : 'none';
  document.getElementById('customDateGroupTo').style.display = show ? '' : 'none';
  renderBrands();
});
document.getElementById('brandDateFrom').value = P.month_start;
document.getElementById('brandDateTo').value = P.today;
[citySelect, brandObjSelect, document.getElementById('brandDateFrom'), document.getElementById('brandDateTo')].forEach(el => {
  el.addEventListener('change', renderBrands);
});
function getBrandDateRange() {
  const mode = brandPeriodEl.value;
  const today = P.today;
  const yesterday = new Date(new Date(today).getTime() - 86400000).toISOString().slice(0,10);
  const last7 = new Date(new Date(today).getTime() - 7*86400000).toISOString().slice(0,10);
  switch(mode) {
    case 'mtd': return [P.month_start, today];
    case 'wtd': return [P.week_start, today];
    case 'last7': return [last7, today];
    case 'yesterday': return [yesterday, today];
    case 'custom': return [document.getElementById('brandDateFrom').value, document.getElementById('brandDateTo').value];
    default: return [P.month_start, today];
  }
}
function renderBrands() {
  const [startDt, endDt] = getBrandDateRange();
  const cityFilter = citySelect.value;
  const objFilter = brandObjSelect.value;
  let filtered = D.brands.filter(r =>
    r.dt >= startDt && r.dt < endDt &&
    (cityFilter === 'all' || r.city === cityFilter) &&
    (objFilter === 'all' || r.obj === objFilter)
  );
  const brandAgg = {};
  filtered.forEach(r => {
    if (!brandAgg[r.brand]) brandAgg[r.brand] = { spend: 0, byObj: {} };
    brandAgg[r.brand].spend += r.spend;
    brandAgg[r.brand].byObj[r.obj] = (brandAgg[r.brand].byObj[r.obj] || 0) + r.spend;
  });
  let gmvFiltered = D.cityGmv.filter(r => r.dt >= startDt && r.dt < endDt);
  if (cityFilter !== 'all') gmvFiltered = gmvFiltered.filter(r => r.city === cityFilter);
  const totalGmv = gmvFiltered.reduce((s, r) => s + r.gmv, 0);
  const totalSpend = Object.values(brandAgg).reduce((s, b) => s + b.spend, 0);
  const totalPct = totalGmv ? 100 * totalSpend / totalGmv : 0;
  const periodLabels = { mtd:'MTD', wtd:P.wtd_label, last7:'Last 7 Days', yesterday:'Yesterday', custom:`${startDt} \\u2192 ${endDt}` };
  const periodLabel = periodLabels[brandPeriodEl.value] || 'MTD';
  document.getElementById('brandSummaryCards').innerHTML = `
    <div class="card"><div class="label">${periodLabel} Total Bolt Spend</div>
      <div class="value">${fmtEur(totalSpend)}</div>
      <div class="sub-info">${fmtPct(totalPct)} of GMV</div></div>
    <div class="card"><div class="label">${periodLabel} GMV</div>
      <div class="value">${fmtEur(totalGmv)}</div>
      <div class="sub-info">${cityFilter === 'all' ? 'All cities' : cityFilter}</div></div>
    <div class="card"><div class="label">Active Brands</div>
      <div class="value">${Object.keys(brandAgg).length}</div>
      <div class="sub-info">With Bolt spend > 0</div></div>
    <div class="card"><div class="label">Avg Spend / Brand</div>
      <div class="value">${fmtEur(Object.keys(brandAgg).length ? totalSpend/Object.keys(brandAgg).length : 0)}</div>
      <div class="sub-info">${fmtPct(Object.keys(brandAgg).length ? totalPct/Object.keys(brandAgg).length : 0)} of GMV</div></div>`;
  document.getElementById('brandTableTitle').textContent =
    `Brand Spend \\u2014 ${periodLabel}${cityFilter !== 'all' ? ' \\u2014 ' + cityFilter : ''} \\u2014 ${fmtPct(totalPct)} of GMV`;
  const sorted = Object.entries(brandAgg).sort((a,b) => b[1].spend - a[1].spend);
  const activeObjsForTable = objFilter === 'all' ? CFG.objectives : [objFilter];
  const objHeaders = activeObjsForTable.map(o => `<th class="text-right">${CFG.labels[o]}</th>`).join('');
  let rows = ''; let rank = 0;
  sorted.forEach(([brand, data]) => {
    rank++;
    const pct = totalGmv ? 100 * data.spend / totalGmv : 0;
    const share = totalSpend ? 100 * data.spend / totalSpend : 0;
    let cells = `<td>${rank}</td><td>${brand}</td>`;
    activeObjsForTable.forEach(o => {
      const s = data.byObj[o] || 0;
      const op = totalGmv ? 100*s/totalGmv : 0;
      cells += `<td class="text-right">${fmtEur(s)}<br><span class="sub">${fmtPct(op)}</span></td>`;
    });
    cells += `<td class="text-right total-cell">${fmtEur(data.spend)}</td>`;
    cells += `<td class="text-right">${fmtPct(pct)}</td>`;
    cells += `<td class="text-right">${share.toFixed(1)}%</td>`;
    rows += `<tr>${cells}</tr>`;
  });
  document.getElementById('brandTable').innerHTML =
    `<tr><th>#</th><th>Brand</th>${objHeaders}<th class="text-right">Total Spend</th><th class="text-right">% of GMV</th><th class="text-right">% of Total Spend</th></tr>${rows}`;
}
renderOverview();
renderBrands();
</script>
</body>
</html>"""
    return html


def main():
    print("Loading config...")
    cfg = load_config()
    periods = get_periods()

    print(f"Period: {periods['month_start']} to {periods['today']} (excl today)")
    print(f"Country: {cfg['country'].upper()} | Budget: {cfg['monthly_budget_pct_gmv']}% of GMV")

    print("Querying daily overview spend...")
    df_overview = query_daily_overview(cfg, periods["month_start"], periods["today"])
    print(f"  Overview: {len(df_overview)} rows, {df_overview['dt'].nunique()} days")

    print("Querying brand-level spend...")
    df_brands = query_brand_spend(cfg, periods["month_start"], periods["today"])
    print(f"  Brands: {len(df_brands)} rows, {df_brands['brand'].nunique()} brands")

    print("Querying city-level GMV...")
    df_city_gmv = query_city_gmv(cfg, periods["month_start"], periods["today"])
    print(f"  City GMV: {len(df_city_gmv)} rows, {df_city_gmv['city'].nunique()} cities")

    OUTPUT_DIR.mkdir(exist_ok=True)

    print("Generating HTML dashboard...")
    html = generate_html(
        df_to_records(df_overview), df_to_records(df_brands),
        df_to_records(df_city_gmv), cfg, periods,
    )

    password = os.environ.get("DASHBOARD_PASSWORD") or cfg.get("password", "")
    if password:
        print("Encrypting dashboard with password protection...")
        html = wrap_with_password(html, password)

    with open(OUTPUT_PATH, "w") as f:
        f.write(html)

    print(f"Dashboard saved to: {OUTPUT_PATH}")


def wrap_with_password(inner_html, password):
    """Encrypt dashboard HTML with AES-GCM, wrap in a password-prompt page."""
    salt = os.urandom(16)
    iv = os.urandom(12)
    key = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000, dklen=32)

    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    aesgcm = AESGCM(key)
    plaintext = inner_html.encode("utf-8")
    ciphertext = aesgcm.encrypt(iv, plaintext, None)

    payload = {
        "salt": base64.b64encode(salt).decode(),
        "iv": base64.b64encode(iv).decode(),
        "ct": base64.b64encode(ciphertext).decode(),
    }

    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Provider Spend Dashboard — Login</title>
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: #f5f6fa; display:flex; justify-content:center; align-items:center; min-height:100vh; }
.login { background:#fff; border:1px solid #e2e4ec; border-radius:12px; padding:40px; width:360px; text-align:center; }
.login h1 { font-size:18px; font-weight:700; margin-bottom:8px; color:#1a1d2e; }
.login p { font-size:13px; color:#6b7085; margin-bottom:24px; }
.login input { width:100%; padding:10px 14px; border:1px solid #e2e4ec; border-radius:8px;
               font-size:14px; margin-bottom:12px; }
.login input:focus { outline:none; border-color:#3D7ABF; }
.login button { width:100%; padding:10px; background:#3D7ABF; color:#fff; border:none;
                border-radius:8px; font-size:14px; font-weight:600; cursor:pointer; }
.login button:hover { background:#2d6aa3; }
.error { color:#D64545; font-size:12px; margin-top:8px; display:none; }
</style>
</head>
<body>
<div class="login" id="loginBox">
  <h1>Provider Spend Dashboard</h1>
  <p>Enter the password to view the dashboard.</p>
  <input type="password" id="pwd" placeholder="Password" autofocus>
  <button onclick="unlock()">Unlock</button>
  <div class="error" id="err">Incorrect password. Try again.</div>
</div>
<script>
const E=""" + json.dumps(payload) + """;
document.getElementById('pwd').addEventListener('keydown', e => { if(e.key==='Enter') unlock(); });
async function unlock() {
  const pwd = document.getElementById('pwd').value;
  try {
    const salt = Uint8Array.from(atob(E.salt), c => c.charCodeAt(0));
    const iv = Uint8Array.from(atob(E.iv), c => c.charCodeAt(0));
    const ct = Uint8Array.from(atob(E.ct), c => c.charCodeAt(0));
    const enc = new TextEncoder();
    const keyMaterial = await crypto.subtle.importKey('raw', enc.encode(pwd), 'PBKDF2', false, ['deriveKey']);
    const key = await crypto.subtle.deriveKey(
      { name:'PBKDF2', salt, iterations:100000, hash:'SHA-256' },
      keyMaterial, { name:'AES-GCM', length:256 }, false, ['decrypt']
    );
    const decrypted = await crypto.subtle.decrypt({ name:'AES-GCM', iv }, key, ct);
    const html = new TextDecoder().decode(decrypted);
    document.open(); document.write(html); document.close();
  } catch(e) {
    document.getElementById('err').style.display = 'block';
  }
}
</script>
</body>
</html>"""


if __name__ == "__main__":
    main()
