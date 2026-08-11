#!/usr/bin/env python3
"""
UDM Pro WAN-Traffic-Monitor
===========================

Holt WAN-Kennzahlen ueber die UniFi Site Manager API (api.ui.com), schreibt sie
fortlaufend in eine CSV und erzeugt nach jedem Durchlauf einen aktuellen
HTML-Bericht. Der Bericht ist sofort nach dem ersten Poll nutzbar und wird mit
jedem weiteren Durchlauf praeziser.

Nur Standardbibliothek, keine Installation noetig.

Der API-Key wird aus der Umgebungsvariable UI_API_KEY gelesen und niemals in
Dateien geschrieben.

Aufrufe
-------
  python3 udm_wan_monitor.py --discover
      Zeigt Hosts, Sites und ein Rohdaten-Sample. Einmalig zum Pruefen.

  python3 udm_wan_monitor.py --loop
      Dauerbetrieb: pollt alle 15 Minuten bis zum Messende und schreibt
      nach jedem Poll den Bericht neu.

  python3 udm_wan_monitor.py --once
      Ein einzelner Poll plus Bericht. Fuer Cron oder Aufgabenplaner.

  python3 udm_wan_monitor.py --report
      Nur Bericht aus vorhandener CSV, ohne API-Zugriff.
"""

import argparse
import csv
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

API_BASE = "https://api.ui.com/ea"
CONNECTOR_BASE = "https://api.ui.com/v1/connector/consoles"
# LSB--UDM-1, Site-Manager hostId. Ueberschreibbar per --host-id.
DEFAULT_HOST_ID = "6C63F8E29F260000000009410E510000000009C09DF000000000686B4930:173285773"
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(DATA_DIR, "wan_traffic.csv")
HTML_PATH = os.path.join(DATA_DIR, "wan_report.html")
RAW_PATH = os.path.join(DATA_DIR, "raw_sample.json")
STATE_PATH = os.path.join(DATA_DIR, "monitor_state.json")

CSV_FIELDS = ["ts", "site", "uplink", "interval_s", "down_bytes", "up_bytes"]

TIME_KEYS = ("metrictime", "timestamp", "time", "periodstart", "starttime", "date")


# ----------------------------------------------------------------------------
# API
# ----------------------------------------------------------------------------

def api_key():
    key = os.environ.get("UI_API_KEY", "").strip()
    if not key:
        sys.exit(
            "UI_API_KEY ist nicht gesetzt.\n"
            "  Linux/macOS:  export UI_API_KEY='...'\n"
            "  PowerShell:   $env:UI_API_KEY='...'"
        )
    return key


def api_get(path, params=None, timeout=30):
    url = API_BASE + path
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
    req = urllib.request.Request(url, headers={
        "X-API-KEY": api_key(),
        "Accept": "application/json",
        "User-Agent": "udm-wan-monitor/1.0",
    })
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")[:400]
            if exc.code == 429 and attempt < 2:
                time.sleep(20 * (attempt + 1))
                continue
            if exc.code == 401:
                sys.exit("401: API-Key wird abgelehnt. Neuen Key erzeugen und UI_API_KEY setzen.")
            sys.exit(f"HTTP {exc.code} bei {path}: {body}")
        except urllib.error.URLError as exc:
            if attempt < 2:
                time.sleep(10)
                continue
            sys.exit(f"Keine Verbindung zu api.ui.com: {exc.reason}")
    return {}


def connector_get(host_id, path, timeout=30):
    """Ruft die lokale UniFi-Network-API einer Konsole ueber den Site-Manager-
    Connector-Proxy auf (kein VPN/lokales Netz noetig). Liefert echte
    Live-Uplink-Raten, im Gegensatz zur unzuverlaessigen isp-metrics-API."""
    url = (CONNECTOR_BASE + "/" + urllib.parse.quote(host_id, safe=":")
           + "/proxy/network/integration/v1" + path)
    req = urllib.request.Request(url, headers={
        "X-API-KEY": api_key(),
        "Accept": "application/json",
        "User-Agent": "udm-wan-monitor/1.0",
    })
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")[:400]
            if exc.code == 429 and attempt < 2:
                time.sleep(20 * (attempt + 1))
                continue
            if exc.code == 401:
                sys.exit("401: API-Key wird abgelehnt (Connector-Proxy).")
            if exc.code == 403:
                sys.exit(
                    f"403: Key hat keinen Zugriff auf Konsole {host_id} (Connector-Proxy).\n"
                    "  Pruefen: API-Key auf unifi.ui.com bearbeiten, alle benoetigten\n"
                    "  Konsolen im Scope freigeben."
                )
            sys.exit(f"HTTP {exc.code} beim Connector-Proxy {path}: {body}")
        except urllib.error.URLError as exc:
            if attempt < 2:
                time.sleep(10)
                continue
            sys.exit(f"Keine Verbindung zu api.ui.com (Connector-Proxy): {exc.reason}")
    return {}


def find_gateway_device(host_id):
    """Ermittelt lokale Network-API site_id und device_id der Konsole (Gateway)
    selbst, ueber MAC-Abgleich mit der Site-Manager hostId."""
    sites = connector_get(host_id, "/sites")
    site_id = sites["data"][0]["id"]
    devices = connector_get(host_id, f"/sites/{site_id}/devices")
    # hostId = <12-stellige MAC><interne Zusatz-Hex-Ziffern>:<numerisch>
    mac_target = host_id.split(":")[0][:12].upper()
    for dev in devices.get("data", []):
        mac = dev.get("macAddress", "").replace(":", "").upper()
        if mac == mac_target:
            return site_id, dev["id"]
    sys.exit(f"Gateway-Geraet fuer Host {host_id} nicht in Network-API gefunden.")


def get_uplink_rates(host_id, site_id, device_id):
    """Liest die aktuelle Live-Uplink-Rate (Bit/s) der Konsole."""
    stats = connector_get(host_id, f"/sites/{site_id}/devices/{device_id}/statistics/latest")
    uplink = stats.get("uplink", {})
    return uplink.get("rxRateBps", 0) or 0, uplink.get("txRateBps", 0) or 0


# ----------------------------------------------------------------------------
# Antwort einlesen (schema-tolerant)
# ----------------------------------------------------------------------------

def parse_ts(value):
    if isinstance(value, (int, float)):
        seconds = value / 1000.0 if value > 1e11 else float(value)
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    if isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def to_bytes(record, direction, interval_s):
    """Rechnet die gefundenen Felder in Bytes um, egal ob kbps, Mbps oder Bytes.

    Sucht auch in verschachtelten dicts/lists (z.B. period["data"]["wan"]["download_kbps"]),
    da die UI-API die Rate-Felder eine Ebene unter dem Zeitstempel liefert.
    """
    want_rx = direction == "down"

    def scan(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if not isinstance(value, (int, float)):
                    continue
                name = key.lower()
                is_dir = (("download" in name or name.startswith("rx") or "_rx" in name)
                          if want_rx else
                          ("upload" in name or name.startswith("tx") or "_tx" in name))
                if not is_dir:
                    continue
                if "byte" in name:
                    return float(value)
                if "kbps" in name or "kbit" in name:
                    return float(value) * 1000.0 / 8.0 * interval_s
                if "mbps" in name or "mbit" in name:
                    return float(value) * 1e6 / 8.0 * interval_s
                if "bps" in name:
                    return float(value) / 8.0 * interval_s
            for value in node.values():
                if isinstance(value, (dict, list)):
                    found = scan(value)
                    if found:
                        return found
        elif isinstance(node, list):
            for item in node:
                found = scan(item)
                if found:
                    return found
        return 0.0

    return scan(record)


def collect_points(payload, interval_s):
    """Durchlaeuft die Antwort und sammelt alle Messpunkte mit Zeitstempel."""
    points = []

    def walk(node, ctx):
        if isinstance(node, dict):
            ctx = dict(ctx)
            for label in ("siteName", "name", "siteId", "hostId", "internetSourceName"):
                if isinstance(node.get(label), str) and node[label]:
                    ctx.setdefault("site", node[label])
            for label in ("wanId", "uplinkId", "internetSourceId", "interfaceName", "wan"):
                if isinstance(node.get(label), str) and node[label]:
                    ctx["uplink"] = node[label]

            ts = None
            for key, value in node.items():
                if key.lower() in TIME_KEYS:
                    ts = parse_ts(value)
                    if ts:
                        break
            if ts:
                down = to_bytes(node, "down", interval_s)
                up = to_bytes(node, "up", interval_s)
                if down or up:
                    points.append({
                        "ts": ts,
                        "site": ctx.get("site", "site"),
                        "uplink": ctx.get("uplink", "wan"),
                        "interval_s": interval_s,
                        "down_bytes": round(down),
                        "up_bytes": round(up),
                    })
            for value in node.values():
                walk(value, ctx)
        elif isinstance(node, list):
            for item in node:
                walk(item, ctx)

    walk(payload, {})
    return points


# ----------------------------------------------------------------------------
# CSV
# ----------------------------------------------------------------------------

def load_rows():
    if not os.path.exists(CSV_PATH):
        return []
    rows = []
    with open(CSV_PATH, newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            ts = parse_ts(row["ts"])
            if not ts:
                continue
            rows.append({
                "ts": ts,
                "site": row["site"],
                "uplink": row["uplink"],
                "interval_s": int(float(row["interval_s"])),
                "down_bytes": float(row["down_bytes"]),
                "up_bytes": float(row["up_bytes"]),
            })
    return rows


def merge_rows(existing, new_points):
    index = {(r["ts"], r["site"], r["uplink"]): r for r in existing}
    added = 0
    for point in new_points:
        key = (point["ts"], point["site"], point["uplink"])
        if key not in index:
            added += 1
        index[key] = point
    merged = sorted(index.values(), key=lambda r: (r["ts"], r["site"], r["uplink"]))
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in merged:
            writer.writerow({
                "ts": row["ts"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "site": row["site"],
                "uplink": row["uplink"],
                "interval_s": row["interval_s"],
                "down_bytes": int(row["down_bytes"]),
                "up_bytes": int(row["up_bytes"]),
            })
    return merged, added


# ----------------------------------------------------------------------------
# Bericht
# ----------------------------------------------------------------------------

def human_bytes(value):
    step = 1024.0
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(value) < step or unit == "TB":
            return f"{value:,.1f} {unit}".replace(",", ".")
        value /= step
    return f"{value:.1f} TB"


def build_report(rows, start, end, site_filter=None):
    now = datetime.now(timezone.utc)
    window = [r for r in rows if start <= r["ts"] < end]
    if site_filter:
        needle = site_filter.lower()
        window = [r for r in window
                  if needle in r["site"].lower() or needle in r["uplink"].lower()]

    total_down = sum(r["down_bytes"] for r in window)
    total_up = sum(r["up_bytes"] for r in window)
    total = total_down + total_up

    elapsed_h = max((min(now, end) - start).total_seconds() / 3600.0, 0.001)
    remaining = max((end - now).total_seconds(), 0)
    covered_h = len({r["ts"].replace(minute=0, second=0, microsecond=0) for r in window}) or 0
    per_day = total / elapsed_h * 24
    per_month = per_day * 30

    # Stundenraster
    hours = {}
    for row in window:
        bucket = row["ts"].replace(minute=0, second=0, microsecond=0)
        entry = hours.setdefault(bucket, [0.0, 0.0])
        entry[0] += row["down_bytes"]
        entry[1] += row["up_bytes"]

    total_hours = int((end - start).total_seconds() // 3600)
    series = []
    for i in range(total_hours):
        bucket = start + timedelta(hours=i)
        down, up = hours.get(bucket, (0.0, 0.0))
        series.append((bucket, down, up))

    peak = max((d + u for _, d, u in series), default=0.0) or 1.0

    # Tagesuebersicht
    days = {}
    for bucket, down, up in series:
        local_day = bucket.astimezone().strftime("%d.%m.%Y")
        entry = days.setdefault(local_day, [0.0, 0.0, 0])
        entry[0] += down
        entry[1] += up
        if down or up:
            entry[2] += 1

    # Ausreisser
    spikes = sorted(series, key=lambda item: item[1] + item[2], reverse=True)[:5]
    spikes = [s for s in spikes if (s[1] + s[2]) > 0]

    chart = render_chart(series, peak, start)
    return render_html(
        window=window, total=total, total_down=total_down, total_up=total_up,
        elapsed_h=elapsed_h, remaining=remaining, covered_h=covered_h,
        per_day=per_day, per_month=per_month, days=days, spikes=spikes,
        chart=chart, start=start, end=end, now=now, peak=peak,
    )


def render_chart(series, peak, start):
    width, height = 960, 220
    left, bottom = 54, 28
    plot_w = width - left - 12
    plot_h = height - bottom - 12
    n = max(len(series), 1)
    slot = plot_w / n
    bar_w = max(slot * 0.72, 1.2)

    parts = []
    for i in range(1, 4):
        y = 12 + plot_h * (1 - i / 4.0)
        label = human_bytes(peak * i / 4.0)
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" class="grid"/>')
        parts.append(f'<text x="{left - 8}" y="{y + 4:.1f}" class="axis" text-anchor="end">{label}</text>')

    for i, (bucket, down, up) in enumerate(series):
        x = left + i * slot + (slot - bar_w) / 2
        h_down = plot_h * (down / peak)
        h_up = plot_h * (up / peak)
        y_down = 12 + plot_h - h_down
        y_up = y_down - h_up
        if down:
            parts.append(f'<rect x="{x:.1f}" y="{y_down:.1f}" width="{bar_w:.1f}" height="{h_down:.1f}" class="down"/>')
        if up:
            parts.append(f'<rect x="{x:.1f}" y="{y_up:.1f}" width="{bar_w:.1f}" height="{h_up:.1f}" class="up"/>')
        local = bucket.astimezone()
        if local.hour == 0 or i == 0:
            parts.append(f'<line x1="{x:.1f}" y1="12" x2="{x:.1f}" y2="{12 + plot_h}" class="daymark"/>')
            parts.append(f'<text x="{x + 4:.1f}" y="{height - 8}" class="axis">{local.strftime("%d.%m. %H:%M")}</text>')

    parts.append(f'<line x1="{left}" y1="{12 + plot_h}" x2="{left + plot_w}" y2="{12 + plot_h}" class="baseline"/>')
    return f'<svg viewBox="0 0 {width} {height}" class="chart" role="img" aria-label="Stundenvolumen">{"".join(parts)}</svg>'


def render_html(**c):
    start, end, now = c["start"], c["end"], c["now"]
    pct = min(c["elapsed_h"] / max((end - start).total_seconds() / 3600.0, 0.001), 1.0)
    rem_h = int(c["remaining"] // 3600)
    rem_m = int((c["remaining"] % 3600) // 60)
    status = "Messung laeuft" if c["remaining"] > 0 else "Messung abgeschlossen"

    day_rows = "".join(
        f"<tr><td>{day}</td><td class='num'>{human_bytes(v[0])}</td>"
        f"<td class='num'>{human_bytes(v[1])}</td>"
        f"<td class='num strong'>{human_bytes(v[0] + v[1])}</td>"
        f"<td class='num dim'>{v[2]} h</td></tr>"
        for day, v in sorted(c["days"].items())
    ) or "<tr><td colspan='5' class='dim'>Noch keine Daten im Messfenster.</td></tr>"

    spike_rows = "".join(
        f"<tr><td>{b.astimezone().strftime('%d.%m. %H:%M')}</td>"
        f"<td class='num'>{human_bytes(d)}</td><td class='num'>{human_bytes(u)}</td>"
        f"<td class='num strong'>{human_bytes(d + u)}</td></tr>"
        for b, d, u in c["spikes"]
    ) or "<tr><td colspan='4' class='dim'>Noch keine Auffaelligkeiten.</td></tr>"

    return f"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="300">
<title>WAN-Volumen UDM Pro</title>
<style>
  :root {{
    --ink: #0e1620; --panel: #16212e; --line: #24344a;
    --text: #dbe6f0; --dim: #7f93a8;
    --down: #46b3a3; --up: #e0a458; --alert: #d4675b;
  }}
  * {{ box-sizing: border-box; }}
  body {{ margin: 0; background: var(--ink); color: var(--text);
    font: 15px/1.55 "Inter", "Segoe UI", system-ui, sans-serif; padding: 32px 24px 64px; }}
  .wrap {{ max-width: 1020px; margin: 0 auto; }}
  header {{ border-bottom: 1px solid var(--line); padding-bottom: 18px; margin-bottom: 26px; }}
  h1 {{ font-size: 25px; margin: 0 0 6px; letter-spacing: -.01em; font-weight: 600; }}
  .sub {{ color: var(--dim); font-size: 13.5px; }}
  .bar {{ height: 5px; background: var(--line); border-radius: 3px; margin-top: 16px; overflow: hidden; }}
  .bar span {{ display: block; height: 100%; width: {pct * 100:.1f}%; background: var(--down); }}
  .grid-cards {{ display: grid; gap: 14px; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); margin-bottom: 30px; }}
  .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 16px 18px; }}
  .card .label {{ color: var(--dim); font-size: 12px; text-transform: uppercase; letter-spacing: .09em; }}
  .card .value {{ font: 600 26px/1.25 ui-monospace, "SFMono-Regular", Consolas, monospace; margin-top: 8px; }}
  .card .foot {{ color: var(--dim); font-size: 12.5px; margin-top: 4px; }}
  h2 {{ font-size: 15px; text-transform: uppercase; letter-spacing: .1em; color: var(--dim);
    margin: 34px 0 12px; font-weight: 600; }}
  .panel {{ background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 18px; }}
  .chart {{ width: 100%; height: auto; }}
  .chart .grid {{ stroke: var(--line); stroke-width: 1; }}
  .chart .baseline {{ stroke: var(--dim); stroke-width: 1; }}
  .chart .daymark {{ stroke: var(--line); stroke-dasharray: 3 4; }}
  .chart .axis {{ fill: var(--dim); font: 10.5px ui-monospace, monospace; }}
  .chart .down {{ fill: var(--down); }}
  .chart .up {{ fill: var(--up); }}
  .legend {{ display: flex; gap: 20px; color: var(--dim); font-size: 12.5px; margin-top: 10px; }}
  .dot {{ display: inline-block; width: 9px; height: 9px; border-radius: 2px; margin-right: 6px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
  th {{ text-align: left; color: var(--dim); font-weight: 500; font-size: 12px;
    text-transform: uppercase; letter-spacing: .07em; padding: 0 10px 10px; }}
  td {{ padding: 9px 10px; border-top: 1px solid var(--line); }}
  .num {{ text-align: right; font-family: ui-monospace, monospace; }}
  .strong {{ color: #fff; }}
  .dim {{ color: var(--dim); }}
  footer {{ color: var(--dim); font-size: 12.5px; margin-top: 34px;
    border-top: 1px solid var(--line); padding-top: 14px; }}
  @media (prefers-reduced-motion: no-preference) {{ .bar span {{ transition: width .4s ease; }} }}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>WAN-Volumen UDM Pro</h1>
    <div class="sub">{status} &nbsp;&middot;&nbsp; Fenster {start.astimezone().strftime('%d.%m.%Y %H:%M')}
      bis {end.astimezone().strftime('%d.%m.%Y %H:%M')} &nbsp;&middot;&nbsp;
      Restlaufzeit {rem_h} h {rem_m} min &nbsp;&middot;&nbsp;
      Stand {now.astimezone().strftime('%d.%m.%Y %H:%M')}</div>
    <div class="bar"><span></span></div>
  </header>

  <div class="grid-cards">
    <div class="card"><div class="label">Bisher gemessen</div>
      <div class="value">{human_bytes(c['total'])}</div>
      <div class="foot">ueber {c['elapsed_h']:.1f} Stunden, {c['covered_h']} Stunden mit Daten</div></div>
    <div class="card"><div class="label">Pro Tag</div>
      <div class="value">{human_bytes(c['per_day'])}</div>
      <div class="foot">laufender Mittelwert</div></div>
    <div class="card"><div class="label">Hochrechnung 30 Tage</div>
      <div class="value">{human_bytes(c['per_month'])}</div>
      <div class="foot">bei gleichbleibender Grundlast</div></div>
    <div class="card"><div class="label">Verhaeltnis</div>
      <div class="value">{human_bytes(c['total_down'])}</div>
      <div class="foot">Download, dazu {human_bytes(c['total_up'])} Upload</div></div>
  </div>

  <h2>Stundenvolumen im Messfenster</h2>
  <div class="panel">
    {c['chart']}
    <div class="legend">
      <span><span class="dot" style="background:var(--down)"></span>Download</span>
      <span><span class="dot" style="background:var(--up)"></span>Upload</span>
      <span>Spitze {human_bytes(c['peak'])} pro Stunde</span>
    </div>
  </div>

  <h2>Tageswerte</h2>
  <div class="panel">
    <table>
      <thead><tr><th>Tag</th><th class="num">Download</th><th class="num">Upload</th>
        <th class="num">Gesamt</th><th class="num">Abdeckung</th></tr></thead>
      <tbody>{day_rows}</tbody>
    </table>
  </div>

  <h2>Groesste Stunden</h2>
  <div class="panel">
    <table>
      <thead><tr><th>Stunde</th><th class="num">Download</th><th class="num">Upload</th>
        <th class="num">Gesamt</th></tr></thead>
      <tbody>{spike_rows}</tbody>
    </table>
    <p class="dim" style="margin:14px 0 0;font-size:13px">Ausschlaege deutlich ueber der Grundlast
      stammen erfahrungsgemaess von Speedtests, Firmware- oder Signatur-Downloads. Fuer die reine
      Management-Grundlast diese Stunden abziehen.</p>
  </div>

  <footer>Datenquelle UniFi Site Manager API, {len(c['window'])} Messpunkte im Fenster.
    Seite aktualisiert sich alle 5 Minuten selbst.</footer>
</div>
</body>
</html>"""


def write_report(rows, start, end, site_filter=None):
    html = build_report(rows, start, end, site_filter)
    with open(HTML_PATH, "w", encoding="utf-8") as handle:
        handle.write(html)
    return HTML_PATH


# ----------------------------------------------------------------------------
# Ablauf
# ----------------------------------------------------------------------------

def poll(rows, host_id, site_id, device_id, interval_s, console_name):
    """Ein Live-Sample der aktuellen Uplink-Rate, hochgerechnet auf interval_s.

    Hinweis: Die isp-metrics-API lieferte nachweislich falsche Werte (Faktor
    ~5000 gegenueber dem GUI-Traffic-Graphen). Diese Funktion nutzt stattdessen
    den Site-Manager-Connector-Proxy zur lokalen Network-API der Konsole, der
    echte Live-Uplink-Raten liefert (verifiziert gegen den GUI-Graphen).
    """
    rx_bps, tx_bps = get_uplink_rates(host_id, site_id, device_id)
    now = datetime.now(timezone.utc)
    point = {
        "ts": now,
        "site": console_name,
        "uplink": "wan",
        "interval_s": interval_s,
        "down_bytes": round(rx_bps / 8.0 * interval_s),
        "up_bytes": round(tx_bps / 8.0 * interval_s),
    }
    return merge_rows(rows, [point])


def discover(host_id=DEFAULT_HOST_ID):
    for name, path in (("Hosts", "/hosts"), ("Sites", "/sites")):
        data = api_get(path)
        print(f"\n=== {name} ===")
        print(json.dumps(data, indent=2)[:3000])

    print(f"\n=== Connector-Proxy Live-Stats fuer hostId {host_id} ===")
    site_id, device_id = find_gateway_device(host_id)
    print(f"lokale site_id: {site_id}, device_id: {device_id}")
    stats = connector_get(host_id, f"/sites/{site_id}/devices/{device_id}/statistics/latest")
    with open(RAW_PATH, "w", encoding="utf-8") as handle:
        json.dump(stats, handle, indent=2)
    print(json.dumps(stats, indent=2))
    rx_bps, tx_bps = get_uplink_rates(host_id, site_id, device_id)
    print(f"\nAktuelle Rate: down={rx_bps/1000:.1f} kbps, up={tx_bps/1000:.1f} kbps")
    print(f"Vollstaendige Rohdaten in {RAW_PATH}")
    print("Hinweis: isp-metrics wird nicht mehr genutzt (lieferte falsche Werte, "
          "siehe Support-Ticket). Diese Live-Rate via Connector-Proxy ist die "
          "verifizierte Datenquelle.")


def main():
    parser = argparse.ArgumentParser(description="WAN-Volumen der UDM Pro messen")
    parser.add_argument("--start", help="Messbeginn, z.B. 2026-08-07T12:00 (lokale Zeit)")
    parser.add_argument("--days", type=float, default=3.0, help="Messdauer in Tagen")
    parser.add_argument("--interval", type=int, default=900, help="Pollintervall in Sekunden")
    parser.add_argument("--site", help="Filter auf einen Site- oder Uplink-Namen")
    parser.add_argument("--host-id", default=DEFAULT_HOST_ID,
                         help="Site-Manager hostId der Zielkonsole (Default: LSB--UDM-1)")
    parser.add_argument("--discover", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args()

    if args.discover:
        discover(args.host_id)
        return

    if args.start:
        start = datetime.fromisoformat(args.start).astimezone()
    elif os.path.exists(STATE_PATH):
        with open(STATE_PATH, encoding="utf-8") as handle:
            start = datetime.fromisoformat(json.load(handle)["start"])
    else:
        start = datetime.now().astimezone().replace(minute=0, second=0, microsecond=0)
    start = start.astimezone(timezone.utc)
    end = start + timedelta(days=args.days)
    with open(STATE_PATH, "w", encoding="utf-8") as handle:
        json.dump({"start": start.isoformat(), "days": args.days}, handle)

    if args.report:
        rows = load_rows()
        print(f"Bericht geschrieben: {write_report(rows, start, end, args.site)}")
        return

    console_name = args.host_id
    for host in api_get("/hosts").get("data", []):
        if host.get("id") == args.host_id:
            console_name = host.get("reportedState", {}).get("name", args.host_id)
            break
    site_id, device_id = find_gateway_device(args.host_id)
    print(f"Ziel: {console_name} (site_id={site_id}, device_id={device_id})")

    rows = load_rows()
    while True:
        rows, added = poll(rows, args.host_id, site_id, device_id, args.interval, console_name)
        path = write_report(rows, start, end, args.site)
        stamp = datetime.now().astimezone().strftime("%H:%M:%S")
        print(f"[{stamp}] {added} neue Messpunkte, {len(rows)} gesamt -> {path}")
        if args.once or datetime.now(timezone.utc) >= end:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
