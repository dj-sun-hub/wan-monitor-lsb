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
import html
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

# Ueberwachte Konsolen (Name, Site-Manager hostId) fuer den Standard-Multi-Konsolen-
# Betrieb. WSG--UDM-1 ist bewusst nicht dabei. Namen sind direkt hinterlegt (statt
# per API aufgeloest), damit --report ohne Netzzugriff funktioniert.
MONITORED_HOSTS = [
    ("HAN--UDM-1", "6C63F8AA761300000000093BE1DD0000000009BB150D00000000685B037F:1493605141"),
    ("KLO--UDM-1", "6C63F8E2993E000000000941189A0000000009C0A86600000000686B5BD1:662409267"),
    ("KNZ--UDM-1", "0CEA14D5BB63000000000899F5C200000000090F78C6000000006763F2E2:1427694241"),
    ("LSB--UDM-1", "6C63F8E29F260000000009410E510000000009C09DF000000000686B4930:173285773"),
    ("NID--UDM-1", "6C63F8AB54A900000000093C22300000000009BB5AAA00000000685B7889:144919651"),
    ("WTB--UDM-1", "0CEA146F1C450000000008887B2B0000000008FCF87300000000674692E9:927254559"),
]
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(DATA_DIR, "wan_traffic.csv")
HTML_PATH = os.path.join(DATA_DIR, "wan_report.html")
RAW_PATH = os.path.join(DATA_DIR, "raw_sample.json")
STATE_PATH = os.path.join(DATA_DIR, "monitor_state.json")

CSV_FIELDS = ["ts", "site", "uplink", "interval_s", "down_bytes", "up_bytes"]
REPORT_REFRESH_S = 120  # Seite laedt sich automatisch neu, siehe <meta refresh> und Countdown

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
                    "  Prüfen: API-Key auf unifi.ui.com bearbeiten, alle benötigten\n"
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
    sys.exit(f"Gateway-Gerät für Host {host_id} nicht in Network-API gefunden.")


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
    """Durchläuft die Antwort und sammelt alle Messpunkte mit Zeitstempel."""
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


GIB = 1024 ** 3
WARN_THRESHOLD_BYTES = 1 * GIB
ALERT_THRESHOLD_BYTES = 4.8 * GIB


def total_alert_class(total_bytes):
    """CSS-Klassen-Zusatz fuer den 'Bisher'-Wert: gelb ab 1 GB, rot ab 4.8 GB."""
    if total_bytes > ALERT_THRESHOLD_BYTES:
        return " value-alert"
    if total_bytes > WARN_THRESHOLD_BYTES:
        return " value-warn"
    return ""


def compute_stats(rows, start, end, site_filter=None):
    """Berechnet alle Kennzahlen fuer eine Konsole (oder alle, falls site_filter
    leer) und liefert sie als dict zurueck - roh, ohne HTML. Wird sowohl fuer
    Detailseiten (render_html) als auch für Karten der Übersichtsseite
    (render_overview_html) genutzt."""
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

    # Letzte 24 Stunden, Einzelauflistung
    last24_start = now - timedelta(hours=24)
    last24 = [s for s in series if last24_start <= s[0] <= now]

    chart = render_chart(series, peak, start)
    flow_chart = render_flow_chart(window, start, min(now, end))
    flow_points = len(window)
    flow_intervals = sorted({r["interval_s"] for r in window if r["interval_s"]})
    flow_interval_min = round(flow_intervals[0] / 60) if flow_intervals else 15
    device = site_filter or (window[0]["site"] if window else (rows[0]["site"] if rows else "unbekannt"))
    return dict(
        window=window, total=total, total_down=total_down, total_up=total_up,
        elapsed_h=elapsed_h, remaining=remaining, covered_h=covered_h,
        per_day=per_day, per_month=per_month, days=days, spikes=spikes,
        chart=chart, start=start, end=end, now=now, peak=peak, device=device,
        flow_chart=flow_chart, flow_points=flow_points, flow_interval_min=flow_interval_min,
        last24=last24,
    )


def build_report(rows, start, end, site_filter=None):
    return render_html(**compute_stats(rows, start, end, site_filter))


def build_overview(rows, start, end, console_names):
    now = datetime.now(timezone.utc)
    consoles = [compute_stats(rows, start, end, site_filter=name) for name in console_names]
    return render_overview_html(consoles=consoles, start=start, end=end, now=now)


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
    return (f'<svg viewBox="0 0 {width} {height}" preserveAspectRatio="none" class="chart" '
            f'role="img" aria-label="Stundenvolumen">{"".join(parts)}</svg>')


def render_flow_chart(window, start, end):
    """Feinkoerniger Traffic-Flow-Graph: Rate (kbps) je Poll-Punkt ueber die Zeit,
    im Gegensatz zum Stundenchart nicht zu Stundensummen aggregiert."""
    width, height = 960, 200
    left, bottom = 54, 28
    plot_w = width - left - 12
    plot_h = height - bottom - 12

    pts = sorted((r for r in window if r["interval_s"]), key=lambda r: r["ts"])
    if len(pts) < 2:
        return '<p class="dim" style="margin:0">Noch nicht genug Messpunkte fuer den Flow-Graphen.</p>'

    span_s = max((end - start).total_seconds(), 1)

    def rate_kbps(bytes_, interval_s):
        return bytes_ * 8.0 / interval_s / 1000.0

    samples = [(r["ts"],
                rate_kbps(r["down_bytes"], r["interval_s"]),
                rate_kbps(r["up_bytes"], r["interval_s"])) for r in pts]
    peak = max((max(d, u) for _, d, u in samples), default=0.0) or 1.0

    def xy(ts, value):
        x = left + (ts - start).total_seconds() / span_s * plot_w
        y = 12 + plot_h - (value / peak) * plot_h
        return x, y

    baseline_y = 12 + plot_h
    down_line = [xy(ts, d) for ts, d, u in samples]
    up_line = [xy(ts, u) for ts, d, u in samples]
    down_pts_str = " ".join(f"{x:.1f},{y:.1f}" for x, y in down_line)
    up_pts_str = " ".join(f"{x:.1f},{y:.1f}" for x, y in up_line)
    down_area = f"{down_line[0][0]:.1f},{baseline_y:.1f} {down_pts_str} {down_line[-1][0]:.1f},{baseline_y:.1f}"

    parts = []
    for i in range(1, 4):
        y = 12 + plot_h * (1 - i / 4.0)
        label = f"{peak * i / 4.0:,.0f} kbps".replace(",", ".")
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" class="grid"/>')
        parts.append(f'<text x="{left - 8}" y="{y + 4:.1f}" class="axis" text-anchor="end">{label}</text>')

    day_cursor = start.astimezone().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    end_local = end.astimezone()
    while day_cursor < end_local:
        x, _ = xy(day_cursor.astimezone(timezone.utc), 0)
        parts.append(f'<line x1="{x:.1f}" y1="12" x2="{x:.1f}" y2="{baseline_y:.1f}" class="daymark"/>')
        parts.append(f'<text x="{x + 4:.1f}" y="{height - 8}" class="axis">{day_cursor.strftime("%d.%m.")}</text>')
        day_cursor += timedelta(days=1)

    parts.append(f'<polygon points="{down_area}" class="flow-down-fill"/>')
    parts.append(f'<polyline points="{down_pts_str}" class="flow-down-line"/>')
    parts.append(f'<polyline points="{up_pts_str}" class="flow-up-line"/>')
    parts.append(f'<line x1="{left}" y1="{baseline_y:.1f}" x2="{left + plot_w}" y2="{baseline_y:.1f}" class="baseline"/>')
    # Hover-Linie: unsichtbar per Default, wird von flow_tooltip_script beim Hovern
    # an die x-Position des naechstgelegenen Messpunkts verschoben und eingeblendet.
    parts.append(f'<line class="hover-line" x1="0" y1="12" x2="0" y2="{baseline_y:.1f}"/>')

    # Rohdaten fuer den Hover-Tooltip (siehe flow_tooltip_script): Zeitstempel, Rate
    # und die exakte x-Pixel-Position je Punkt (fuer die Hover-Linie), plus die
    # Plot-Geometrie, damit JS Maus-X auf den naechsten Punkt mappen kann.
    samples_json = json.dumps([
        [ts.isoformat(), round(d, 1), round(u, 1), round(x, 1)]
        for (ts, d, u), (x, _y) in zip(samples, down_line)
    ])
    samples_attr = html.escape(samples_json, quote=True)
    return (f'<svg viewBox="0 0 {width} {height}" preserveAspectRatio="none" class="chart flow-chart" '
            f'role="img" aria-label="Traffic-Flow" '
            f'data-samples="{samples_attr}" data-left="{left}" data-plot-w="{plot_w:.2f}">'
            f'{"".join(parts)}</svg>')


BASE_CSS = """
  :root {
    --ink: #0e1620; --panel: #16212e; --line: #24344a;
    --text: #dbe6f0; --dim: #7f93a8;
    --down: #46b3a3; --up: #e0a458; --alert: #d4675b; --warn: #e8c14c;
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--ink); color: var(--text);
    font: 15px/1.55 "Inter", "Segoe UI", system-ui, sans-serif; padding: 32px 24px 64px; }
  .wrap { max-width: 1020px; margin: 0 auto; }
  header { border-bottom: 1px solid var(--line); padding-bottom: 18px; margin-bottom: 26px; }
  h1 { font-size: 25px; margin: 0 0 6px; letter-spacing: -.01em; font-weight: 600; }
  .sub { color: var(--dim); font-size: 13.5px; }
  .bar { height: 5px; background: var(--line); border-radius: 3px; margin-top: 16px; overflow: hidden; }
  .bar span { display: block; height: 100%; background: var(--down); }
  .grid-cards { display: grid; gap: 14px; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); margin-bottom: 30px; }
  .card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 16px 18px; }
  .card .label { color: var(--dim); font-size: 12px; text-transform: uppercase; letter-spacing: .09em; }
  .card .value { font: 600 26px/1.25 ui-monospace, "SFMono-Regular", Consolas, monospace; margin-top: 8px; }
  .card .foot { color: var(--dim); font-size: 12.5px; margin-top: 4px; }
  h2 { font-size: 15px; text-transform: uppercase; letter-spacing: .1em; color: var(--dim);
    margin: 34px 0 12px; font-weight: 600; }
  .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 18px; }
  .chart { width: 100%; height: auto; }
  .chart .grid { stroke: var(--line); stroke-width: 1; }
  .chart .baseline { stroke: var(--dim); stroke-width: 1; }
  .chart .daymark { stroke: var(--line); stroke-dasharray: 3 4; }
  .chart .axis { fill: var(--dim); font: 10.5px ui-monospace, monospace; }
  .chart .down { fill: var(--down); }
  .chart .up { fill: var(--up); }
  .chart .flow-down-fill { fill: var(--down); opacity: .16; stroke: none; }
  .chart .flow-down-line { fill: none; stroke: var(--down); stroke-width: 1.6; }
  .chart .flow-up-line { fill: none; stroke: var(--up); stroke-width: 1.6; }
  .legend { display: flex; gap: 20px; color: var(--dim); font-size: 12.5px; margin-top: 10px; flex-wrap: wrap; }
  .legend.small { font-size: 11.5px; gap: 12px; }
  .dot { display: inline-block; width: 9px; height: 9px; border-radius: 2px; margin-right: 6px; }
  table { width: 100%; border-collapse: collapse; font-size: 14px; }
  th { text-align: left; color: var(--dim); font-weight: 500; font-size: 12px;
    text-transform: uppercase; letter-spacing: .07em; padding: 0 10px 10px; }
  td { padding: 9px 10px; border-top: 1px solid var(--line); }
  .num { text-align: right; font-family: ui-monospace, monospace; }
  .strong { color: #fff; }
  .dim { color: var(--dim); }
  .value-warn { color: var(--warn); }
  .value-alert { color: var(--alert); }
  .flow-chart .hover-line { stroke: var(--text); stroke-width: 1; stroke-dasharray: 3 3;
    opacity: 0; pointer-events: none; }
  .flow-tooltip { position: fixed; display: none; z-index: 50; pointer-events: none;
    background: var(--panel); border: 1px solid var(--line); border-radius: 6px;
    padding: 7px 11px; font-size: 12px; line-height: 1.5; color: var(--text);
    box-shadow: 0 4px 14px rgba(0,0,0,.45); white-space: nowrap; }
  .flow-tooltip b { color: #fff; }
  .live-tag { display: inline-block; font-size: 10.5px; text-transform: uppercase;
    letter-spacing: .06em; color: var(--down); border: 1px solid var(--down);
    border-radius: 3px; padding: 1px 5px; margin-left: 6px; vertical-align: middle; }
  footer { color: var(--dim); font-size: 12.5px; margin-top: 34px;
    border-top: 1px solid var(--line); padding-top: 14px; }
  a { color: inherit; }
  @media (prefers-reduced-motion: no-preference) { .bar span { transition: width .4s ease; } }
"""


def refresh_countdown_script(now):
    """Gemeinsames Countdown-Skript für Detail- und Übersichtsseite. An den
    tatsaechlichen Erzeugungszeitpunkt gekoppelt, damit ein manueller Reload
    den Countdown nicht auf voll zuruecksetzt.

    Der eigentliche Reload passiert NICHT ueber <meta refresh> (das laedt
    dieselbe URL und kann von Browser/GitHub-Pages-CDN als zwischengespeicherte
    Antwort ausgeliefert werden). Stattdessen navigiert das Skript selbst auf
    die eigene URL mit einem Cache-Buster-Query-Parameter.

    Bremse eingebaut (sessionStorage): Ist der naechste echte Poll noch nicht
    passiert, hat die frisch geladene Seite denselben alten Erzeugungszeitpunkt
    und der Countdown ist sofort wieder bei 0 - ohne Bremse laedt das dann in
    einer engen Schleife (mehrfach pro Sekunde) neu. Deshalb: nach einem
    Reload-Versuch mindestens 10s warten, bevor der naechste Versuch startet.
    """
    return f"""<script>
(function() {{
  var generatedAtMs = new Date("{now.isoformat()}").getTime();
  var refreshS = {REPORT_REFRESH_S};
  var minRetryMs = 10000;
  var el = document.getElementById('refresh-cd');
  if (!el) return;
  function tick() {{
    var elapsedS = Math.floor((Date.now() - generatedAtMs) / 1000);
    var remaining = Math.max(refreshS - elapsedS, 0);
    var m = Math.floor(remaining / 60), s = remaining % 60;
    el.textContent = m + ':' + (s < 10 ? '0' : '') + s;
    if (remaining <= 0) {{
      var lastTry = parseInt(sessionStorage.getItem('wanmon_last_reload') || '0', 10);
      var nowMs = Date.now();
      if (nowMs - lastTry > minRetryMs) {{
        sessionStorage.setItem('wanmon_last_reload', String(nowMs));
        window.location.href = window.location.pathname + '?_=' + nowMs;
      }} else {{
        setTimeout(tick, 2000);
      }}
      return;
    }}
    setTimeout(tick, 1000);
  }}
  tick();
}})();
</script>"""


def flow_tooltip_script():
    """Hover-Tooltip fuer alle Traffic-Flow-Charts (svg.flow-chart) der Seite.
    Liest die in data-samples eingebetteten Rohdaten, mappt die Maus-X-Position
    ueber die SVG-CTM (funktioniert auch mit preserveAspectRatio="none") auf den
    naechstgelegenen Messpunkt und zeigt Zeit + Down/Up-Rate in einer kleinen,
    dem Cursor folgenden Box."""
    return """<script>
(function() {
  var tip = document.createElement('div');
  tip.className = 'flow-tooltip';
  document.body.appendChild(tip);

  function fmtKbps(v) {
    if (v >= 1000) return (v / 1000).toFixed(2).replace('.', ',') + ' Mbps';
    return v.toFixed(1).replace('.', ',') + ' kbps';
  }

  document.querySelectorAll('svg.flow-chart').forEach(function (svg) {
    var samples;
    try { samples = JSON.parse(svg.getAttribute('data-samples')); } catch (e) { return; }
    if (!samples || !samples.length) return;
    var hoverLine = svg.querySelector('.hover-line');

    // Suche ueber die tatsaechliche Pixel-Position jedes Punkts (samples[i][3]),
    // NICHT ueber den Index - die Messpunkte liegen wegen wechselnder Poll-
    // Intervalle in der Historie nicht gleichmaessig verteilt auf der x-Achse.
    // Binaersuche, da samples nach x aufsteigend sortiert sind.
    function nearest(svgX) {
      var lo = 0, hi = samples.length - 1;
      while (lo < hi) {
        var mid = (lo + hi) >> 1;
        if (samples[mid][3] < svgX) lo = mid + 1; else hi = mid;
      }
      if (lo > 0 && Math.abs(samples[lo - 1][3] - svgX) < Math.abs(samples[lo][3] - svgX)) {
        lo -= 1;
      }
      return samples[lo];
    }

    svg.addEventListener('mousemove', function (ev) {
      var pt = svg.createSVGPoint();
      pt.x = ev.clientX; pt.y = ev.clientY;
      var svgP = pt.matrixTransform(svg.getScreenCTM().inverse());
      var s = nearest(svgP.x);
      var d = new Date(s[0]);
      var timeStr = d.toLocaleString('de-DE', {
        day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit'
      });
      tip.innerHTML = '<b>' + timeStr + '</b><br>Down: ' + fmtKbps(s[1]) +
        '<br>Up: ' + fmtKbps(s[2]);
      var x = ev.clientX + 16, y = ev.clientY + 16;
      if (x + 170 > window.innerWidth) x = ev.clientX - 186;
      if (y + 60 > window.innerHeight) y = ev.clientY - 76;
      tip.style.left = x + 'px';
      tip.style.top = y + 'px';
      tip.style.display = 'block';
      if (hoverLine) {
        hoverLine.setAttribute('x1', s[3]);
        hoverLine.setAttribute('x2', s[3]);
        hoverLine.style.opacity = '1';
      }
    });
    svg.addEventListener('mouseleave', function () {
      tip.style.display = 'none';
      if (hoverLine) hoverLine.style.opacity = '0';
    });
  });
})();
</script>"""


def render_html(**c):
    start, end, now = c["start"], c["end"], c["now"]
    pct = min(c["elapsed_h"] / max((end - start).total_seconds() / 3600.0, 0.001), 1.0)
    rem_h = int(c["remaining"] // 3600)
    rem_m = int((c["remaining"] % 3600) // 60)
    status = "Messung läuft" if c["remaining"] > 0 else "Messung abgeschlossen"

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
    ) or "<tr><td colspan='4' class='dim'>Noch keine Auffälligkeiten.</td></tr>"

    current_hour = now.replace(minute=0, second=0, microsecond=0)
    last24_rows = "".join(
        f"<tr><td>{b.astimezone().strftime('%d.%m. %H:%M')}"
        + (' <span class="live-tag">läuft</span>' if b == current_hour else '') + "</td>"
        f"<td class='num'>{human_bytes(d)}</td><td class='num'>{human_bytes(u)}</td>"
        f"<td class='num strong'>{human_bytes(d + u)}</td></tr>"
        for b, d, u in sorted(c["last24"], key=lambda item: item[0], reverse=True)
    ) or "<tr><td colspan='4' class='dim'>Noch keine Daten in den letzten 24 Stunden.</td></tr>"

    return f"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WAN-Volumen {c['device']}</title>
<style>
{BASE_CSS}
  .detail-link {{ font-size: 13px; color: var(--down); text-decoration: none; }}
  .detail-link:hover {{ text-decoration: underline; }}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>WAN-Volumen {c['device']}</h1>
    <div class="sub"><a class="detail-link" href="index.html">&larr; Übersicht aller Konsolen</a> &nbsp;&middot;&nbsp;
      {status} &nbsp;&middot;&nbsp; Fenster {start.astimezone().strftime('%d.%m.%Y %H:%M')}
      bis {end.astimezone().strftime('%d.%m.%Y %H:%M')} &nbsp;&middot;&nbsp;
      Restlaufzeit {rem_h} h {rem_m} min &nbsp;&middot;&nbsp;
      Stand {now.astimezone().strftime('%d.%m.%Y %H:%M')} &nbsp;&middot;&nbsp;
      nächster Refresh in <span id="refresh-cd">{REPORT_REFRESH_S // 60}:00</span></div>
    <div class="bar"><span style="width:{pct * 100:.1f}%"></span></div>
  </header>

  <div class="grid-cards">
    <div class="card"><div class="label">Bisher gemessen</div>
      <div class="value{total_alert_class(c['total'])}">{human_bytes(c['total'])}</div>
      <div class="foot">über {c['elapsed_h']:.1f} Stunden, {c['covered_h']} Stunden mit Daten</div></div>
    <div class="card"><div class="label">Pro Tag</div>
      <div class="value">{human_bytes(c['per_day'])}</div>
      <div class="foot">laufender Mittelwert</div></div>
    <div class="card"><div class="label">Hochrechnung 30 Tage</div>
      <div class="value">{human_bytes(c['per_month'])}</div>
      <div class="foot">bei gleichbleibender Grundlast</div></div>
    <div class="card"><div class="label">Verhältnis</div>
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

  <h2>Traffic-Flow (Rate je Messpunkt)</h2>
  <div class="panel">
    {c['flow_chart']}
    <div class="legend">
      <span><span class="dot" style="background:var(--down)"></span>Download (kbps)</span>
      <span><span class="dot" style="background:var(--up)"></span>Upload (kbps)</span>
      <span>{c['flow_points']} Messpunkte, Pollintervall ~{c['flow_interval_min']} Min</span>
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

  <h2>Letzte 24 Stunden</h2>
  <div class="panel">
    <table>
      <thead><tr><th>Stunde</th><th class="num">Download</th><th class="num">Upload</th>
        <th class="num">Gesamt</th></tr></thead>
      <tbody>{last24_rows}</tbody>
    </table>
  </div>

  <h2>Größte Stunden</h2>
  <div class="panel">
    <table>
      <thead><tr><th>Stunde</th><th class="num">Download</th><th class="num">Upload</th>
        <th class="num">Gesamt</th></tr></thead>
      <tbody>{spike_rows}</tbody>
    </table>
    <p class="dim" style="margin:14px 0 0;font-size:13px">Ausschläge deutlich über der Grundlast
      stammen erfahrungsgemäß von Speedtests, Firmware- oder Signatur-Downloads. Für die reine
      Management-Grundlast diese Stunden abziehen.</p>
  </div>

  <footer>Datenquelle: UniFi Network API (Live-Uplink-Rate via Site-Manager-Connector-Proxy),
    {len(c['window'])} Messpunkte im Fenster. Seite aktualisiert sich alle {REPORT_REFRESH_S // 60} Minuten selbst.</footer>
</div>
{refresh_countdown_script(now)}
{flow_tooltip_script()}
</body>
</html>"""


def render_overview_html(consoles, start, end, now):
    """Übersichtsseite: eine Karte pro Konsole (Kernzahlen + Mini-Chart),
    Link zur jeweiligen Detailseite. consoles = Liste von compute_stats()-dicts."""
    remaining = max((end - now).total_seconds(), 0)
    rem_h = int(remaining // 3600)
    rem_m = int((remaining % 3600) // 60)
    pct = min((min(now, end) - start).total_seconds() / max((end - start).total_seconds(), 0.001), 1.0)
    status = "Messung läuft" if remaining > 0 else "Messung abgeschlossen"
    current_hour = now.replace(minute=0, second=0, microsecond=0)

    total_all = sum(c["total"] for c in consoles)
    per_day_all = sum(c["per_day"] for c in consoles)
    per_month_all = sum(c["per_month"] for c in consoles)

    cards = []
    for c in consoles:
        live_badge = ""
        has_current_point = any(r["ts"] >= current_hour for r in c["window"])
        if has_current_point:
            live_badge = '<span class="live-tag">läuft</span>'
        filename = f"{c['device']}.html"
        cards.append(f"""<div class="card console-card">
      <div class="card-head"><h3>{c['device']}</h3>{live_badge}</div>
      <div class="mini-grid">
        <div><div class="mlabel">Bisher</div><div class="mvalue{total_alert_class(c['total'])}">{human_bytes(c['total'])}</div></div>
        <div><div class="mlabel">Pro Tag</div><div class="mvalue">{human_bytes(c['per_day'])}</div></div>
        <div><div class="mlabel">30 Tage</div><div class="mvalue">{human_bytes(c['per_month'])}</div></div>
      </div>
      <div class="mini-charts">
        <div class="mini-chart-col">
          <div class="mini-chart-label">Stunde</div>
          {c['chart']}
        </div>
        <div class="mini-chart-col">
          <div class="mini-chart-label">Flow</div>
          {c['flow_chart']}
        </div>
      </div>
      <div class="legend small">
        <span><span class="dot" style="background:var(--down)"></span>Down</span>
        <span><span class="dot" style="background:var(--up)"></span>Up</span>
        <span>Spitze {human_bytes(c['peak'])}/h</span>
      </div>
      <a class="detail-link" href="{filename}">Details &rarr;</a>
    </div>""")
    cards_html = "\n    ".join(cards) or '<p class="dim">Keine Konsolen konfiguriert.</p>'

    return f"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WAN-Volumen Übersicht</title>
<style>
{BASE_CSS}
  .wrap {{ max-width: 1840px; }}
  header {{ margin-bottom: 16px; padding-bottom: 12px; }}
  .totals {{ margin-top: 4px; }}
  .totals b {{ color: #fff; font-weight: 600; }}
  .overview-grid {{ display: grid; gap: 20px; grid-template-columns: repeat(3, 1fr); }}
  @media (max-width: 900px) {{ .overview-grid {{ grid-template-columns: repeat(2, 1fr); }} }}
  @media (max-width: 600px) {{ .overview-grid {{ grid-template-columns: 1fr; }} }}
  .console-card {{ display: flex; flex-direction: column; gap: 11px; padding: 21px 23px; }}
  .card-head {{ display: flex; align-items: center; gap: 8px; }}
  .card-head h3 {{ margin: 0; font-size: 16px; font-weight: 600; }}
  .mini-grid {{ display: flex; gap: 20px; }}
  .mlabel {{ color: var(--dim); font-size: 11px; text-transform: uppercase; letter-spacing: .07em; }}
  .mvalue {{ font: 600 18px/1.25 ui-monospace, monospace; margin-top: 2px; }}
  .mini-charts {{ display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-top: 3px; }}
  .mini-chart-col .chart {{ height: 140px; }}
  .mini-chart-label {{ color: var(--dim); font-size: 10px; text-transform: uppercase;
    letter-spacing: .07em; margin-bottom: 2px; }}
  .console-card .legend {{ font-size: 12px; margin-top: 2px; }}
  .console-card .detail-link {{ font-size: 12.5px; margin-top: 2px; }}
  .console-card .legend {{ font-size: 13px; margin-top: 6px; }}
  .console-card .detail-link {{ font-size: 14px; margin-top: 6px; }}
  .console-card .legend {{ margin-top: 0; }}
  .detail-link {{ align-self: flex-start; font-size: 12.5px; color: var(--down); text-decoration: none; margin-top: 1px; }}
  .detail-link:hover {{ text-decoration: underline; }}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>WAN-Volumen Übersicht</h1>
    <div class="sub">{status} &nbsp;&middot;&nbsp; Fenster {start.astimezone().strftime('%d.%m.%Y %H:%M')}
      bis {end.astimezone().strftime('%d.%m.%Y %H:%M')} &nbsp;&middot;&nbsp;
      Restlaufzeit {rem_h} h {rem_m} min &nbsp;&middot;&nbsp;
      Stand {now.astimezone().strftime('%d.%m.%Y %H:%M')} &nbsp;&middot;&nbsp;
      nächster Refresh in <span id="refresh-cd">{REPORT_REFRESH_S // 60}:00</span></div>
    <div class="sub totals">Alle Konsolen: <b>{human_bytes(total_all)}</b> bisher &nbsp;&middot;&nbsp;
      <b>{human_bytes(per_day_all)}</b>/Tag &nbsp;&middot;&nbsp;
      <b>{human_bytes(per_month_all)}</b> Hochrechnung/30 Tage</div>
    <div class="bar"><span style="width:{pct * 100:.1f}%"></span></div>
  </header>

  <div class="overview-grid">
    {cards_html}
  </div>

  <footer>Datenquelle: UniFi Network API (Live-Uplink-Rate via Site-Manager-Connector-Proxy),
    {len(consoles)} Konsolen. Seite aktualisiert sich alle {REPORT_REFRESH_S // 60} Minuten selbst.</footer>
</div>
{refresh_countdown_script(now)}
{flow_tooltip_script()}
</body>
</html>"""


def write_report(rows, start, end, site_filter=None):
    html = build_report(rows, start, end, site_filter)
    with open(HTML_PATH, "w", encoding="utf-8") as handle:
        handle.write(html)
    return HTML_PATH


def write_reports(rows, start, end, console_names):
    """Schreibt die Übersichtsseite (wan_report.html) plus eine Detailseite
    pro Konsole ({Konsolenname}.html)."""
    overview_html = build_overview(rows, start, end, console_names)
    with open(HTML_PATH, "w", encoding="utf-8") as handle:
        handle.write(overview_html)
    detail_paths = []
    for name in console_names:
        detail_html = build_report(rows, start, end, site_filter=name)
        path = os.path.join(DATA_DIR, f"{name}.html")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(detail_html)
        detail_paths.append(path)
    return HTML_PATH, detail_paths


# ----------------------------------------------------------------------------
# Ablauf
# ----------------------------------------------------------------------------

def poll(rows, targets, interval_s):
    """Ein Live-Sample der aktuellen Uplink-Rate pro Konsole, hochgerechnet auf
    interval_s. targets = Liste von (console_name, host_id, site_id, device_id).
    Alle Konsolen bekommen denselben Zeitstempel (ein Poll-Durchlauf), das haelt
    sie fuer Vergleiche synchron und braucht nur einen CSV-/Git-Commit pro Lauf.

    Hinweis: Die isp-metrics-API lieferte nachweislich falsche Werte (Faktor
    ~5000 gegenueber dem GUI-Traffic-Graphen). Diese Funktion nutzt stattdessen
    den Site-Manager-Connector-Proxy zur lokalen Network-API jeder Konsole, der
    echte Live-Uplink-Raten liefert (verifiziert gegen den GUI-Graphen).
    """
    now = datetime.now(timezone.utc)
    points = []
    for console_name, host_id, site_id, device_id in targets:
        rx_bps, tx_bps = get_uplink_rates(host_id, site_id, device_id)
        points.append({
            "ts": now,
            "site": console_name,
            "uplink": "wan",
            "interval_s": interval_s,
            "down_bytes": round(rx_bps / 8.0 * interval_s),
            "up_bytes": round(tx_bps / 8.0 * interval_s),
        })
    return merge_rows(rows, points)


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
    print(f"Vollständige Rohdaten in {RAW_PATH}")
    print("Hinweis: isp-metrics wird nicht mehr genutzt (lieferte falsche Werte, "
          "siehe Support-Ticket). Diese Live-Rate via Connector-Proxy ist die "
          "verifizierte Datenquelle.")


def main():
    parser = argparse.ArgumentParser(description="WAN-Volumen mehrerer UDM Pro messen")
    parser.add_argument("--start", help="Messbeginn, z.B. 2026-08-07T12:00 (lokale Zeit)")
    parser.add_argument("--days", type=float, default=3.0, help="Messdauer in Tagen")
    parser.add_argument("--interval", type=int, default=900, help="Pollintervall in Sekunden")
    parser.add_argument("--site", help="Bericht auf einen einzelnen Konsolennamen beschränken")
    parser.add_argument("--host-id", default=None,
                         help="Nur diese eine Konsole pollen/berichten (Site-Manager hostId). "
                              "Ohne Angabe: alle Konsolen aus MONITORED_HOSTS (Übersicht + Details).")
    parser.add_argument("--discover", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args()

    if args.discover:
        discover(args.host_id or DEFAULT_HOST_ID)
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

    if args.host_id:
        name = args.host_id
        for host in api_get("/hosts").get("data", []):
            if host.get("id") == args.host_id:
                name = host.get("reportedState", {}).get("name", args.host_id)
                break
        targets_cfg = [(name, args.host_id)]
    else:
        targets_cfg = MONITORED_HOSTS
    console_names = [name for name, _ in targets_cfg]
    single = args.site or (len(console_names) == 1 and console_names[0])

    if args.report:
        rows = load_rows()
        if single:
            print(f"Bericht geschrieben: {write_report(rows, start, end, single)}")
        else:
            index_path, detail_paths = write_reports(rows, start, end, console_names)
            print(f"Übersicht geschrieben: {index_path}")
            for p in detail_paths:
                print(f"  Detail: {p}")
        return

    targets = []
    for name, host_id in targets_cfg:
        site_id, device_id = find_gateway_device(host_id)
        targets.append((name, host_id, site_id, device_id))
        print(f"Ziel: {name} (site_id={site_id}, device_id={device_id})")

    rows = load_rows()
    while True:
        rows, added = poll(rows, targets, args.interval)
        if single:
            path = write_report(rows, start, end, single)
        else:
            path, _ = write_reports(rows, start, end, console_names)
        stamp = datetime.now().astimezone().strftime("%H:%M:%S")
        print(f"[{stamp}] {added} neue Messpunkte, {len(rows)} gesamt -> {path}")
        if args.once or datetime.now(timezone.utc) >= end:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
