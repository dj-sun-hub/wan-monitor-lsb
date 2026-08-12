#!/usr/bin/env python3
"""
UDM Pro WAN-Traffic-Monitor
===========================

Holt WAN-Kennzahlen ueber die UniFi Site Manager API (api.ui.com), schreibt sie
fortlaufend in eine CSV und erzeugt nach jedem Durchlauf einen aktuellen
HTML-Bericht. Dauerbetrieb ohne festes Messende: Kennzahlen sind aktueller
Kalendermonat, rollierende letzte 30 Tage, und Gesamt seit dem einmaligen
Messbeginn.

Nur Standardbibliothek, keine Installation noetig.

Der API-Key wird aus der Umgebungsvariable UI_API_KEY gelesen und niemals in
Dateien geschrieben.

Aufrufe
-------
  python3 udm_wan_monitor.py --discover
      Zeigt Hosts, Sites und ein Rohdaten-Sample. Einmalig zum Pruefen.

  python3 udm_wan_monitor.py --loop
      Dauerbetrieb: pollt endlos alle --interval Sekunden und schreibt nach
      jedem Poll den Bericht neu (Strg+C zum Beenden).

  python3 udm_wan_monitor.py --once
      Ein einzelner Poll plus Bericht. Fuer Cron oder Aufgabenplaner.

  python3 udm_wan_monitor.py --report
      Nur Bericht aus vorhandener CSV, ohne API-Zugriff.
"""

import argparse
import calendar
import csv
import heapq
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
REPORT_REFRESH_S = 60  # Seite laedt sich automatisch neu, siehe <meta refresh> und Countdown

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
                # Betrifft ALLE Konsolen gleichermassen (kaputter/abgelaufener
                # Key) - hier bewusst laut abbrechen statt jede Konsole
                # einzeln "unerreichbar" zu melden.
                sys.exit("401: API-Key wird abgelehnt (Connector-Proxy).")
            if exc.code == 403:
                raise RuntimeError(
                    f"403: Key hat keinen Zugriff auf Konsole {host_id} (Connector-Proxy)."
                )
            # Alles andere (z.B. 404 "device_offline") ist typischerweise ein
            # Problem EINER einzelnen Konsole - normale Exception werfen,
            # damit der Aufrufer nur diese Konsole ueberspringen kann, statt
            # den kompletten Poll-Durchlauf fuer ALLE Konsolen abzubrechen.
            raise RuntimeError(f"HTTP {exc.code} beim Connector-Proxy {path}: {body}")
        except urllib.error.URLError as exc:
            if attempt < 2:
                time.sleep(10)
                continue
            raise RuntimeError(f"Keine Verbindung zu api.ui.com (Connector-Proxy): {exc.reason}")
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
    raise RuntimeError(f"Gateway-Gerät für Host {host_id} nicht in Network-API gefunden.")


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


def human_kbps(value):
    """Aktuelle Live-Rate (Down+Up kombiniert), kurz formatiert fuer die
    Kachel-Anzeige neben dem Gesamt-Wert."""
    if value >= 1000:
        return f"{value / 1000:.1f}".replace(".", ",") + " Mbps"
    return f"{value:.0f} kbps"


GIB = 1024 ** 3
WARN_THRESHOLD_BYTES = 1 * GIB
ALERT_THRESHOLD_BYTES = 4.8 * GIB
# Live kalibriert: Grundlast lag im Ernstfall bei KNZ im einstelligen
# kbps-Bereich, der echte LTE-Failover-Ausschlag bei 400-877 kbps.
FAILOVER_THRESHOLD_KBPS = 500.0
# Einzelne kurze Ausschlaege (Speedtest, Firmware-/Signatur-Download) sollen
# keinen Fehlalarm ausloesen. Erst wenn die letzten FAILOVER_CONSECUTIVE Polls
# IN FOLGE ueber dem Schwellwert liegen, gilt Failover als bestaetigt - bei
# 1-Minuten-Takt sind das 2 Minuten Verzoegerung, kaum spuerbar langsamer als
# vorher (1 Poll), aber deutlich weniger anfaellig fuer Einzel-Spitzen.
FAILOVER_CONSECUTIVE = 2
FAILOVER_EXCLUDED_DEVICES = set()
# Ab wann eine Konsole als "offline/nicht erreichbar" statt nur "kurz kein
# Update" gilt (5x der 1-Minuten-Pollintervall Toleranz fuer vereinzelt
# uebersprungene Laeufe, siehe poll()/find_gateway_device() Fehlerbehandlung).
OFFLINE_THRESHOLD_S = 300

# Dauerbetrieb: Stundenchart/Flow-Chart und die Tageswerte-Tabelle bleiben auf
# ein recentes Fenster begrenzt, sonst werden sie nach Wochen/Monaten Laufzeit
# unbrauchbar gross. Kennzahlen (Monat/30 Tage/Gesamt) sind davon unabhaengig.
CHART_WINDOW_DAYS = 7
TABLE_WINDOW_DAYS = 30


def total_alert_class(total_bytes):
    """CSS-Klassen-Zusatz fuer den 'Bisher'-Wert: gelb ab 1 GB, rot ab 4.8 GB."""
    if total_bytes > ALERT_THRESHOLD_BYTES:
        return " value-alert"
    if total_bytes > WARN_THRESHOLD_BYTES:
        return " value-warn"
    return ""


def compute_stats(rows, start, site_filter=None):
    """Berechnet alle Kennzahlen fuer eine Konsole (oder alle, falls site_filter
    leer) und liefert sie als dict zurueck - roh, ohne HTML. Wird sowohl fuer
    Detailseiten (render_html) als auch für Karten der Übersichtsseite
    (render_overview_html) genutzt.

    Dauerbetrieb (kein festes Messende mehr): start ist der einmalige
    Messbeginn, es gibt kein "end". Statt einem einzelnen Gesamtfenster gibt
    es drei Kennzahlen nebeneinander - aktueller Kalendermonat, rollierende
    letzte 30 Tage, und Gesamt seit Start. Stundenchart/Flow-Chart/Tageswerte
    bleiben auf ein kuerzeres, recentes Fenster begrenzt (CHART_WINDOW_DAYS /
    TABLE_WINDOW_DAYS), sonst wuerden sie nach Wochen/Monaten Laufzeit riesig
    und unbrauchbar.
    """
    now = datetime.now(timezone.utc)

    def in_site(r):
        if not site_filter:
            return True
        needle = site_filter.lower()
        return needle in r["site"].lower() or needle in r["uplink"].lower()

    all_rows = [r for r in rows if r["ts"] >= start and in_site(r)]

    total_down = sum(r["down_bytes"] for r in all_rows)
    total_up = sum(r["up_bytes"] for r in all_rows)
    total = total_down + total_up

    # Aktueller Kalendermonat (lokale Zeit, damit "Monat" dem echten
    # Kalendermonat entspricht, nicht UTC).
    now_local = now.astimezone()
    month_start_local = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_start = month_start_local.astimezone(timezone.utc)
    month_rows = [r for r in all_rows if r["ts"] >= month_start]
    total_month_down = sum(r["down_bytes"] for r in month_rows)
    total_month_up = sum(r["up_bytes"] for r in month_rows)
    total_month = total_month_down + total_month_up
    # Fortschrittsbalken/"Tag X von Y" soll den echten Kalendertag zeigen,
    # unabhaengig davon, ob fuer alle Tage schon Daten vorliegen.
    days_elapsed_month_calendar = max((now - month_start).total_seconds() / 86400.0, 0.001)
    days_in_month = calendar.monthrange(now_local.year, now_local.month)[1]
    # Nenner fuer die Tagesrate (Hochrechnung) ist dagegen die Zeit seit
    # Messbeginn INNERHALB des Monats, nicht seit Kalendermonatsbeginn - sonst
    # wird in einem Monat, der bei Messbeginn schon laeuft, durch zu viele
    # "Nulltage" (vor Messbeginn) geteilt und die Hochrechnung faellt
    # kuenstlich viel zu niedrig aus.
    month_data_start = max(month_start, start)
    days_elapsed_month = max((now - month_data_start).total_seconds() / 86400.0, 0.001)
    per_day_month = total_month / days_elapsed_month
    projected_month = per_day_month * days_in_month

    # Rollierende letzte 30 Tage: sobald wirklich 30 Tage Messhistorie
    # vorliegen, die echte gemessene Summe. Vorher (erste 30 Tage nach
    # Messbeginn) waere die "rollierende" Summe nur ein unvollstaendiger
    # Ausschnitt und faelschlich identisch zum Monatswert - stattdessen wird
    # anhand der Tagesrate im aktuellen Monat auf 30 Tage hochgerechnet.
    d30_start = now - timedelta(days=30)
    if start > d30_start:
        total_30d = per_day_month * 30.0
    else:
        d30_rows = [r for r in all_rows if r["ts"] >= d30_start]
        total_30d = sum(r["down_bytes"] + r["up_bytes"] for r in d30_rows)
    per_day_30d = total_30d / 30.0

    # Failover-Verdacht: die letzten FAILOVER_CONSECUTIVE Messpunkte IN FOLGE
    # ueber dem Schwellwert (kbps), damit ein einzelner kurzer Ausschlag
    # (Speedtest, Download) keinen Fehlalarm ausloest. Schwellwert live am
    # NAS-Failover-Monitor kalibriert, bevor der durchs GitHub-Pages-Board
    # ersetzt wurde.
    is_failover = False
    last_rate_kbps = 0.0
    if all_rows and site_filter not in FAILOVER_EXCLUDED_DEVICES:
        recent = heapq.nlargest(FAILOVER_CONSECUTIVE, all_rows, key=lambda r: r["ts"])
        recent.sort(key=lambda r: r["ts"])  # chronologisch, aeltester zuerst
        rates = []
        for r in recent:
            if r["interval_s"]:
                rates.append((r["down_bytes"] + r["up_bytes"]) * 8.0 / r["interval_s"] / 1000.0)
            else:
                rates.append(0.0)
        if rates:
            last_rate_kbps = rates[-1]
        if len(rates) == FAILOVER_CONSECUTIVE:
            is_failover = all(rate > FAILOVER_THRESHOLD_KBPS for rate in rates)

    # Offline/nicht erreichbar (z.B. von poll() uebersprungen, siehe dortige
    # Fehlerbehandlung): der letzte BEKANNTE Wert kann veraltet sein und
    # faelschlich noch "Failover" zeigen. Stattdessen wie 0 kbps behandeln -
    # dann greift automatisch die graue "idle"-Darstellung der Kachel.
    last_seen = max((r["ts"] for r in all_rows), default=None)
    is_offline = last_seen is None or (now - last_seen).total_seconds() > OFFLINE_THRESHOLD_S
    if is_offline:
        last_rate_kbps = 0.0
        is_failover = False

    # Stundenchart & Flow-Chart: nur die letzten CHART_WINDOW_DAYS Tage.
    chart_start = max(start, now - timedelta(days=CHART_WINDOW_DAYS))
    chart_rows = [r for r in all_rows if r["ts"] >= chart_start]

    hours = {}
    for row in chart_rows:
        bucket = row["ts"].replace(minute=0, second=0, microsecond=0)
        entry = hours.setdefault(bucket, [0.0, 0.0])
        entry[0] += row["down_bytes"]
        entry[1] += row["up_bytes"]

    chart_start_hour = chart_start.replace(minute=0, second=0, microsecond=0)
    total_hours = int((now - chart_start_hour).total_seconds() // 3600) + 1
    series = []
    for i in range(total_hours):
        bucket = chart_start_hour + timedelta(hours=i)
        down, up = hours.get(bucket, (0.0, 0.0))
        series.append((bucket, down, up))

    peak = max((d + u for _, d, u in series), default=0.0) or 1.0

    # Tageswerte-Tabelle: eigenes, laengeres Fenster (TABLE_WINDOW_DAYS).
    table_start = max(start, now - timedelta(days=TABLE_WINDOW_DAYS))
    table_rows = [r for r in all_rows if r["ts"] >= table_start]
    days = {}
    hours_by_day = {}
    for row in table_rows:
        local_day = row["ts"].astimezone().strftime("%d.%m.%Y")
        entry = days.setdefault(local_day, [0.0, 0.0, 0])
        entry[0] += row["down_bytes"]
        entry[1] += row["up_bytes"]
        hours_by_day.setdefault(local_day, set()).add(
            row["ts"].replace(minute=0, second=0, microsecond=0))
    for day, hset in hours_by_day.items():
        days[day][2] = len(hset)

    # Ausreisser (innerhalb des Chart-Fensters)
    spikes = sorted(series, key=lambda item: item[1] + item[2], reverse=True)[:5]
    spikes = [s for s in spikes if (s[1] + s[2]) > 0]

    # Letzte 24 Stunden, Einzelauflistung
    last24_start = now - timedelta(hours=24)
    last24 = [s for s in series if last24_start <= s[0] <= now]

    chart = render_chart(series, peak, chart_start_hour)
    flow_chart = render_flow_chart(chart_rows, chart_start, now)
    flow_points = len(chart_rows)
    flow_intervals = sorted({r["interval_s"] for r in chart_rows if r["interval_s"]})
    flow_interval_min = round(flow_intervals[0] / 60) if flow_intervals else 15
    device = site_filter or (all_rows[0]["site"] if all_rows else (rows[0]["site"] if rows else "unbekannt"))
    return dict(
        window=all_rows, total=total, total_down=total_down, total_up=total_up,
        total_month=total_month, per_day_month=per_day_month, projected_month=projected_month,
        days_elapsed_month=days_elapsed_month, days_in_month=days_in_month,
        days_elapsed_month_calendar=days_elapsed_month_calendar,
        total_30d=total_30d, per_day_30d=per_day_30d,
        days=days, spikes=spikes,
        chart=chart, start=start, now=now, peak=peak, device=device,
        flow_chart=flow_chart, flow_points=flow_points, flow_interval_min=flow_interval_min,
        last24=last24, is_failover=is_failover, last_rate_kbps=last_rate_kbps, is_offline=is_offline,
    )


def build_report(rows, start, site_filter=None):
    return render_html(**compute_stats(rows, start, site_filter))


def build_overview(rows, start, console_names):
    now = datetime.now(timezone.utc)
    consoles = [compute_stats(rows, start, site_filter=name) for name in console_names]
    return render_overview_html(consoles=consoles, start=start, now=now)


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

    # Laufender Durchschnitt (kumulativer Mittelwert bis zu jedem Punkt) - passt
    # sich mit fortschreitender Zeit an, statt eine starre Gesamt-Durchschnittslinie
    # zu sein. Werte werden auch fuer den Hover-Tooltip mitgefuehrt.
    avg_down_line, avg_up_line, avgs = [], [], []
    sum_d = sum_u = 0.0
    for i, (ts, d, u) in enumerate(samples):
        sum_d += d
        sum_u += u
        avg_d, avg_u = sum_d / (i + 1), sum_u / (i + 1)
        avg_down_line.append(xy(ts, avg_d))
        avg_up_line.append(xy(ts, avg_u))
        avgs.append((avg_d, avg_u))
    avg_down_pts_str = " ".join(f"{x:.1f},{y:.1f}" for x, y in avg_down_line)
    avg_up_pts_str = " ".join(f"{x:.1f},{y:.1f}" for x, y in avg_up_line)

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
    parts.append(f'<polyline points="{avg_down_pts_str}" class="flow-avg-down-line"/>')
    parts.append(f'<polyline points="{avg_up_pts_str}" class="flow-avg-up-line"/>')
    parts.append(f'<line x1="{left}" y1="{baseline_y:.1f}" x2="{left + plot_w}" y2="{baseline_y:.1f}" class="baseline"/>')
    # Hover-Linie: unsichtbar per Default, wird von flow_tooltip_script beim Hovern
    # an die x-Position des naechstgelegenen Messpunkts verschoben und eingeblendet.
    parts.append(f'<line class="hover-line" x1="0" y1="12" x2="0" y2="{baseline_y:.1f}"/>')

    # Rohdaten fuer den Hover-Tooltip (siehe flow_tooltip_script): Zeitstempel, Rate,
    # laufender Durchschnitt und die exakte x-Pixel-Position je Punkt (fuer die
    # Hover-Linie).
    samples_json = json.dumps([
        [ts.isoformat(), round(d, 1), round(u, 1), round(x, 1), round(avg_d, 1), round(avg_u, 1)]
        for (ts, d, u), (x, _y), (avg_d, avg_u) in zip(samples, down_line, avgs)
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
  .chart .flow-avg-down-line { fill: none; stroke: var(--down); stroke-width: 1.1;
    stroke-dasharray: 5 3; opacity: .8; }
  .chart .flow-avg-up-line { fill: none; stroke: var(--up); stroke-width: 1.1;
    stroke-dasharray: 5 3; opacity: .8; }
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
  .flow-tooltip-avg { color: var(--dim); display: inline-block; margin-top: 3px;
    padding-top: 3px; border-top: 1px dashed var(--line); }
  .live-tag { display: inline-block; font-size: 10.5px; text-transform: uppercase;
    letter-spacing: .06em; color: var(--down); border: 1px solid var(--down);
    border-radius: 3px; padding: 1px 5px; margin-left: 6px; vertical-align: middle; }
  .failover-tag { display: inline-block; font-size: 10.5px; text-transform: uppercase;
    letter-spacing: .06em; color: #fff; background: var(--alert); border: 1px solid var(--alert);
    border-radius: 3px; padding: 1px 6px; margin-left: 6px; vertical-align: middle; font-weight: 600; }
  .offline-tag { display: inline-block; font-size: 10.5px; text-transform: uppercase;
    letter-spacing: .06em; color: var(--dim); border: 1px solid var(--dim);
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
        '<br>Up: ' + fmtKbps(s[2]) +
        '<br><span class="flow-tooltip-avg">Ø Down: ' + fmtKbps(s[4]) +
        '<br>Ø Up: ' + fmtKbps(s[5]) + '</span>';
      var x = ev.clientX + 16, y = ev.clientY + 16;
      if (x + 170 > window.innerWidth) x = ev.clientX - 186;
      if (y + 92 > window.innerHeight) y = ev.clientY - 108;
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
    start, now = c["start"], c["now"]
    pct = min(c["days_elapsed_month_calendar"] / max(c["days_in_month"], 0.001), 1.0)
    running_days = max((now - start).days, 0)

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
<title>WAN-Failover {c['device']}</title>
<style>
{BASE_CSS}
  .detail-link {{ font-size: 13px; color: var(--down); text-decoration: none; }}
  .detail-link:hover {{ text-decoration: underline; }}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>WAN-Failover {c['device']}{' <span class="failover-tag">FAILOVER VERMUTET</span>' if c['is_failover'] else ''}</h1>
    <div class="sub"><a class="detail-link" href="index.html">&larr; Übersicht aller Konsolen</a> &nbsp;&middot;&nbsp;
      Dauerbetrieb, läuft seit {start.astimezone().strftime('%d.%m.%Y %H:%M')} ({running_days} Tage) &nbsp;&middot;&nbsp;
      Stand {now.astimezone().strftime('%d.%m.%Y %H:%M')} &nbsp;&middot;&nbsp;
      nächster Refresh in <span id="refresh-cd">{REPORT_REFRESH_S // 60}:00</span></div>
    <div class="bar"><span style="width:{pct * 100:.1f}%"></span></div>
    <div class="dim" style="font-size:11.5px;margin-top:3px">Balken: Fortschritt im aktuellen Kalendermonat
      (Tag {int(c['days_elapsed_month_calendar']) + 1} von {c['days_in_month']})</div>
  </header>

  <div class="grid-cards">
    <div class="card"><div class="label">Aktueller Monat</div>
      <div class="value{total_alert_class(c['total_month'])}">{human_bytes(c['total_month'])}</div>
      <div class="foot">Hochrechnung Monatsende: {human_bytes(c['projected_month'])}</div></div>
    <div class="card"><div class="label">Letzte 30 Tage</div>
      <div class="value">{human_bytes(c['total_30d'])}</div>
      <div class="foot">Ø {human_bytes(c['per_day_30d'])}/Tag</div></div>
    <div class="card"><div class="label">Gesamt seit Start (nur Failover-Traffic)</div>
      <div class="value">{human_bytes(c['total'])}</div>
      <div class="foot">seit {start.astimezone().strftime('%d.%m.%Y')}</div></div>
    <div class="card"><div class="label">Verhältnis (gesamt)</div>
      <div class="value">{human_bytes(c['total_down'])}</div>
      <div class="foot">Download, dazu {human_bytes(c['total_up'])} Upload</div></div>
  </div>

  <h2>Stundenvolumen (letzte {CHART_WINDOW_DAYS} Tage)</h2>
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
      <span class="dim">- - - laufender Durchschnitt</span>
      <span>{c['flow_points']} Messpunkte, Pollintervall ~{c['flow_interval_min']} Min</span>
    </div>
  </div>

  <h2>Tageswerte (letzte {TABLE_WINDOW_DAYS} Tage)</h2>
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
    {len(c['window'])} Messpunkte seit Start. Seite aktualisiert sich alle {REPORT_REFRESH_S // 60} Minute{'n' if REPORT_REFRESH_S // 60 != 1 else ''} selbst.</footer>
</div>
{refresh_countdown_script(now)}
{flow_tooltip_script()}
</body>
</html>"""


def render_overview_html(consoles, start, now):
    """Übersichtsseite: eine Karte pro Konsole (Kernzahlen + Mini-Chart),
    Link zur jeweiligen Detailseite. consoles = Liste von compute_stats()-dicts."""
    running_days = max((now - start).days, 0)
    days_elapsed_month_calendar = consoles[0]["days_elapsed_month_calendar"] if consoles else 1
    days_in_month = consoles[0]["days_in_month"] if consoles else 30
    pct = min(days_elapsed_month_calendar / max(days_in_month, 0.001), 1.0)
    current_hour = now.replace(minute=0, second=0, microsecond=0)

    total_month_all = sum(c["total_month"] for c in consoles)
    total_30d_all = sum(c["total_30d"] for c in consoles)
    total_all = sum(c["total"] for c in consoles)

    cards = []
    for c in consoles:
        live_badge = ""
        has_current_point = any(r["ts"] >= current_hour for r in c["window"])
        if has_current_point and not c["is_offline"]:
            live_badge = '<span class="live-tag">läuft</span>'
        failover_badge = '<span class="failover-tag">FAILOVER</span>' if c["is_failover"] else ""
        offline_badge = '<span class="offline-tag">OFFLINE</span>' if c["is_offline"] else ""
        if c["is_failover"]:
            card_class = "card console-card failover"
        elif c["is_offline"] or c["last_rate_kbps"] <= 0:
            card_class = "card console-card idle"
        else:
            card_class = "card console-card"
        filename = f"{c['device']}.html"
        cards.append(f"""<div class="{card_class}">
      <div class="card-head"><h3>{c['device']}</h3>{live_badge}{failover_badge}{offline_badge}</div>
      <div class="mini-grid">
        <div><div class="mlabel">Monat</div><div class="mvalue{total_alert_class(c['total_month'])}">{human_bytes(c['total_month'])}</div></div>
        <div><div class="mlabel">30 Tage</div><div class="mvalue">{human_bytes(c['total_30d'])}</div></div>
        <div><div class="mlabel">Gesamt</div><div class="mvalue">{human_bytes(c['total'])}
          <span class="live-rate">Akt. Bandbreite: {human_kbps(c['last_rate_kbps'])}</span></div></div>
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
<title>WAN-Failover Übersicht</title>
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
  .console-card.failover {{ border-color: var(--alert); background: #2a1414; }}
  @media (prefers-reduced-motion: no-preference) {{
    .console-card.failover {{ animation: failover-blink 1.2s ease-in-out infinite; }}
  }}
  @keyframes failover-blink {{
    0%, 100% {{ border-color: var(--alert); background: #2a1414; }}
    50% {{ border-color: #ff6b6b; background: #3a1414; }}
  }}
  .console-card.idle {{ opacity: .5; filter: grayscale(85%); }}
  .card-head {{ display: flex; align-items: center; gap: 8px; }}
  .card-head h3 {{ margin: 0; font-size: 16px; font-weight: 600; }}
  .mini-grid {{ display: flex; gap: 20px; }}
  .mlabel {{ color: var(--dim); font-size: 11px; text-transform: uppercase; letter-spacing: .07em; }}
  .mvalue {{ font: 600 18px/1.25 ui-monospace, monospace; margin-top: 2px; }}
  .live-rate {{ font: 500 11.5px/1.25 ui-monospace, monospace; color: var(--dim);
    margin-left: 4px; white-space: nowrap; }}
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
    <h1>WAN-Failover Übersicht</h1>
    <div class="sub">Dauerbetrieb, läuft seit {start.astimezone().strftime('%d.%m.%Y %H:%M')} ({running_days} Tage) &nbsp;&middot;&nbsp;
      Stand {now.astimezone().strftime('%d.%m.%Y %H:%M')} &nbsp;&middot;&nbsp;
      nächster Refresh in <span id="refresh-cd">{REPORT_REFRESH_S // 60}:00</span></div>
    <div class="sub totals">Alle Konsolen: <b>{human_bytes(total_month_all)}</b> diesen Monat &nbsp;&middot;&nbsp;
      <b>{human_bytes(total_30d_all)}</b> letzte 30 Tage &nbsp;&middot;&nbsp;
      <b>{human_bytes(total_all)}</b> gesamt seit Start (nur Failover-Traffic)</div>
    <div class="bar"><span style="width:{pct * 100:.1f}%"></span></div>
    <div class="dim" style="font-size:11.5px;margin-top:3px">Balken: Fortschritt im aktuellen Kalendermonat
      (Tag {int(days_elapsed_month_calendar) + 1} von {days_in_month})</div>
  </header>

  <div class="overview-grid">
    {cards_html}
  </div>

  <footer>Datenquelle: UniFi Network API (Live-Uplink-Rate via Site-Manager-Connector-Proxy),
    {len(consoles)} Konsolen. Seite aktualisiert sich alle {REPORT_REFRESH_S // 60} Minute{'n' if REPORT_REFRESH_S // 60 != 1 else ''} selbst.</footer>
</div>
{refresh_countdown_script(now)}
{flow_tooltip_script()}
</body>
</html>"""


def write_report(rows, start, site_filter=None):
    html = build_report(rows, start, site_filter)
    with open(HTML_PATH, "w", encoding="utf-8") as handle:
        handle.write(html)
    return HTML_PATH


def write_reports(rows, start, console_names):
    """Schreibt die Übersichtsseite (wan_report.html) plus eine Detailseite
    pro Konsole ({Konsolenname}.html)."""
    overview_html = build_overview(rows, start, console_names)
    with open(HTML_PATH, "w", encoding="utf-8") as handle:
        handle.write(overview_html)
    detail_paths = []
    for name in console_names:
        detail_html = build_report(rows, start, site_filter=name)
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
        # Eine einzelne offline/nicht erreichbare Konsole darf nicht den
        # kompletten Poll-Durchlauf (und damit die Daten ALLER anderen
        # Konsolen) zum Absturz bringen - hier nur ueberspringen und weiter.
        try:
            rx_bps, tx_bps = get_uplink_rates(host_id, site_id, device_id)
        except Exception as exc:
            print(f"  {console_name}: Poll-Fehler, ueberspringe diesen Durchlauf - {exc}")
            continue
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
    parser = argparse.ArgumentParser(description="WAN-Volumen mehrerer UDM Pro messen (Dauerbetrieb)")
    parser.add_argument("--start", help="Messbeginn, z.B. 2026-08-07T12:00 (lokale Zeit). "
                                         "Nur beim allerersten Lauf relevant, danach aus monitor_state.json.")
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
    # Kein festes Messende mehr (Dauerbetrieb) - start wird nur einmalig gesetzt
    # und danach immer aus monitor_state.json uebernommen.
    with open(STATE_PATH, "w", encoding="utf-8") as handle:
        json.dump({"start": start.isoformat()}, handle)

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
            print(f"Bericht geschrieben: {write_report(rows, start, single)}")
        else:
            index_path, detail_paths = write_reports(rows, start, console_names)
            print(f"Übersicht geschrieben: {index_path}")
            for p in detail_paths:
                print(f"  Detail: {p}")
        return

    targets = []
    for name, host_id in targets_cfg:
        # Wie in poll(): eine einzelne offline Konsole darf nicht verhindern,
        # dass die restigen Konsolen ueberhaupt erst Daten bekommen.
        try:
            site_id, device_id = find_gateway_device(host_id)
        except Exception as exc:
            print(f"Ziel: {name}: uebersprungen, nicht erreichbar - {exc}")
            continue
        targets.append((name, host_id, site_id, device_id))
        print(f"Ziel: {name} (site_id={site_id}, device_id={device_id})")

    rows = load_rows()
    while True:
        rows, added = poll(rows, targets, args.interval)
        if single:
            path = write_report(rows, start, single)
        else:
            path, _ = write_reports(rows, start, console_names)
        stamp = datetime.now().astimezone().strftime("%H:%M:%S")
        print(f"[{stamp}] {added} neue Messpunkte, {len(rows)} gesamt -> {path}")
        if args.once:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
