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
from collections import deque
import heapq
import html
import json
import math
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


def _connector_request(host_id, url_path, timeout=30):
    """Gemeinsame GET-Ausfuehrung + Fehlerbehandlung/Retry fuer den Site-
    Manager-Connector-Proxy - genutzt sowohl von der offiziellen Integration-
    API (connector_get) als auch von der klassischen Controller-API
    (legacy_get), die beide ueber denselben Proxy-Tunnel laufen."""
    url = CONNECTOR_BASE + "/" + urllib.parse.quote(host_id, safe=":") + url_path
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
            raise RuntimeError(f"HTTP {exc.code} beim Connector-Proxy {url_path}: {body}")
        except urllib.error.URLError as exc:
            if attempt < 2:
                time.sleep(10)
                continue
            raise RuntimeError(f"Keine Verbindung zu api.ui.com (Connector-Proxy): {exc.reason}")
    return {}


def connector_get(host_id, path, timeout=30):
    """Ruft die offizielle, dokumentierte Network-Integration-API (v1) einer
    Konsole ueber den Site-Manager-Connector-Proxy auf (kein VPN/lokales Netz
    noetig)."""
    return _connector_request(host_id, "/proxy/network/integration/v1" + path, timeout)


def legacy_get(host_id, path, timeout=30):
    """Ruft die klassische (undokumentierte) UniFi-Controller-API auf - laeuft
    ueber denselben Connector-Proxy-Tunnel wie connector_get(), aber ohne den
    "/integration/v1"-Pfad. Noetig fuer den SIM-Datenzaehler (rx/txbytes) des
    LTE-Modems, den die offizielle Integration-API nicht exportiert."""
    return _connector_request(host_id, "/proxy/network" + path, timeout)


def find_gateway_device(host_id):
    """Ermittelt lokale Network-API site_id und device_id der Konsole (Gateway)
    selbst, ueber MAC-Abgleich mit der Site-Manager hostId. Historisch: wurde
    vom alten Live-Rate-Pfad genutzt (siehe get_uplink_rates()), inzwischen
    weder vom Polling (poll()) noch von --discover (discover()) mehr
    aufgerufen - vollstaendig unbenutzt, nur als Referenz belassen."""
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


def find_lte_modem(host_id, site_id):
    """Findet das LTE-Backup-Modem (U5G Max o.ae.) der Konsole ueber Namens-/
    Modell-Muster in der Geraeteliste. Liefert dessen MAC-Adresse."""
    devices = connector_get(host_id, f"/sites/{site_id}/devices")
    for dev in devices.get("data", []):
        name = dev.get("name", "").upper()
        model = dev.get("model", "").upper()
        if "LTE" in name or "U5G" in model or "U-LTE" in model:
            return dev["macAddress"]
    raise RuntimeError(f"Kein LTE-Modem für Host {host_id} in Network-API gefunden.")


def get_sim_bytes(host_id, site_name, mac):
    """Liest den kumulativen SIM-Datenzaehler (rx/tx Bytes seit letztem Reset,
    von Modem/Provider selbst gezaehlt) des LTE-Modems ueber die klassische
    Controller-API. Deutlich genauer als eine Rate-Hochrechnung, siehe
    Chat-Verlauf: Werte gegen das LCM-Display des Geraets verifiziert."""
    data = legacy_get(host_id, f"/api/s/{site_name}/stat/device/{mac}")
    entries = data.get("data") or []
    if not entries:
        raise RuntimeError(f"Kein Gerät für MAC {mac} in Legacy-API gefunden.")
    sims = entries[0].get("mbb", {}).get("sim", [])
    active = next((s for s in sims if s.get("active")), None)
    if not active:
        raise RuntimeError(f"Keine aktive SIM im LTE-Modem {mac} gefunden.")
    return int(active["rxbytes"]), int(active["txbytes"])


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

def _atomic_write(path, write_fn, newline=None):
    """Schreibt ueber eine temporaere Datei im selben Verzeichnis + os.replace()
    (atomarer Rename) statt direkt in die Zieldatei. GitHub Actions'
    concurrency.cancel-in-progress:true kann den Prozess jederzeit mitten im
    Schreiben abbrechen - ein direktes open(path, "w") wuerde dann eine
    abgeschnittene/kaputte Datei hinterlassen, die der naechste Lauf nicht
    mehr lesen kann. os.replace() ersetzt die Zieldatei immer nur als Ganzes,
    nie mit einem Teilzustand."""
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", newline=newline, encoding="utf-8") as handle:
        write_fn(handle)
    os.replace(tmp_path, path)


def load_rows():
    if not os.path.exists(CSV_PATH):
        return []
    rows = []
    with open(CSV_PATH, newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            try:
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
            except (KeyError, ValueError, TypeError):
                # Einzelne kaputte/unvollstaendige Zeile (z.B. Rest eines durch
                # cancel-in-progress abgebrochenen Schreibvorgangs aus einer
                # frueheren, nicht-atomaren Version) soll nicht die komplette
                # Historie unlesbar machen - nur diese Zeile ueberspringen.
                continue
    return rows


def _rollup_old_rows(rows, now):
    """Fasst Zeilen aelter als ROLLUP_AFTER_DAYS zu einer Zeile pro
    (Kalendertag, Konsole, Uplink) zusammen (Summe down_bytes/up_bytes), statt
    sie einzeln pro Poll fuer immer mitzuschleppen. Haelt wan_traffic.csv
    langfristig beschraenkt statt unbegrenzt zu wachsen.

    Verfaelscht keine Kennzahl: Stundenchart/Flow-Chart (CHART_WINDOW_DAYS),
    Tageswerte-Tabelle (TABLE_WINDOW_DAYS) und die Failover-Erkennung
    schauen alle nur auf die letzten paar Tage/Minuten - mit dem
    Sicherheitsabstand von ROLLUP_AFTER_DAYS gegenueber TABLE_WINDOW_DAYS
    treffen sie nie auf bereits aggregierte Zeilen. 'Aktueller Monat'/
    '30 Tage'/'Gesamt seit Start' bleiben korrekt, da sie nur die SUMME
    brauchen, keine Einzelzeilen - die Aggregation ist reine Summenbildung.

    Idempotent: laeuft bei jedem Poll erneut ueber alle 'alten' Zeilen
    (auch bereits aggregierte Tageszeilen aus frueheren Laeufen) - eine
    einzelne Tageszeile mit sich selbst zusammengefasst ergibt wieder
    dieselbe Zeile, kein Doppelzaehlen.

    interval_s der aggregierten Zeile ist ein reiner Platzhalter (86400,
    "ein Tag") und wird nie fuer eine Raten-Berechnung ausgewertet, da
    diese Zeilen ausserhalb aller Zeitfenster liegen, die interval_s
    dafuer nutzen (chart_rows, die juengsten Failover-Messpunkte)."""
    cutoff = now - timedelta(days=ROLLUP_AFTER_DAYS)
    recent, old = [], []
    for r in rows:
        (recent if r["ts"] >= cutoff else old).append(r)
    if not old:
        return recent

    daily = {}
    for r in old:
        # Lokaler Kalendertag (wie die Tageswerte-Tabelle es auch tut), fester
        # Zeitpunkt (12:00 lokal) je Tag, damit alle Zeilen desselben Tages
        # zuverlaessig auf denselben Aggregations-Key fallen.
        day = r["ts"].astimezone().replace(hour=12, minute=0, second=0, microsecond=0)
        key = (day, r["site"], r["uplink"])
        entry = daily.setdefault(key, {
            "ts": day, "site": r["site"], "uplink": r["uplink"],
            "interval_s": 86400, "down_bytes": 0.0, "up_bytes": 0.0,
        })
        entry["down_bytes"] += r["down_bytes"]
        entry["up_bytes"] += r["up_bytes"]
    return recent + list(daily.values())


def merge_rows(existing, new_points):
    existing = _rollup_old_rows(existing, datetime.now(timezone.utc))
    index = {(r["ts"], r["site"], r["uplink"]): r for r in existing}
    added = 0
    for point in new_points:
        key = (point["ts"], point["site"], point["uplink"])
        if key not in index:
            added += 1
        index[key] = point
    merged = sorted(index.values(), key=lambda r: (r["ts"], r["site"], r["uplink"]))

    def write(handle):
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

    _atomic_write(CSV_PATH, write, newline="")
    return merged, added


# ----------------------------------------------------------------------------
# Bericht
# ----------------------------------------------------------------------------

def human_bytes(value):
    # Dezimal (1000er-Schritte), nicht binaer (1024er) - damit die Zahlen
    # exakt zur Telekom-/LCM-Anzeige des LTE-Modems passen (siehe SIM-
    # Zaehler-Umstellung im Chat-Verlauf: 1024er waere "GB" beschriftet,
    # aber tatsaechlich GiB und damit ~7% niedriger als der Provider-Wert).
    step = 1000.0
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


# Dezimale GB (1000er), passend zu human_bytes() und dem SIM-Zaehler.
GB = 1000 ** 3
# Pro Konsole unterschiedliche Rot-Schwelle fuer "Aktueller Monat" - je nach
# ueblichem/erwartetem Datenvolumen des Standorts. Gelb liegt einheitlich bei
# 80% der jeweiligen Rot-Schwelle.
ALERT_THRESHOLD_BYTES_BY_CONSOLE = {
    "WTB--UDM-1": 24 * GB,
    "KNZ--UDM-1": 9 * GB,
    "LSB--UDM-1": 9 * GB,
    "KLO--UDM-1": 4 * GB,
    "NID--UDM-1": 4 * GB,
    "HAN--UDM-1": 4 * GB,
}
DEFAULT_ALERT_THRESHOLD_BYTES = 4 * GB  # Fallback fuer nicht gelistete Konsolen
WARN_THRESHOLD_FACTOR = 0.8
# Nur die UPLOAD-Rate zaehlt (nicht Down+Up kombiniert) - der KNZ-Vorfall
# zeigte, dass ein Routing-/Failover-Problem sich vor allem als massiver
# Upload ueber die LTE-Leitung aeussert.
FAILOVER_THRESHOLD_KBPS = 120.0
# Einzelne kurze Ausschlaege (Speedtest, Firmware-/Signatur-Download) sollen
# keinen Fehlalarm ausloesen. Erst wenn die letzten FAILOVER_CONSECUTIVE Polls
# IN FOLGE ueber dem Schwellwert liegen, gilt Failover als bestaetigt - bei
# 1-Minuten-Takt sind das 2 Minuten Verzoegerung, kaum spuerbar langsamer als
# vorher (1 Poll), aber deutlich weniger anfaellig fuer Einzel-Spitzen.
FAILOVER_CONSECUTIVE = 2
FAILOVER_EXCLUDED_DEVICES = set()
# Ab wann eine Konsole als "offline/nicht erreichbar" statt nur "kurz kein
# Update" gilt (5x der 1-Minuten-Pollintervall Toleranz fuer vereinzelt
# uebersprungene Laeufe, siehe poll() Fehlerbehandlung).
OFFLINE_THRESHOLD_S = 300

# Dauerbetrieb: Stundenchart/Flow-Chart und die Tageswerte-Tabelle bleiben auf
# ein recentes Fenster begrenzt, sonst werden sie nach Wochen/Monaten Laufzeit
# unbrauchbar gross. Kennzahlen (Monat/30 Tage/Gesamt) sind davon unabhaengig.
CHART_WINDOW_DAYS = 1  # Stunden-/Flow-Chart (Detail + Uebersichtskacheln): 24 Stunden
TABLE_WINDOW_DAYS = 30
ROLLING_AVG_MINUTES = 60  # Gleitendes Fenster fuer die Durchschnittslinie im Flow-Chart

# Ohne Rotation waechst wan_traffic.csv unbegrenzt (bereits ~21.000 Zeilen/
# 1.2 MB nach 3 Tagen) und merge_rows() schreibt bei JEDEM Poll die komplette
# Datei neu - das wird mit der Zeit zum dominanten Kostenfaktor. Zeilen
# aelter als ROLLUP_AFTER_DAYS werden deshalb zu einer Zeile pro Kalendertag/
# Konsole/Uplink zusammengefasst (siehe _rollup_old_rows()). Deutlicher
# Sicherheitsabstand zu TABLE_WINDOW_DAYS, damit die Tageswerte-Tabelle nie
# auf bereits aggregierte Zeilen trifft.
ROLLUP_AFTER_DAYS = 35


def _console_alert_threshold(console_name):
    """Loest die Rot-Schwelle einer Konsole auf - per Substring-Match wie
    sim_totals in compute_stats(), nicht per exaktem dict-Key-Vergleich, damit
    z.B. '--site lsb' (Kleinschreibung/Kurzform) dieselbe Schwelle bekommt wie
    die kanonische Konsole 'LSB--UDM-1', statt still auf den generischen
    Default zurueckzufallen."""
    if console_name:
        needle = console_name.lower()
        for name, threshold in ALERT_THRESHOLD_BYTES_BY_CONSOLE.items():
            if needle in name.lower():
                return threshold
    return DEFAULT_ALERT_THRESHOLD_BYTES


def total_alert_class(total_bytes, console_name=None):
    """CSS-Klassen-Zusatz fuer den 'Monat'-Wert: pro Konsole eigene Rot-
    Schwelle (siehe ALERT_THRESHOLD_BYTES_BY_CONSOLE), Gelb bei 80% davon."""
    alert = _console_alert_threshold(console_name)
    warn = alert * WARN_THRESHOLD_FACTOR
    if total_bytes > alert:
        return " value-alert"
    if total_bytes > warn:
        return " value-warn"
    return ""


def alert_threshold_label(console_name):
    """Rot-Schwelle der Konsole, kurz formatiert fuer die Anzeige neben dem
    'Monat'-Wert (z.B. '64.4 GB / 24.0 GB')."""
    return human_bytes(_console_alert_threshold(console_name))


def compute_stats(rows, start, site_filter=None, sim_totals=None):
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

    sim_totals: optionales dict console_name -> (rx_bytes, tx_bytes), der
    zuletzt bekannte ABSOLUTE SIM-Zaehlerstand (siehe load_sim_totals()). Der
    Provider setzt diesen Zaehler nachweislich monatlich zurueck - deshalb
    wird er, wenn vorhanden, DIREKT als "Aktueller Monat" verwendet, statt
    selbst ueber den Kalendermonat zu summieren (das wuerde alte, Rate-
    geschaetzte und neue, SIM-exakte Messpunkte vermischen und nie den echten
    Modem-Zaehlerstand erreichen).
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
    # Fortschrittsbalken/"Tag X von Y" soll den echten Kalendertag zeigen,
    # unabhaengig davon, ob fuer alle Tage schon Daten vorliegen.
    days_elapsed_month_calendar = max((now - month_start).total_seconds() / 86400.0, 0.001)
    days_in_month = calendar.monthrange(now_local.year, now_local.month)[1]

    # sim_totals-Key mit derselben Substring-Logik wie in_site() aufloesen,
    # nicht per exaktem dict-Key-Vergleich - sonst faellt z.B. "--site lsb"
    # (Kleinschreibung/Kurzform, wie in_site() es eigentlich unterstuetzt)
    # faelschlich auf die weniger genaue CSV-Delta-Berechnung zurueck, obwohl
    # der SIM-Zaehlerstand vorhanden waere.
    sim_total = None
    if sim_totals and site_filter:
        needle = site_filter.lower()
        sim_entry = next((v for k, v in sim_totals.items() if needle in k.lower()), None)
        if sim_entry is not None:
            rx, tx, baseline_ts_str = sim_entry
            baseline_ts = parse_ts(baseline_ts_str) if baseline_ts_str else None
            # Nur vertrauen, wenn die Baseline aus dem AKTUELLEN Kalendermonat
            # stammt - sonst wuerde eine seit letztem Monat offline Konsole
            # weiterhin den eingefrorenen alten Monatswert zeigen, obwohl im
            # neuen Monat noch gar keine Daten vorliegen.
            if baseline_ts is not None and baseline_ts >= month_start:
                sim_total = rx + tx

    if sim_total is not None:
        total_month = sim_total
        days_elapsed_month = days_elapsed_month_calendar
    else:
        # Fallback (z.B. noch kein Poll seit dem Umstieg auf SIM-Zaehler
        # gelaufen): alte Kalendermonat-Summe aus den CSV-Deltas.
        month_rows = [r for r in all_rows if r["ts"] >= month_start]
        total_month = sum(r["down_bytes"] + r["up_bytes"] for r in month_rows)
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

    # Failover-Verdacht: der Durchschnitt der letzten FAILOVER_CONSECUTIVE
    # Messpunkte ueber dem Schwellwert (kbps UPLOAD, nicht kombiniert - siehe
    # KNZ-Vorfall). Bewusst der DURCHSCHNITT, nicht "jeder Einzelwert fuer
    # sich" (wie frueher): bei WTB springt die Rate teils innerhalb von
    # Sekunden zwischen 0 und mehreren Mbps (bursty Traffic waehrend eines
    # echten Ausfalls) - eine "alle Einzelwerte muessen ueber der Schwelle
    # liegen"-Regel kippte dadurch bei einem einzelnen Null-Poll faelschlich
    # auf "kein Failover", obwohl der (identisch angezeigte) Durchschnitt
    # klar drueber lag - fuer den Nutzer ein sichtbarer Widerspruch zwischen
    # angezeigtem Wert und Badge. Ein einzelner kurzer Ausschlag OHNE echten
    # Ausfall bleibt trotzdem unwahrscheinlich, da er beide Poll-Werte des
    # Fensters ueberdurchschnittlich anheben muesste.
    is_failover = False
    last_rate_kbps = 0.0
    if all_rows and site_filter not in FAILOVER_EXCLUDED_DEVICES:
        recent = heapq.nlargest(FAILOVER_CONSECUTIVE, all_rows, key=lambda r: r["ts"])
        recent.sort(key=lambda r: r["ts"])  # chronologisch, aeltester zuerst
        rates = []
        for r in recent:
            if r["interval_s"]:
                rates.append(r["up_bytes"] * 8.0 / r["interval_s"] / 1000.0)
            else:
                rates.append(0.0)
        if rates:
            last_rate_kbps = sum(rates) / len(rates)
        if len(rates) == FAILOVER_CONSECUTIVE:
            is_failover = last_rate_kbps > FAILOVER_THRESHOLD_KBPS

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


def load_sim_totals():
    """Liest die zuletzt bekannten ABSOLUTEN SIM-Zaehlerstaende (rx,tx,ts) je
    Konsole aus monitor_state.json - siehe compute_stats()/sim_totals. ts wird
    mitgeliefert, damit compute_stats() erkennen kann, ob die Baseline noch
    aus dem aktuellen Kalendermonat stammt."""
    if not os.path.exists(STATE_PATH):
        return {}
    try:
        with open(STATE_PATH, encoding="utf-8") as handle:
            state = json.load(handle)
    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
        # Kaputte/unvollstaendige State-Datei - lieber ohne SIM-Totals
        # weitermachen (compute_stats faellt dann auf die CSV-Delta-Summe
        # zurueck) als den Bericht komplett scheitern zu lassen.
        return {}
    baseline = state.get("sim_baseline", {})
    return {name: (b["rx"], b["tx"], b.get("ts")) for name, b in baseline.items()}


def build_report(rows, start, site_filter=None, sim_totals=None):
    if sim_totals is None:
        sim_totals = load_sim_totals()
    return render_html(**compute_stats(rows, start, site_filter, sim_totals))


def render_chart(series, peak, start):
    width, height = 960, 220
    left, bottom = 54, 28
    plot_w = width - left - 12
    plot_h = height - bottom - 12
    n = max(len(series), 1)
    slot = plot_w / n
    bar_w = max(slot * 0.72, 1.2)

    # Logarithmische Hoehen-Skalierung (log1p) statt linear: bei Konsolen mit
    # wenig Grundlast (z.B. LSB) und nur seltenen, dafuer hohen Ausschlaegen
    # (Failover) verschluckt eine lineare Skala die Grundlast komplett - sie
    # bleibt bei ein paar Pixeln Hoehe unsichtbar, waehrend der seltene
    # Ausschlag den gesamten Balken fuellt. log1p(0) = 0 (Nullwerte bleiben
    # exakt auf der Grundlinie), waechst aber anfangs viel steiler als linear,
    # sodass auch kleine Werte sichtbare Balkenhoehe bekommen. Gilt fuer alle
    # Konsolen gleichermassen (dieselbe Render-Funktion).
    peak_scaled = math.log1p(peak) or 1.0

    parts = []
    for i in range(1, 4):
        y = 12 + plot_h * (1 - i / 4.0)
        label_value = math.expm1((i / 4.0) * peak_scaled)
        label = human_bytes(label_value)
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" class="grid"/>')
        parts.append(f'<text x="{left - 8}" y="{y + 4:.1f}" class="axis" text-anchor="end">{label}</text>')

    bars_data = []
    for i, (bucket, down, up) in enumerate(series):
        x = left + i * slot + (slot - bar_w) / 2
        x_center = x + bar_w / 2
        total = down + up
        # Gesamthoehe des Balkens folgt der Log-Skala; die Aufteilung in
        # Down-/Up-Anteil bleibt linear-proportional zum tatsaechlichen
        # Verhaeltnis (sonst waere log(down)+log(up) != log(down+up) und die
        # Stapelhoehe wuerde nicht mehr zur Achsenbeschriftung passen).
        total_h = plot_h * (math.log1p(total) / peak_scaled) if total > 0 else 0.0
        h_down = total_h * (down / total) if total > 0 else 0.0
        h_up = total_h * (up / total) if total > 0 else 0.0
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
        # Rohdaten fuer den Hover-Tooltip (siehe flow_tooltip_script): Zeitstempel
        # der Stunde, Down/Up in Bytes, exakte x-Pixel-Position des Balkens.
        bars_data.append([bucket.isoformat(), round(down), round(up), round(x_center, 1)])

    parts.append(f'<line x1="{left}" y1="{12 + plot_h}" x2="{left + plot_w}" y2="{12 + plot_h}" class="baseline"/>')
    # Hover-Linie: unsichtbar per Default, wird von flow_tooltip_script beim Hovern
    # an die x-Position des naechstgelegenen Balkens verschoben und eingeblendet.
    parts.append(f'<line class="hover-line" x1="0" y1="12" x2="0" y2="{12 + plot_h}"/>')
    bars_attr = html.escape(json.dumps(bars_data), quote=True)
    return (f'<svg viewBox="0 0 {width} {height}" preserveAspectRatio="none" class="chart hour-chart" '
            f'role="img" aria-label="Stundenvolumen" data-bars="{bars_attr}">{"".join(parts)}</svg>')


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
    # Logarithmische Hoehen-Skalierung (log1p), siehe render_chart() fuer die
    # Begruendung: sonst verschwindet die Grundlast ruhiger Konsolen (LSB,
    # HAN, KLO, NID) neben seltenen hohen Failover-Ausschlaegen komplett.
    peak_scaled = math.log1p(peak) or 1.0

    def xy(ts, value):
        x = left + (ts - start).total_seconds() / span_s * plot_w
        scaled = math.log1p(max(value, 0.0)) / peak_scaled if peak_scaled else 0.0
        y = 12 + plot_h - scaled * plot_h
        return x, y

    baseline_y = 12 + plot_h
    down_line = [xy(ts, d) for ts, d, u in samples]
    up_line = [xy(ts, u) for ts, d, u in samples]
    down_pts_str = " ".join(f"{x:.1f},{y:.1f}" for x, y in down_line)
    up_pts_str = " ".join(f"{x:.1f},{y:.1f}" for x, y in up_line)
    down_area = f"{down_line[0][0]:.1f},{baseline_y:.1f} {down_pts_str} {down_line[-1][0]:.1f},{baseline_y:.1f}"

    # Gleitender Durchschnitt ueber die letzten ROLLING_AVG_MINUTES Minuten -
    # NICHT der kumulative Mittelwert seit Fensterbeginn (der wurde bei
    # laengeren Fenstern mit vielen Messpunkten praktisch unbeweglich, weil
    # ein einzelner neuer Punkt gegen hunderte alte kaum noch ins Gewicht
    # faellt - die Linie "fror" ein statt dem Trend zu folgen). Zeitbasiertes
    # Fenster statt Punktezahl, robust gegen wechselnde Poll-Intervalle.
    avg_down_line, avg_up_line, avgs = [], [], []
    window = deque()  # (ts, d, u) der Punkte im aktuellen Rolling-Fenster
    sum_d = sum_u = 0.0
    window_span = timedelta(minutes=ROLLING_AVG_MINUTES)
    for ts, d, u in samples:
        window.append((ts, d, u))
        sum_d += d
        sum_u += u
        while window and (ts - window[0][0]) > window_span:
            _, old_d, old_u = window.popleft()
            sum_d -= old_d
            sum_u -= old_u
        avg_d, avg_u = sum_d / len(window), sum_u / len(window)
        avg_down_line.append(xy(ts, avg_d))
        avg_up_line.append(xy(ts, avg_u))
        avgs.append((avg_d, avg_u))
    avg_down_pts_str = " ".join(f"{x:.1f},{y:.1f}" for x, y in avg_down_line)
    avg_up_pts_str = " ".join(f"{x:.1f},{y:.1f}" for x, y in avg_up_line)

    parts = []
    for i in range(1, 4):
        y = 12 + plot_h * (1 - i / 4.0)
        label_value = math.expm1((i / 4.0) * peak_scaled)
        label = f"{label_value:,.0f} kbps".replace(",", ".")
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
  #refresh-cd { color: var(--warn); font-weight: 600; font-family: ui-monospace, monospace; }
  .bar { height: 5px; background: var(--line); border-radius: 3px; margin-top: 16px; overflow: hidden; }
  .bar span { display: block; height: 100%; background: var(--down); }
  .grid-cards { display: grid; gap: 14px; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); margin-bottom: 30px; }
  .card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 16px 18px; }
  .card .label { color: var(--dim); font-size: 12px; text-transform: uppercase; letter-spacing: .09em; }
  .card .value { font: 600 26px/1.25 ui-monospace, "SFMono-Regular", Consolas, monospace; margin-top: 8px; }
  .card .foot { color: var(--dim); font-size: 12.5px; margin-top: 4px; }
  .threshold-ref { font-size: .55em; font-weight: 500; color: var(--dim); }
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
  .flow-chart .hover-line, .hour-chart .hover-line { stroke: var(--text); stroke-width: 1; stroke-dasharray: 3 3;
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
    """Hover-Tooltip fuer alle Traffic-Flow-Charts (svg.flow-chart) UND
    Stundencharts (svg.hour-chart) der Seite. Liest die in data-samples bzw.
    data-bars eingebetteten Rohdaten, mappt die Maus-X-Position ueber die
    SVG-CTM (funktioniert auch mit preserveAspectRatio="none") auf den
    naechstgelegenen Punkt/Balken und zeigt eine kleine, dem Cursor folgende
    Box mit den Werten."""
    return """<script>
(function() {
  var tip = document.createElement('div');
  tip.className = 'flow-tooltip';
  document.body.appendChild(tip);

  function fmtKbps(v) {
    if (v >= 1000) return (v / 1000).toFixed(2).replace('.', ',') + ' Mbps';
    return v.toFixed(1).replace('.', ',') + ' kbps';
  }
  function fmtBytes(v) {
    var units = ['B', 'KB', 'MB', 'GB', 'TB'];
    var i = 0;
    while (Math.abs(v) >= 1000 && i < units.length - 1) { v /= 1000; i++; }
    return v.toFixed(1).replace('.', ',') + ' ' + units[i];
  }

  // Gemeinsame Hover-Logik fuer beide Chart-Typen: samples/bars ist eine nach
  // x aufsteigend sortierte Liste, xIndex das Feld mit der Pixel-Position
  // (NICHT der Index selbst - die Punkte/Balken liegen wegen wechselnder
  // Poll-Intervalle bzw. variabler Bucket-Breite nicht gleichmaessig auf der
  // x-Achse). Binaersuche zum naechstgelegenen Eintrag.
  function attachHover(svg, entries, xIndex, buildHtml) {
    var hoverLine = svg.querySelector('.hover-line');
    function nearest(svgX) {
      var lo = 0, hi = entries.length - 1;
      while (lo < hi) {
        var mid = (lo + hi) >> 1;
        if (entries[mid][xIndex] < svgX) lo = mid + 1; else hi = mid;
      }
      if (lo > 0 && Math.abs(entries[lo - 1][xIndex] - svgX) < Math.abs(entries[lo][xIndex] - svgX)) {
        lo -= 1;
      }
      return entries[lo];
    }
    svg.addEventListener('mousemove', function (ev) {
      var pt = svg.createSVGPoint();
      pt.x = ev.clientX; pt.y = ev.clientY;
      var svgP = pt.matrixTransform(svg.getScreenCTM().inverse());
      var s = nearest(svgP.x);
      tip.innerHTML = buildHtml(s);
      var x = ev.clientX + 16, y = ev.clientY + 16;
      if (x + 170 > window.innerWidth) x = ev.clientX - 186;
      if (y + 92 > window.innerHeight) y = ev.clientY - 108;
      tip.style.left = x + 'px';
      tip.style.top = y + 'px';
      tip.style.display = 'block';
      if (hoverLine) {
        hoverLine.setAttribute('x1', s[xIndex]);
        hoverLine.setAttribute('x2', s[xIndex]);
        hoverLine.style.opacity = '1';
      }
    });
    svg.addEventListener('mouseleave', function () {
      tip.style.display = 'none';
      if (hoverLine) hoverLine.style.opacity = '0';
    });
  }

  document.querySelectorAll('svg.flow-chart').forEach(function (svg) {
    var samples;
    try { samples = JSON.parse(svg.getAttribute('data-samples')); } catch (e) { return; }
    if (!samples || !samples.length) return;
    attachHover(svg, samples, 3, function (s) {
      var d = new Date(s[0]);
      var timeStr = d.toLocaleString('de-DE', {
        day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit'
      });
      return '<b>' + timeStr + '</b><br>Down: ' + fmtKbps(s[1]) +
        '<br>Up: ' + fmtKbps(s[2]) +
        '<br><span class="flow-tooltip-avg">Ø Down: ' + fmtKbps(s[4]) +
        '<br>Ø Up: ' + fmtKbps(s[5]) + '</span>';
    });
  });

  document.querySelectorAll('svg.hour-chart').forEach(function (svg) {
    var bars;
    try { bars = JSON.parse(svg.getAttribute('data-bars')); } catch (e) { return; }
    if (!bars || !bars.length) return;
    attachHover(svg, bars, 3, function (s) {
      var d = new Date(s[0]);
      var timeStr = d.toLocaleString('de-DE', {
        day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit'
      });
      return '<b>' + timeStr + ' Uhr</b><br>Down: ' + fmtBytes(s[1]) +
        '<br>Up: ' + fmtBytes(s[2]) +
        '<br><span class="flow-tooltip-avg">Gesamt: ' + fmtBytes(s[1] + s[2]) + '</span>';
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
      <div class="value{total_alert_class(c['total_month'], c['device'])}">{human_bytes(c['total_month'])} <span class="threshold-ref">/ {alert_threshold_label(c['device'])}</span></div>
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

  <h2>Stundenvolumen (letzte {int(CHART_WINDOW_DAYS * 24)} Stunden)</h2>
  <div class="panel">
    {c['chart']}
    <div class="legend">
      <span><span class="dot" style="background:var(--down)"></span>Download</span>
      <span><span class="dot" style="background:var(--up)"></span>Upload</span>
      <span>Spitze {human_bytes(c['peak'])} pro Stunde</span>
      <span class="dim">Achse logarithmisch (Grundlast bleibt neben Ausschlägen sichtbar)</span>
    </div>
  </div>

  <h2>Traffic-Flow (Rate je Messpunkt)</h2>
  <div class="panel">
    {c['flow_chart']}
    <div class="legend">
      <span><span class="dot" style="background:var(--down)"></span>Download (kbps)</span>
      <span><span class="dot" style="background:var(--up)"></span>Upload (kbps)</span>
      <span class="dim">- - - Ø letzte {ROLLING_AVG_MINUTES} Min</span>
      <span>{c['flow_points']} Messpunkte, Pollintervall ~{c['flow_interval_min']} Min</span>
      <span class="dim">Achse logarithmisch</span>
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

  <footer>Datenquelle: SIM-Datenzähler des LTE-Modems (via Site-Manager-Connector-Proxy),
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
        # Graue Kachel/OFFLINE haengt NUR noch an echter Daten-Staere (kein
        # neuer SIM-Zaehlerstand seit OFFLINE_THRESHOLD_S), NICHT mehr an
        # 0 kbps: manche Konsolen (z.B. LSB) haben legitim oft 0 kbps Upload,
        # ohne offline zu sein - das fuehrte zu Fehlalarmen.
        has_current_point = any(r["ts"] >= current_hour for r in c["window"])
        live_badge = '<span class="live-tag">läuft</span>' if has_current_point and not c["is_offline"] else ""
        failover_badge = '<span class="failover-tag">FAILOVER</span>' if c["is_failover"] else ""
        offline_badge = '<span class="offline-tag">OFFLINE</span>' if c["is_offline"] and not c["is_failover"] else ""
        if c["is_failover"]:
            card_class = "card console-card failover"
        elif c["is_offline"]:
            card_class = "card console-card idle"
        else:
            card_class = "card console-card"
        filename = f"{c['device']}.html"
        cards.append(f"""<div class="{card_class}">
      <div class="card-head"><h3>{c['device']}</h3>{live_badge}{failover_badge}{offline_badge}</div>
      <div class="mini-grid">
        <div><div class="mlabel">Monat</div><div class="mvalue{total_alert_class(c['total_month'], c['device'])}">{human_bytes(c['total_month'])} <span class="threshold-ref">/ {alert_threshold_label(c['device'])}</span></div></div>
        <div><div class="mlabel">30 Tage</div><div class="mvalue">{human_bytes(c['total_30d'])}</div></div>
        <div><div class="mlabel">Gesamt</div><div class="mvalue">{human_bytes(c['total'])}
          <span class="live-rate">Akt. Upload: {human_kbps(c['last_rate_kbps'])}</span></div></div>
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

  <footer>Datenquelle: SIM-Datenzähler des LTE-Modems (via Site-Manager-Connector-Proxy),
    {len(consoles)} Konsolen. Seite aktualisiert sich alle {REPORT_REFRESH_S // 60} Minute{'n' if REPORT_REFRESH_S // 60 != 1 else ''} selbst.</footer>
</div>
{refresh_countdown_script(now)}
{flow_tooltip_script()}
</body>
</html>"""


def write_report(rows, start, site_filter=None):
    html = build_report(rows, start, site_filter)
    _atomic_write(HTML_PATH, lambda handle: handle.write(html))
    return HTML_PATH


def _group_by_console(rows, console_names):
    """Teilt rows EINMAL in einem einzigen Durchlauf in Buckets pro Konsole
    auf (dieselbe Substring-Logik wie compute_stats()' in_site()), statt
    compute_stats() die komplette, mit wachsender CSV immer laenger werdende
    Liste PRO Konsole (6x) unabhaengig voneinander scannen zu lassen."""
    needles = [(name, name.lower()) for name in console_names]
    buckets = {name: [] for name in console_names}
    for r in rows:
        site_l = r["site"].lower()
        uplink_l = r["uplink"].lower()
        for name, needle in needles:
            if needle in site_l or needle in uplink_l:
                buckets[name].append(r)
    return buckets


def write_reports(rows, start, console_names):
    """Schreibt die Übersichtsseite (wan_report.html) plus eine Detailseite
    pro Konsole ({Konsolenname}.html).

    Performance: compute_stats() ist mit wachsender CSV der teuerste Teil
    (scannt Zeilen je Konsole fuer Monat/30-Tage/Charts). Frueher wurde es
    PRO KONSOLE zweimal aufgerufen - einmal fuer die Uebersichtskachel (via
    build_overview), einmal fuer die Detailseite (via build_report). Jetzt:
    einmal berechnen, Ergebnis fuer beide Seiten wiederverwenden. Zusaetzlich
    wird rows VORAB einmal per _group_by_console() aufgeteilt, statt dass
    jeder der 6 compute_stats()-Aufrufe die komplette Liste erneut scannt."""
    now = datetime.now(timezone.utc)
    sim_totals = load_sim_totals()
    buckets = _group_by_console(rows, console_names)
    consoles = [compute_stats(buckets[name], start, site_filter=name, sim_totals=sim_totals)
                for name in console_names]

    overview_html = render_overview_html(consoles=consoles, start=start, now=now)
    _atomic_write(HTML_PATH, lambda handle: handle.write(overview_html))

    detail_paths = []
    for c in consoles:
        detail_html = render_html(**c)
        path = os.path.join(DATA_DIR, f"{c['device']}.html")
        _atomic_write(path, lambda handle: handle.write(detail_html))
        detail_paths.append(path)
    return HTML_PATH, detail_paths


# ----------------------------------------------------------------------------
# Ablauf
# ----------------------------------------------------------------------------

# Ein neuer SIM-Zaehlerstand unter dem letzten bekannten gilt nur dann als
# ECHTER Reset (Monatswechsel beim Provider oder Modem-Neustart), wenn er
# deutlich (auf <= RESET_SANITY_FACTOR des alten Stands) abgefallen ist. Ein
# kleiner Ruecksprung ist plausibler eine kurzzeitig veraltete/zwischen-
# gespeicherte Antwort des Connector-Proxys als ein echter Reset - wuerde
# sonst faelschlich als riesiger Delta-Sprung verbucht (der komplette neue
# Zaehlerstand auf einmal, statt der paar tatsaechlich seit dem letzten Poll
# uebertragenen Bytes).
RESET_SANITY_FACTOR = 0.5


def _sim_delta(new_value, old_value):
    """Liefert (delta, ok). ok=False heisst: unplausibler Ruecksprung, dieser
    Poll sollte uebersprungen und die Baseline NICHT aktualisiert werden."""
    if new_value >= old_value:
        return new_value - old_value, True
    if old_value == 0 or new_value <= old_value * RESET_SANITY_FACTOR:
        return new_value, True  # echter Reset: neuer Stand IST die Menge seit Reset
    return None, False


def poll(rows, targets, interval_s, sim_baseline):
    """Ein Live-Sample des kumulativen SIM-Datenzaehlers jeder Konsole.
    targets = Liste von (console_name, host_id, site_name, lte_mac).

    Historie: zuerst isp-metrics-API (lieferte nachweislich falsche Werte,
    Faktor ~5000 gegenueber dem GUI-Traffic-Graphen), dann Live-Uplink-Rate
    hochgerechnet auf das Poll-Intervall (rate * interval_s - anfaellig fuer
    Fehler, wenn der tatsaechliche Poll-Abstand vom angenommenen interval_s
    abweicht). Jetzt: der SIM-eigene Byte-Zaehler des LTE-Modems selbst
    (rx/txbytes, vom Provider/Modem gezaehlt) - exakt, gegen das LCM-Display
    des Geraets verifiziert.

    sim_baseline: dict console_name -> {"rx": int, "tx": int, "ts": iso-str},
    der zuletzt bekannte Zaehlerstand. Wird IN-PLACE aktualisiert; der
    Aufrufer muss sim_baseline danach in monitor_state.json sichern, sonst
    geht die Baseline beim naechsten Prozessstart verloren (--once startet ja
    bei jedem Poll einen neuen Prozess). Ein erkannter Reset (siehe
    _sim_delta) laesst die Zaehlung fuer diese Konsole einfach wieder bei 0
    beginnen.
    """
    now = datetime.now(timezone.utc)
    points = []
    for console_name, host_id, site_name, mac in targets:
        # Der GESAMTE Block fuer eine Konsole (nicht nur der Netzwerk-Aufruf)
        # steht bewusst im try/except: eine einzelne offline/nicht erreichbare
        # Konsole ODER eine unerwartet fehlerhafte Baseline darf nicht den
        # kompletten Poll-Durchlauf (und damit die Daten ALLER anderen
        # Konsolen) zum Absturz bringen - hier nur diese Konsole ueberspringen.
        try:
            rx, tx = get_sim_bytes(host_id, site_name, mac)
            base = sim_baseline.get(console_name)
            if base is None:
                # Erster Poll fuer diese Konsole: nur Baseline setzen, kein
                # Delta - wir wissen nicht, seit wann der Zaehler schon laeuft,
                # ein Delta gegen 0 waere ein riesiger, irrefuehrender erster
                # Messpunkt.
                down_delta, up_delta, actual_interval = 0, 0, interval_s
                sim_baseline[console_name] = {"rx": rx, "tx": tx, "ts": now.isoformat()}
            else:
                down_delta, down_ok = _sim_delta(rx, base["rx"])
                up_delta, up_ok = _sim_delta(tx, base["tx"])
                if not (down_ok and up_ok):
                    print(f"  {console_name}: unplausibler SIM-Zaehlerstand "
                          f"(rx {base['rx']}->{rx}, tx {base['tx']}->{tx}), "
                          f"Poll uebersprungen, Baseline beibehalten")
                    continue
                actual_interval = max((now - datetime.fromisoformat(base["ts"])).total_seconds(), 1.0)
                sim_baseline[console_name] = {"rx": rx, "tx": tx, "ts": now.isoformat()}
            points.append({
                "ts": now,
                "site": console_name,
                "uplink": "wan",
                "interval_s": round(actual_interval),
                "down_bytes": down_delta,
                "up_bytes": up_delta,
            })
        except Exception as exc:
            print(f"  {console_name}: Poll-Fehler, ueberspringe diesen Durchlauf - {exc}")
            continue
    return merge_rows(rows, points)


def discover(host_id=DEFAULT_HOST_ID):
    """Prueft, ob eine Konsole bereit fuer MONITORED_HOSTS ist - ueber GENAU
    den Pfad, den poll() im Produktivbetrieb nutzt (find_lte_modem +
    get_sim_bytes), nicht den alten, abgeloesten Live-Rate-Pfad. Ein Erfolg
    hier heisst also wirklich, dass das eigentliche Polling funktionieren
    wird."""
    for name, path in (("Hosts", "/hosts"), ("Sites", "/sites")):
        data = api_get(path)
        print(f"\n=== {name} ===")
        print(json.dumps(data, indent=2)[:3000])

    print(f"\n=== SIM-Zaehler-Pfad (Produktivbetrieb) fuer hostId {host_id} ===")
    sites = connector_get(host_id, "/sites")
    site_id = sites["data"][0]["id"]
    site_name = sites["data"][0]["internalReference"]
    print(f"lokale site_id: {site_id}, site_name: {site_name}")
    lte_mac = find_lte_modem(host_id, site_id)
    print(f"LTE-Modem gefunden: MAC {lte_mac}")
    rx, tx = get_sim_bytes(host_id, site_name, lte_mac)
    print(f"SIM-Zaehlerstand: rx={rx} Bytes ({human_bytes(rx)}), tx={tx} Bytes ({human_bytes(tx)})")
    print("\nErfolg - dies ist derselbe Pfad, den poll() im Dauerbetrieb verwendet. "
          "Konsole kann zu MONITORED_HOSTS hinzugefuegt werden.")


def main():
    parser = argparse.ArgumentParser(description="WAN-Volumen mehrerer UDM Pro messen (Dauerbetrieb)")
    parser.add_argument("--start", help="Messbeginn, z.B. 2026-08-07T12:00 (lokale Zeit). "
                                         "Nur beim allerersten Lauf relevant, danach aus monitor_state.json.")
    parser.add_argument("--interval", type=int, default=60, help="Pollintervall in Sekunden")
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

    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, encoding="utf-8") as handle:
                state = json.load(handle)
        except (json.JSONDecodeError, OSError, UnicodeDecodeError) as exc:
            # Kaputte/abgeschnittene State-Datei (z.B. Rest eines durch
            # cancel-in-progress abgebrochenen Laufs) soll nicht jeden
            # weiteren Lauf dauerhaft blockieren - lieber mit leerem State neu
            # anfangen (verliert im schlimmsten Fall nur die SIM-Baseline
            # eines Polls, die naechste Messung setzt sie automatisch neu).
            print(f"Warnung: monitor_state.json konnte nicht gelesen werden ({exc}), starte mit leerem State neu.")
            state = {}
    else:
        state = {}

    if args.start:
        start = datetime.fromisoformat(args.start).astimezone()
    elif "start" in state:
        start = datetime.fromisoformat(state["start"])
    else:
        start = datetime.now().astimezone().replace(minute=0, second=0, microsecond=0)
    start = start.astimezone(timezone.utc)
    # Kein festes Messende mehr (Dauerbetrieb) - start wird nur einmalig gesetzt
    # und danach immer aus monitor_state.json uebernommen. sim_baseline haelt
    # den letzten bekannten SIM-Zaehlerstand je Konsole (siehe poll()) - muss
    # ueber Prozessneustarts hinweg erhalten bleiben (--once startet ja bei
    # jedem Poll einen neuen Prozess).
    state["start"] = start.isoformat()
    sim_baseline = state.setdefault("sim_baseline", {})

    def save_state():
        _atomic_write(STATE_PATH, lambda handle: json.dump(state, handle))

    save_state()

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
    # Stiller Fallback auf DEFAULT_ALERT_THRESHOLD_BYTES fuer nicht gelistete
    # Konsolen ist beabsichtigt, aber soll nicht UNBEMERKT bleiben - hier
    # einmalig als Hinweis geloggt (ALERT_THRESHOLD_BYTES_BY_CONSOLE wird
    # unabhaengig von MONITORED_HOSTS gepflegt).
    for name in console_names:
        if name not in ALERT_THRESHOLD_BYTES_BY_CONSOLE:
            print(f"Hinweis: {name} hat keine eigene Schwelle in ALERT_THRESHOLD_BYTES_BY_CONSOLE, "
                  f"nutzt Standard ({human_bytes(DEFAULT_ALERT_THRESHOLD_BYTES)}).")
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
            sites = connector_get(host_id, "/sites")
            site_id = sites["data"][0]["id"]
            site_name = sites["data"][0]["internalReference"]
            lte_mac = find_lte_modem(host_id, site_id)
        except Exception as exc:
            print(f"Ziel: {name}: uebersprungen, nicht erreichbar - {exc}")
            continue
        targets.append((name, host_id, site_name, lte_mac))
        print(f"Ziel: {name} (site={site_name}, lte_mac={lte_mac})")

    rows = load_rows()
    while True:
        rows, added = poll(rows, targets, args.interval, sim_baseline)
        save_state()
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
