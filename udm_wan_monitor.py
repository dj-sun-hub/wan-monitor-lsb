#!/usr/bin/env python3
"""
UDM Pro WAN-Traffic-Monitor
===========================

Holt WAN-Kennzahlen ueber die UniFi Site Manager API (api.ui.com), schreibt sie
fortlaufend in eine CSV und erzeugt nach jedem Durchlauf einen aktuellen
HTML-Bericht. Dauerbetrieb ohne festes Messende: EINZIGE Volumenkennzahl ist
der aktuelle Kalendermonat (1. bis Letzter). "Letzte 30 Tage" und "Gesamt
seit Start" gab es frueher zusaetzlich - sie sind entfallen, weil drei
Volumenzahlen nebeneinander mehr Verwirrung stifteten als Erkenntnis.

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
from concurrent.futures import ThreadPoolExecutor, as_completed
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
# Winzige Datei neben dem Bericht, die nur den Erzeugungszeitpunkt enthaelt.
# Die Seite fragt sie im Sekundentakt ab, statt sich blind neu zu laden -
# siehe refresh_countdown_script(). 30 Byte statt 130 KB je Abfrage.
STAND_PATH = os.path.join(DATA_DIR, "wan_stand.txt")
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


def _connector_request(host_id, url_path, timeout=12):
    """Gemeinsame GET-Ausfuehrung + Fehlerbehandlung/Retry fuer den Site-
    Manager-Connector-Proxy - genutzt sowohl von der offiziellen Integration-
    API (connector_get) als auch von der klassischen Controller-API
    (legacy_get), die beide ueber denselben Proxy-Tunnel laufen.

    Laeuft seit der Parallelisierung von poll() (ThreadPoolExecutor) in
    einem Worker-Thread, nicht mehr im Hauptthread - sys.exit() unten
    funktioniert trotzdem wie beabsichtigt: concurrent.futures faengt
    BaseException (SystemExit ist keine Exception-Unterklasse) im Worker ab
    und wirft sie beim naechsten future.result()-Aufruf im Hauptthread erneut
    - dort bricht sie den Prozess dann wie vorgesehen komplett ab. Per
    gemocktem Test verifiziert.

    timeout/Retry-Wartezeiten bewusst knapper als frueher (30s/10s): bei
    parallelen Abfragen bestimmt die LANGSAMSTE Konsole die Gesamtlaufzeit
    des gesamten Batches - eine haengende/nicht erreichbare Konsole soll den
    Batch nicht unnoetig lange blockieren. Der 429-Backoff (Rate-Limit vom
    Server selbst signalisiert) bleibt bewusst grosszuegiger, um nicht noch
    mehr 429s zu provozieren."""
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
                time.sleep(4)
                continue
            raise RuntimeError(f"Keine Verbindung zu api.ui.com (Connector-Proxy): {exc.reason}")
    return {}


def connector_get(host_id, path, timeout=12):
    """Ruft die offizielle, dokumentierte Network-Integration-API (v1) einer
    Konsole ueber den Site-Manager-Connector-Proxy auf (kein VPN/lokales Netz
    noetig)."""
    return _connector_request(host_id, "/proxy/network/integration/v1" + path, timeout)


def legacy_get(host_id, path, timeout=12):
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

def _thin_latency(reihe, n):
    """Reduziert (avg, loss)-Punkte auf n Stueck und behaelt je Abschnitt das
    MAXIMUM beider Groessen - sonst verschwinden genau die Latenzspitzen und
    Verlustereignisse, wegen derer man hinschaut (dieselbe Ueberlegung wie in
    _thin_keeping_peaks fuer den Flow-Chart)."""
    if len(reihe) <= n:
        return reihe
    out = []
    for i in range(n):
        lo = i * len(reihe) // n
        hi = max((i + 1) * len(reihe) // n, lo + 1)
        stueck = reihe[lo:hi]
        out.append([max(s[0] for s in stueck), max(s[1] for s in stueck)])
    return out


def fetch_latency(console_names_by_host):
    """Holt Latenz und Paketverlust aller Konsolen in EINEM Aufruf.

    WAS hier gemessen wird (vom Betreiber bestaetigt und nachgeprueft):
    Die UDM misst per ICMP ueber ihren WAN-Uplink. Nachgesehen ueber
    /stat/device haengen alle sechs Konsolen am selben physischen Port 9
    (ifname eth8, type "ethernet") - die UDM sieht dort also nur ein
    Ethernet-Kabel. Dahinter steckt aber das LTE-Modem: das Failover
    uebernimmt eine vorgeschaltete Sophos, die UDM selbst ist
    ausschliesslich ueber LTE angebunden. Die Zahl auf dem Dashboard
    beschreibt damit IMMER die LTE-Strecke und wechselt nie den Pfad - was
    fuer dieses Dashboard genau richtig ist, denn es geht um genau diese
    Leitung. (Die Latenzen von 6-20 ms waeren fuer Glasfaser zu hoch.)

    Stand 15.09.2026, voruebergehend: bei HAN, KLO und NID ist die
    Verkabelung noch nicht die richtige (bei KNZ, LSB und WTB schon). Dort
    antworten deshalb zwei der drei ICMP-Ziele gar nicht (www.microsoft.com
    und google.com, 0 % availability), nur 1.1.1.1 antwortet - und mit 6-12 ms
    auffallend schnell fuer eine LTE-Strecke. UniFi bildet fuer den
    WAN-Uplink dort folgerichtig keinen latency_average, weil zwei Monitore
    ausgefallen sind; dieselben drei Konsolen fuehren zusaetzlich eine
    Schnittstelle gre1 (type "wireless_5g") mit 22-51 ms.

    Der Betreiber stellt die Verkabelung in den Tagen nach dem 15.09.2026 um.
    Danach sollten dort alle drei Ziele antworten - und die angezeigte Latenz
    WIRD an diesen drei Standorten steigen, vermutlich in den Bereich der
    anderen drei (11-20 ms) oder hoeher. Das ist dann kein Defekt, sondern
    der erste ehrliche Messwert.

    Liefert {konsolenname: {"cur": ms, "loss": pct, "series": [[ms, loss], ...]}}.
    Konsolen ohne Daten fehlen im Ergebnis - der Aufrufer muss damit umgehen
    koennen (die Uebersicht zeigt dann einfach keine Kurve, statt gar nicht zu
    erscheinen)."""
    payload = api_get("/isp-metrics/5m", {"duration": "24h"})
    out = {}
    for eintrag in payload.get("data", []):
        name = console_names_by_host.get(eintrag.get("hostId"))
        if not name:
            continue  # fremde Konsole im Account, gehoert nicht zu MONITORED_HOSTS
        reihe = []
        for p in eintrag.get("periods", []):
            wan = (p.get("data") or {}).get("wan") or {}
            avg = wan.get("avgLatency")
            if avg is None:
                continue
            reihe.append([float(avg), float(wan.get("packetLoss") or 0)])
        if not reihe:
            continue
        # Haeufigkeit auf der VOLLEN Reihe zaehlen, nicht auf der gleich
        # ausgeduennten: _thin_latency behaelt je Abschnitt das Maximum, aus
        # 1 von 151 Punkten wuerde dort 1 von 30 - die Schwelle in
        # _loss_zeigen() waere damit wertlos.
        mit_verlust = [w for _a, w in reihe if w > 0]
        out[name] = {
            "cur": round(reihe[-1][0]),
            "loss": reihe[-1][1],
            "loss_punkte": len(mit_verlust),
            "punkte": len(reihe),
            "series": _thin_latency(reihe, LATENCY_POINTS),
        }
    return out


def refresh_latency(state, console_names_by_host, now):
    """Aktualisiert state['latency'], aber nur wenn der Stand aelter als
    LATENCY_REFRESH_S ist. Schlaegt der Abruf fehl, bleibt der letzte Stand
    stehen - eine fehlende Latenzkurve darf niemals den Poll scheitern lassen,
    an dem die eigentliche Volumenmessung haengt."""
    cache = state.get("latency") or {}
    ts = cache.get("ts")
    if ts:
        try:
            if (now - datetime.fromisoformat(ts)).total_seconds() < LATENCY_REFRESH_S:
                return cache.get("sites") or {}
        except (ValueError, TypeError):
            pass
    try:
        sites = fetch_latency(console_names_by_host)
    except Exception as exc:
        print(f"Hinweis: Latenz konnte nicht geholt werden ({exc}), behalte letzten Stand.")
        return cache.get("sites") or {}
    state["latency"] = {"ts": now.isoformat(), "sites": sites}
    return sites


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

    Verfaelscht keine Kennzahl: Flow-Chart (CHART_WINDOW_DAYS) und die
    Failover-Erkennung schauen nur auf die letzten paar Tage/Minuten.
    'Aktueller Monat'
    bleibt korrekt, da er nur die SUMME braucht, keine Einzelzeilen - die
    Aggregation ist reine Summenbildung.

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
FAILOVER_THRESHOLD_KBPS = 150.0
# Einzelne Standorte haben im Normalbetrieb eine deutlich hoehere Grundlast
# und loesten mit den globalen 150 kbps laufend Fehlalarme aus (WTB meldete
# an einem Vormittag vier Failover bei 178-602 kbps). Hier steht deshalb die
# eigene Schwelle, sonst gilt FAILOVER_THRESHOLD_KBPS.
FAILOVER_THRESHOLD_KBPS_BY_CONSOLE = {
    "WTB--UDM-1": 1500.0,   # 1,5 Mbps (Nutzervorgabe)
}
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
# Datenfrische-Anzeige ("SYNC" oben rechts): die Seite zaehlt clientseitig
# die Sekunden seit ihrem Erzeugungszeitpunkt hoch. Bis FRESH_WARN_S teal,
# danach amber, ab FRESH_STALE_S rot blinkend. Zweck: die Seite ist statisch
# und wird alle ~60s neu deployt - haengt der Workflow (Deploy-Livelock,
# Runner-Rueckstau, das gab es mehrfach), sieht sie trotzdem voellig normal
# aus, die Zahlen sind aber 20 Minuten alt und niemand merkt es. So markiert
# sie sich selbst als veraltet, ohne Reload und ohne Blick in GitHub Actions.
# FRESH_STALE_S bewusst = OFFLINE_THRESHOLD_S: dieselbe Toleranz, die auch
# eine Konsole als "Link Lost" einstuft.
FRESH_WARN_S = 180
FRESH_STALE_S = OFFLINE_THRESHOLD_S
# Ereignisprotokoll: Statuswechsel je Konsole (Nominal <-> Failover <-> Link
# Lost) werden in monitor_state.json festgehalten (EVENT_LOG_KEEP Eintraege
# rollierend) und die juengsten EVENT_LOG_SHOW auf der Uebersicht gezeigt.
# Vorher war ein Failover nur sichtbar, SOLANGE er lief - wer 10 Minuten
# spaeter draufschaute, sah nichts mehr davon.
EVENT_LOG_KEEP = 50
# Die Uebersicht muss ohne Scrollbalken auf EINEN Bildschirm passen (harte
# Vorgabe) - das Protokoll steht deshalb neben dem Systemschema statt darunter
# und zeigt nur wenige Eintraege; aeltere sind per Scrollen im Protokoll
# selbst erreichbar, ohne die Seitenhoehe zu veraendern.
EVENT_LOG_SHOW = 4

# Latenz im Systemschema (Nutzerwunsch: die Abzweigleitung IST die Messkurve).
# Quelle ist /isp-metrics/5m der Site-Manager-API - EIN Aufruf fuer alle
# Konsolen (1,8s gemessen) statt sechs paralleler Controller-Abfragen (6,5s).
# Gegengeprueft gegen die klassische /stat/health-API: die Werte stimmen bis
# auf 1-3 ms ueberein. Das ist wichtig, weil derselbe isp-metrics-Endpunkt bei
# den BYTE-Werten nachweislich falsch liegt (Faktor ~5000, siehe poll()) - fuer
# die Latenz bestaetigen sich beide Quellen gegenseitig.
#
# Die API liefert ohnehin nur alle 5 Minuten neue Punkte. Bei 1-Minuten-Takt
# waeren vier von fuenf Abrufen verschenkte Laufzeit, deshalb wird das Ergebnis
# in monitor_state.json zwischengespeichert und nur nachgeholt, wenn es aelter
# als LATENCY_REFRESH_S ist. Gespeichert wird die bereits ausgeduennte Reihe
# (~1,5 KB fuer alle sechs) und nicht die vollen ~288 Punkte je Konsole - der
# State wird bei JEDEM Poll committet, da gehoeren keine 35 KB hinein.
LATENCY_REFRESH_S = 240
LATENCY_POINTS = 30
# Ab wann ein Paketverlust als Prozentzahl in der Leiste steht. Die rote
# Kerbe erscheint immer; die ZAHL erst, wenn der Verlust nicht nur ein
# einzelner Ausreisser war - sonst liest sich "1 %" wie ein Dauerzustand,
# obwohl es fuenf Minuten von 24 Stunden waren. Dieselbe Unterscheidung
# trifft der UniFi Site Manager mit seinem "None" / "Detected".
LOSS_MIN_PUNKTE = 2      # mindestens so viele betroffene Messpunkte, ODER
LOSS_MIN_ANTEIL = 0.01   # mehr als dieser Anteil aller Messpunkte

# Umlaufdauern der Dauerbewegungen im Bild. Sie stehen als Konstanten hier,
# weil das Phasen-Skript (CANOPY_HTML) und das CSS denselben Wert brauchen -
# laufen sie auseinander, springt die Bewegung bei jedem Neuladen der Seite,
# statt durchzulaufen.
RADAR_SWEEP_S = 4.2      # Suchstrahl des Latenz-Radars, ein Umlauf
PULSE_TRAVEL_S = 2.6     # Tropfen auf den Abzweigleitungen im Bus

# Dauerbetrieb: der Flow-Chart bleibt auf ein recentes Fenster begrenzt, sonst
# wird er nach Wochen/Monaten Laufzeit unbrauchbar gross. Die Monatskennzahl
# ist davon unabhaengig.
CHART_WINDOW_DAYS = 1  # Flow-Chart der Uebersichtskacheln: 24 Stunden
ROLLING_AVG_MINUTES = 60  # Gleitendes Fenster fuer die Durchschnittslinie im Flow-Chart

# Ohne Rotation waechst wan_traffic.csv unbegrenzt und merge_rows() schreibt
# bei JEDEM Poll die komplette Datei neu - das wird schnell zum dominanten
# Kostenfaktor. Zeilen aelter als ROLLUP_AFTER_DAYS werden deshalb zu einer
# Zeile pro Kalendertag/Konsole/Uplink zusammengefasst (_rollup_old_rows()).
#
# Stand vorher: 35 Tage - so grosszuegig, dass die Rotation nach 34 Tagen
# Laufzeit noch KEIN einziges Mal gegriffen hatte und alle 284.657 Poll-
# Zeilen einzeln in einer 16,6-MB-Datei lagen, die jede Minute komplett neu
# geschrieben und committet wurde.
#
# Poll-genaue Aufloesung wird tatsaechlich nur gebraucht fuer die Charts
# (CHART_WINDOW_DAYS = 1 Tag) und die Failover-Erkennung (die letzten
# FAILOVER_CONSECUTIVE Polls). Alles Aeltere geht ausschliesslich in Summen
# ein, und die bleiben beim Zusammenfassen exakt erhalten (nachgerechnet:
# Down- und Up-Summen identisch). Gemessen an der echten CSV: 3 Tage ergaeben
# 1,5 MB (-91%), 7 Tage ergeben ~3,5 MB (-79%).
#
# Preis: poll-genaue Forensik ("was genau passierte im KNZ-Vorfall") reicht
# nur noch 7 Tage zurueck - bewusst so gewaehlt (Nutzerentscheidung), damit
# auch eine Nachfrage "was war da letzte Woche" noch minutengenau
# beantwortbar bleibt und nicht nur als Tagessumme.
ROLLUP_AFTER_DAYS = 7


def _console_failover_threshold(console_name):
    """Loest die Failover-Schwelle einer Konsole auf (kbps Upload-Durchschnitt).

    Substring-Match wie bei _console_alert_threshold(), damit auch eine
    Kurzform dieselbe Schwelle bekommt statt still auf den globalen Wert
    zurueckzufallen."""
    if console_name:
        needle = console_name.lower()
        for name, limit in FAILOVER_THRESHOLD_KBPS_BY_CONSOLE.items():
            if needle in name.lower() or name.lower() in needle:
                return limit
    return FAILOVER_THRESHOLD_KBPS


def _console_alert_threshold(console_name):
    """Loest die Rot-Schwelle einer Konsole auf - per Substring-Match (wie
    in_site() in compute_stats()), nicht per exaktem dict-Key-Vergleich, damit
    auch eine Kurzform wie 'lsb' dieselbe Schwelle bekommt wie die kanonische
    Konsole 'LSB--UDM-1', statt still auf den generischen Default
    zurueckzufallen."""
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


def compute_stats(rows, start, site_filter=None, rows_prefiltered=False):
    """Berechnet alle Kennzahlen fuer eine Konsole (oder alle, falls site_filter
    leer) und liefert sie als dict zurueck - roh, ohne HTML. Einziger Abnehmer
    ist die Uebersichtsseite (render_overview_html).

    Die Kacheln verzichten bewusst auf Hover-Tooltips und die dafuer noetigen
    eingebetteten Rohdaten (Nutzerwunsch: weder gebraucht noch gewuenscht) -
    das spart Groesse und Rechenzeit auf der Seite, die bei jedem Poll neu
    committet und gepusht wird.

    Dauerbetrieb (kein festes Messende mehr): start ist der einmalige
    Messbeginn, es gibt kein "end". Einzige Volumenkennzahl ist der aktuelle
    Kalendermonat, aufgeteilt in Download und Upload. Der Flow-Chart bleibt
    auf ein kuerzeres, recentes Fenster begrenzt (CHART_WINDOW_DAYS), sonst
    wuerde er nach Wochen/Monaten Laufzeit riesig und unbrauchbar.

    "Aktueller Monat" wird IMMER strikt aus den CSV-Deltas seit dem 1. des
    Kalendermonats summiert - NICHT aus dem rohen absoluten SIM-Zaehlerstand.
    Fruehere Annahme war, der Provider setze diesen Zaehler nachweislich
    monatlich zurueck, weshalb er direkt als "Aktueller Monat" verwendet
    wurde. Ueber die gesamte bisherige Laufzeit (>1 Monat seit Messbeginn)
    hat sich aber KEIN einziger Reset gezeigt (kein Poll hat je einen Abfall
    des Zaehlerstands registriert) - der Zaehler laeuft einfach durch,
    unabhaengig vom Kalendermonat. Das fuehrte zu falschen, weit ueberhoehten
    Monatswerten (z.B. "Monat" hoeher als "Letzte 30 Tage", ein logischer
    Widerspruch, der die Konsolenbetreiber bei KNZ/WTB aufmerksam machte -
    235 GB/254 GB "Monat" statt der tatsaechlichen ~11 GB/~14 GB seit dem 1.).
    Die CSV besteht seit Messbeginn durchgehend aus SIM-Zaehler-Deltas (keine
    Vermischung mit der alten, abgeloesten Rate-Schaetzung mehr) - die Summe
    ueber den Kalendermonat ist daher genauso exakt UND (im Gegensatz zum
    rohen Zaehlerstand) tatsaechlich auf den Kalendermonat begrenzt.
    """
    now = datetime.now(timezone.utc)

    # rows_prefiltered: im Dauerbetrieb hat _group_by_console() die Zeilen
    # bereits exakt nach Konsole aufgeteilt - sie hier per Substring ERNEUT
    # zu pruefen war reine Doppelarbeit (284.657 in_site-Aufrufe mit 569.314
    # .lower()-Allokationen je Bericht, ~0,16s). Der Filter selbst bleibt
    # erhalten: compute_stats() muss auch mit einer ungefilterten Zeilenliste
    # umgehen koennen (rows_prefiltered=False).
    if rows_prefiltered and site_filter and rows:
        # Billige Plausibilitaetspruefung (erste und letzte Zeile), denn ein
        # falsches rows_prefiltered=True faellt sonst NICHT auf: es wuerde
        # stillschweigend ueber alle Konsolen summieren und die Kachel
        # trotzdem mit einem einzelnen Konsolennamen beschriften. Genau diese
        # Klasse Fehler (plausibel aussehende, aber falsche Volumenzahlen)
        # hat schon einmal fuer Rueckfragen der Konsolenbetreiber gesorgt.
        # Im Zweifel lieber doch filtern als falsche Zahlen anzeigen - und
        # laut sein, statt den Poll abzubrechen.
        needle = site_filter.lower()
        for probe in (rows[0], rows[-1]):
            if needle not in probe["site"].lower() and needle not in probe["uplink"].lower():
                print(f"Warnung: compute_stats({site_filter}) mit rows_prefiltered=True aufgerufen, "
                      f"aber die Zeilen enthalten auch '{probe['site']}' - filtere sicherheitshalber selbst.")
                rows_prefiltered = False
                break

    if rows_prefiltered or not site_filter:
        all_rows = [r for r in rows if r["ts"] >= start]
    else:
        needle = site_filter.lower()

        def in_site(r):
            return needle in r["site"].lower() or needle in r["uplink"].lower()

        all_rows = [r for r in rows if r["ts"] >= start and in_site(r)]


    # Aktueller Kalendermonat (lokale Zeit, damit "Monat" dem echten
    # Kalendermonat entspricht, nicht UTC).
    now_local = now.astimezone()
    month_start_local = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_start = month_start_local.astimezone(timezone.utc)
    # Fortschrittsbalken/"Tag X von Y" soll den echten Kalendertag zeigen,
    # unabhaengig davon, ob fuer alle Tage schon Daten vorliegen.
    days_elapsed_month_calendar = max((now - month_start).total_seconds() / 86400.0, 0.001)
    days_in_month = calendar.monthrange(now_local.year, now_local.month)[1]

    # Strikt auf den Kalendermonat begrenzte Summe der CSV-Deltas - siehe
    # Docstring oben, warum der rohe SIM-Zaehlerstand dafuer NICHT mehr
    # verwendet wird.
    month_rows = [r for r in all_rows if r["ts"] >= month_start]
    month_down = sum(r["down_bytes"] for r in month_rows)
    month_up = sum(r["up_bytes"] for r in month_rows)
    total_month = month_down + month_up
    month_data_start = max(month_start, start)
    days_elapsed_month = max((now - month_data_start).total_seconds() / 86400.0, 0.001)
    per_day_month = total_month / days_elapsed_month
    projected_month = per_day_month * days_in_month

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
            is_failover = last_rate_kbps > _console_failover_threshold(site_filter)

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

    # Die Kacheln kommen bewusst ohne Hover-Tooltips aus (Nutzerwunsch) und
    # brauchen deshalb auch die dafuer noetigen eingebetteten Rohdaten nicht.
    # Die Stundenwerte selbst (series/peak) bleiben - sie kosten fast nichts,
    # laufen nur ueber das 1-Tage-Chartfenster, und "Spitze X/h" steht
    # weiterhin an der Kachel.
    flow_chart_mini = render_flow_chart(chart_rows, chart_start, now, max_points=200)
    device = site_filter or (all_rows[0]["site"] if all_rows else (rows[0]["site"] if rows else "unbekannt"))
    return dict(
        window=all_rows, month_down=month_down, month_up=month_up,
        total_month=total_month, per_day_month=per_day_month, projected_month=projected_month,
        days_elapsed_month=days_elapsed_month, days_in_month=days_in_month,
        days_elapsed_month_calendar=days_elapsed_month_calendar,
        start=start, now=now, peak=peak, device=device,
        flow_chart_mini=flow_chart_mini,
        is_failover=is_failover, last_rate_kbps=last_rate_kbps, is_offline=is_offline,
        last_seen=last_seen,
    )


def _thin_keeping_peaks(samples, max_points):
    """Duennt (ts, down_kbps, up_kbps) auf hoechstens max_points aus und
    BEHAELT dabei die Spitzen.

    Vorher wurde schlicht jeder n-te Messpunkt genommen (step = len/max,
    pts[int(i*step)]) - also 6 von 7 Punkten kommentarlos weggeworfen. Damit
    verschwanden echte Ausschlaege komplett aus dem Bild: gemessen ueber 24h
    zeigte der Chart bei HAN nur 4 statt 15 kbps Spitze (-73%), bei WTB 1873
    statt 2507 kbps (-25%). Fuer ein Failover-Dashboard, in dem genau die
    Spitzen die Aussage sind, war das die falsche Reduktion.

    Stattdessen je Abschnitt das Maximum beider Reihen. Beide Werte sind im
    Abschnitt tatsaechlich gemessen worden, es wird also nichts erfunden; und
    der Zeitstempel ist der des groesseren Ausschlags, damit die Spitze an
    der richtigen Stelle der Zeitachse steht (die Punkte stehen dadurch nicht
    exakt aequidistant, bleiben aber chronologisch)."""
    n = len(samples)
    if not max_points or n <= max_points:
        return samples
    out = []
    for i in range(max_points):
        lo = i * n // max_points
        hi = max((i + 1) * n // max_points, lo + 1)
        chunk = samples[lo:hi]
        ts = max(chunk, key=lambda s: max(s[1], s[2]))[0]
        out.append((ts, max(s[1] for s in chunk), max(s[2] for s in chunk)))
    return out


def render_flow_chart(window, start, end, max_points=None):
    """Feinkoerniger Traffic-Flow-Graph: Rate (kbps) je Poll-Punkt ueber die Zeit.

    max_points: bei CHART_WINDOW_DAYS=1 und 1-Minuten-Poll-Takt waeren das bis
    zu ~1440 Punkte PRO Konsole, eingebettet als SVG-Pfadkoordinaten (5
    Polylines/Polygon). Das war ein Haupttreiber dafuer, dass
    wan_report.html auf ueber 1 MB wuchs und der poll-Job dadurch beim
    Committen/Pushen zunehmend laenger brauchte (bis an den Rand des
    1-Minuten-Poll-Takts). Deshalb eine leichtgewichtige Fassung ohne
    Hover-Daten und mit deutlich weniger Punkten."""
    width, height = 960, 200
    # bottom/top waren 28 und 12: unten fuer eine Datumszeile, oben als Rand.
    # Damit blieben der Kurve nur 160 von 200 Einheiten. Die Datumszeile ist
    # entfallen (bei 24 Stunden Fenster steht dort hoechstens ein Datum, und
    # die Kachelbeschriftung sagt "Flow - 24 h"), der obere Rand reicht mit 8
    # - die Kurve beruehrt ihn nur im Punkt der Spitze. Jetzt 184 von 200,
    # also 15 % mehr Kurvenhoehe bei gleicher Anzeigehoehe.
    left, bottom, top = 54, 8, 8
    plot_w = width - left - 12
    plot_h = height - bottom - top

    pts = sorted((r for r in window if r["interval_s"]), key=lambda r: r["ts"])
    if len(pts) < 2:
        return '<p class="dim" style="margin:0">Noch nicht genug Messpunkte fuer den Flow-Graphen.</p>'

    span_s = max((end - start).total_seconds(), 1)

    def rate_kbps(bytes_, interval_s):
        return bytes_ * 8.0 / interval_s / 1000.0

    samples = [(r["ts"],
                rate_kbps(r["down_bytes"], r["interval_s"]),
                rate_kbps(r["up_bytes"], r["interval_s"])) for r in pts]
    samples = _thin_keeping_peaks(samples, max_points)
    peak = max((max(d, u) for _, d, u in samples), default=0.0) or 1.0
    # Logarithmische Hoehen-Skalierung (log1p): linear verschwindet die
    # Grundlast ruhiger Konsolen (LSB,
    # HAN, KLO, NID) neben seltenen hohen Failover-Ausschlaegen komplett.
    peak_scaled = math.log1p(peak) or 1.0

    def xy(ts, value):
        x = left + (ts - start).total_seconds() / span_s * plot_w
        scaled = math.log1p(max(value, 0.0)) / peak_scaled if peak_scaled else 0.0
        y = top + plot_h - scaled * plot_h
        return x, y

    baseline_y = top + plot_h
    down_line = [xy(ts, d) for ts, d, u in samples]
    up_line = [xy(ts, u) for ts, d, u in samples]
    down_pts_str = " ".join(f"{x:.1f},{y:.1f}" for x, y in down_line)
    up_pts_str = " ".join(f"{x:.1f},{y:.1f}" for x, y in up_line)

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
        y = top + plot_h * (1 - i / 4.0)
        label_value = math.expm1((i / 4.0) * peak_scaled)
        label = f"{label_value:,.0f} kbps".replace(",", ".")
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" class="grid"/>')
        parts.append(f'<text x="{left - 8}" y="{y + 4:.1f}" class="axis" text-anchor="end">{label}</text>')

    day_cursor = start.astimezone().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    end_local = end.astimezone()
    while day_cursor < end_local:
        x, _ = xy(day_cursor.astimezone(timezone.utc), 0)
        # Nur die senkrechte Mitternachtslinie, ohne Datumstext darunter -
        # der kostete 28 der 200 viewBox-Einheiten Kurvenhoehe.
        parts.append(f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{baseline_y:.1f}" class="daymark"/>')
        day_cursor += timedelta(days=1)

    # Ohne Flaeche unter der Download-Linie (Nutzerwunsch: zwei leuchtende
    # Linien auf Glas wirken passender als gefuellte Masse). Das Polygon wird
    # gar nicht erst erzeugt statt nur unsichtbar geschaltet - es wiederholt
    # saemtliche Punkte der Linie und machte rund ein Fuenftel des
    # Chart-Markups aus.
    parts.append(f'<polyline points="{down_pts_str}" class="flow-down-line"/>')
    parts.append(f'<polyline points="{up_pts_str}" class="flow-up-line"/>')
    parts.append(f'<polyline points="{avg_down_pts_str}" class="flow-avg-down-line"/>')
    parts.append(f'<polyline points="{avg_up_pts_str}" class="flow-avg-up-line"/>')
    parts.append(f'<line x1="{left}" y1="{baseline_y:.1f}" x2="{left + plot_w}" y2="{baseline_y:.1f}" class="baseline"/>')
    return (f'<svg viewBox="0 0 {width} {height}" preserveAspectRatio="none" class="chart flow-chart" '
            f'role="img" aria-label="Traffic-Flow">'
            f'{"".join(parts)}</svg>')


# Zwei komplette Farbpaletten, ueber COLOR_THEME unten umschaltbar. "default"
# ist das bisherige dunkle Layout - bleibt vollstaendig im Code erhalten als
# Fallback, falls die Weisgerber-Testfarben nicht gefallen (einfach
# COLOR_THEME wieder auf "default" setzen und pushen, kein Code-Loeschen
# noetig). "weisgerber" ist testweise aus den tatsaechlich auf
# weisgerber-umweltservice.de verwendeten Farben abgeleitet (Markengruen
# #008351, Akzent-Orange #e26e0e, Header/Text schwarz, Seite weiss) - Alert-/
# Warn-Rot/Gelb wurden dabei bewusst NICHT von dort uebernommen (die Seite
# hat keine), sondern separat auf ausreichenden Kontrast zu Weiss geprueft.
COLOR_THEMES = {
    "default": {
        "ink": "#0e1620", "panel": "#16212e", "line": "#24344a",
        "text": "#dbe6f0", "dim": "#7f93a8", "strong": "#ffffff",
        "down": "#46b3a3", "up": "#e0a458", "alert": "#d4675b", "warn": "#e8c14c",
        "failover_bg": "#2a1414", "failover_bg_strong": "#3a1414", "failover_border_strong": "#ff6b6b",
        "h2_bg": "transparent", "h2_color": "var(--dim)", "h2_padding": "0", "h2_radius": "0",
        "logo": "negative",  # Farblayout zurueckgesetzt, Logo (transparent, passt auf dunklen Grund) bleibt
    },
    # Aus dem offiziellen Corporate-Design-Handbuch (Stand Februar 2026), NICHT
    # mehr von der Live-Website: Primaerfarben sind Tieforange RAL 2011
    # (#e26e0e), Reinweiss und Schwarz - Schwarz dabei AUSDRUECKLICH nur als
    # Typografiefarbe, nie als Hintergrund/Flaechenfarbe. Das fruehere
    # Gruen von der Website gehoert zu einem im Handbuch explizit
    # durchgestrichenen, nicht mehr verwendeten Sekundaerfarbschema - daher
    # hier NICHT uebernommen. Down/Up im Chart nutzen stattdessen die vom
    # Handbuch selbst fuer Diagramme vorgesehenen Tieforange-Aufhellungen
    # (100% / ~85%), da das Handbuch keine zweite Akzentfarbe mehr vorsieht -
    # dadurch etwas subtiler unterscheidbar als vorher, siehe Chat-Hinweis.
    # Alert-Rot/Warn-Gelb sind vom Handbuch nicht abgedeckt (reine Failover-
    # Statusfarben) und bewusst beibehalten fuer eindeutige Lesbarkeit.
    "weisgerber": {
        "ink": "#ffffff", "panel": "#ffffff", "line": "#e2e2e2",
        "text": "#000000", "dim": "#6b6b6b", "strong": "#000000",
        "down": "#e26e0e", "up": "#e68432", "alert": "#c0392b", "warn": "#b8860b",
        "failover_bg": "#fdecea", "failover_bg_strong": "#fbdad6", "failover_border_strong": "#e0483a",
        "h2_bg": "var(--down)", "h2_color": "#ffffff", "h2_padding": "5px 14px", "h2_radius": "8px",
        "logo": "block",
    },
    # Dunkle Variante: gleiche Markenfarbe (Tieforange), aber nur noch als
    # Akzent (Badges, Links, ein Chart-Wert) statt als flaechendeckendes
    # Weiss - Reaktion auf "das ist sehr orange". Hintergrund/Flaechen sind
    # ein warmes Dunkelbraun-Schwarz (nicht rein neutral-grau), damit der
    # Markencharakter trotz dunklem Grund erhalten bleibt.
    "weisgerber-dark": {
        "ink": "#17130f", "panel": "#221c16", "line": "#3d332a",
        "text": "#f0ece5", "dim": "#a89686", "strong": "#ffffff",
        "down": "#e26e0e", "up": "#f2a44e", "alert": "#d4675b", "warn": "#e8c14c",
        "failover_bg": "#2a1410", "failover_bg_strong": "#3a1a12", "failover_border_strong": "#ff6b4a",
        "h2_bg": "var(--down)", "h2_color": "#ffffff", "h2_padding": "5px 14px", "h2_radius": "8px",
        "logo": "negative",
    },
    # Leitstand-/HUD-Optik (Nutzerwunsch, aus der Stilstudie "WAN-
    # Kommandobruecke" uebernommen): dieselbe Farbfamilie wie "default"
    # (Teal fuer Down, Amber fuer Up, Rot fuer Alert), nur gesaettigter und
    # heller fuer den Charakter eines leuchtenden Konsolen-Displays, auf
    # nahezu schwarzem Grund. Bewusst KEIN reines Neon-Gruen-auf-Schwarz.
    # Zusaetzliche HUD-Tokens (--hull-2, --phosphor-dim, --alert-dim) werden
    # nur von der Uebersichtsseite gebraucht und dort direkt gesetzt.
    "hud": {
        "ink": "#090c0f", "panel": "#10161b", "line": "#263139",
        "text": "#d9ece9", "dim": "#6d848c", "strong": "#ffffff",
        "down": "#5fe0d1", "up": "#e6ac5c", "alert": "#ff5a4d", "warn": "#e8c14c",
        "failover_bg": "#1a1210", "failover_bg_strong": "#241410", "failover_border_strong": "#ff5a4d",
        "h2_bg": "transparent", "h2_color": "var(--dim)", "h2_padding": "0", "h2_radius": "0",
        "logo": "negative",
    },
    # Glasprojektion (Nutzerwunsch nach der Stilstudie "Glaskanzel"): wie
    # "hud", aber das Licht wirkt auf eine Cockpit-Scheibe geworfen statt auf
    # einen Bildschirm gemalt. Deshalb duenner und heller - projiziertes Licht
    # liest sich heller als leuchtende Pixel. Grund ist fast schwarz, aber
    # cyan-gestochen (nicht neutral), damit die kalte Tiefenebene dahinter
    # (siehe GLASS_CSS .depth) nicht grau wirkt.
    "glas": {
        "ink": "#04070a", "panel": "#0b1218", "line": "rgba(160, 220, 235, .10)",
        "text": "#d6f0ee", "dim": "#6b8b92", "strong": "#ffffff",
        "down": "#7ff0e4", "up": "#ffc47a", "alert": "#ff6a58", "warn": "#e8c14c",
        "failover_bg": "#1a100e", "failover_bg_strong": "#241410", "failover_border_strong": "#ff6a58",
        "h2_bg": "transparent", "h2_color": "var(--dim)", "h2_padding": "0", "h2_radius": "0",
        "logo": "negative",
    },
}
# "glas" = Glasprojektion (aktuell). "hud" = vorheriges Leitstand-Layout auf
# undurchsichtigen Kacheln, "default" = urspruengliches dunkles Kachel-Layout.
# Beide bleiben vollstaendig im Code: ein Wort hier schaltet zurueck.
COLOR_THEME = "glas"


# Logo-Bilddaten aus dem offiziellen CI-Handbuch extrahiert (Seite 1: Block-
# Variante, weisser Schriftzug auf Tieforange-Flaeche, fuer helle Seiten-
# Hintergruende; Seite 2: "Negativdesign", Tieforange-Schriftzug ohne
# Flaeche/transparenter Hintergrund, fuer dunkle Hintergruende), auf
# Webgroesse herunterskaliert und palettenquantisiert, um die generierten
# Seiten klein zu halten.
LOGO_BLOCK_B64 = (
    "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"
)
LOGO_NEGATIVE_B64 = (
    "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"
)


def _logo_html():
    """Logo-<img>-Tag fuer den Seitenkopf, falls das aktuelle Theme eines
    definiert (COLOR_THEMES[COLOR_THEME]['logo']) - sonst leerer String
    (z.B. beim urspruenglichen dunklen Standard-Theme, das kein
    Markenlogo hat)."""
    kind = COLOR_THEMES[COLOR_THEME].get("logo")
    if kind == "block":
        return ('<img class="brand-logo" src="data:image/png;base64,' + LOGO_BLOCK_B64
                + '" alt="Weisgerber Umweltservice">')
    if kind == "negative":
        return ('<img class="brand-logo" src="data:image/png;base64,' + LOGO_NEGATIVE_B64
                + '" alt="Weisgerber Umweltservice">')
    return ""


def _root_css_vars(theme):
    t = COLOR_THEMES[theme]
    return (
        "  :root {\n"
        f"    --ink: {t['ink']}; --panel: {t['panel']}; --line: {t['line']};\n"
        f"    --text: {t['text']}; --dim: {t['dim']}; --strong: {t['strong']};\n"
        f"    --down: {t['down']}; --up: {t['up']}; --alert: {t['alert']}; --warn: {t['warn']};\n"
        f"    --failover-bg: {t['failover_bg']}; --failover-bg-strong: {t['failover_bg_strong']};\n"
        f"    --failover-border-strong: {t['failover_border_strong']};\n"
        f"    --h2-bg: {t['h2_bg']}; --h2-color: {t['h2_color']};\n"
        f"    --h2-padding: {t['h2_padding']}; --h2-radius: {t['h2_radius']};\n"
        "  }\n"
    )


BASE_CSS = _root_css_vars(COLOR_THEME) + """
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--ink); color: var(--text);
    font: 15px/1.55 "Inter", "Segoe UI", system-ui, sans-serif; padding: 32px 24px 42px;
    overflow-x: hidden; }
  .wrap { max-width: 1020px; margin: 0 auto; }
  header { border-bottom: 1px solid var(--line); padding-bottom: 18px; margin-bottom: 26px; }
  .header-top { display: flex; justify-content: space-between; align-items: flex-start; gap: 24px; }
  .header-info { flex: 1 1 auto; min-width: 0; }
  .brand-logo { flex: 0 0 auto; height: 42px; width: auto; }
  h1 { font-size: 25px; margin: 0 0 6px; letter-spacing: -.01em; font-weight: 600; }
  #refresh-cd { color: var(--warn); font-weight: 600; font-family: ui-monospace, monospace; }
  .threshold-ref { font-size: .55em; font-weight: 500; color: var(--dim); }
  h2 { display: inline-block; background: var(--h2-bg); color: var(--h2-color);
    padding: var(--h2-padding); border-radius: var(--h2-radius);
    font-size: 15px; text-transform: uppercase; letter-spacing: .1em;
    margin: 34px 0 12px; font-weight: 600; }
  .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 18px; }
  .chart { width: 100%; height: auto; }
  .chart .grid { stroke: var(--line); stroke-width: 1; }
  .chart .baseline { stroke: var(--dim); stroke-width: 1; }
  .chart .daymark { stroke: var(--line); stroke-dasharray: 3 4; }
  .chart .axis { fill: var(--dim); font: 10.5px ui-monospace, monospace; }
  .chart .flow-down-line { fill: none; stroke: var(--down); stroke-width: 1.6; }
  .chart .flow-up-line { fill: none; stroke: var(--up); stroke-width: 1.6; }
  .chart .flow-avg-down-line { fill: none; stroke: var(--down); stroke-width: 1.1;
    stroke-dasharray: 5 3; opacity: .8; }
  .chart .flow-avg-up-line { fill: none; stroke: var(--up); stroke-width: 1.1;
    stroke-dasharray: 5 3; opacity: .8; }
  .dot { display: inline-block; width: 9px; height: 9px; border-radius: 2px; margin-right: 6px; }
  th { text-align: left; color: var(--dim); font-weight: 500; font-size: 12px;
    text-transform: uppercase; letter-spacing: .07em; padding: 0 10px 10px; }
  .num { text-align: right; font-family: ui-monospace, monospace; }
  .strong { color: var(--strong); }
  .dim { color: var(--dim); }
  .value-warn { color: var(--warn); }
  .value-alert { color: var(--alert); }
  footer { color: var(--dim); font-size: 12.5px; margin-top: 34px;
    border-top: 1px solid var(--line); padding-top: 14px; }
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
  var freshWarnS = {FRESH_WARN_S}, freshStaleS = {FRESH_STALE_S};
  // Wie oft bei der Standdatei nachgefragt wird. Sie ist rund 30 Byte gross,
  // ein kurzer Takt kostet also praktisch nichts - und die Seite zeigt einen
  // neuen Stand dadurch frueher als der alte 60-Sekunden-Countdown.
  var standPruefMs = 5000;
  var el = document.getElementById('refresh-cd');
  var syncVal = document.getElementById('sync-val');
  var syncDot = document.querySelector('.sync-dot');
  if (!el) return;
  // Datenfrische (siehe FRESH_WARN_S): Alter des Datenstands, nicht der
  // Reload-Countdown - nach einem Reload ohne neuen Poll bleibt
  // generatedAtMs gleich und das Alter waechst weiter, genau das soll
  // sichtbar werden.
  function syncTick(elapsedS) {{
    if (!syncVal || !syncDot) return;
    syncVal.textContent = elapsedS < 60 ? elapsedS + 's'
      : Math.floor(elapsedS / 60) + 'm ' + (elapsedS % 60) + 's';
    syncDot.className = 'sync-dot' + (elapsedS > freshStaleS ? ' stale' : (elapsedS > freshWarnS ? ' aging' : ''));
  }}
  function tick() {{
    var elapsedS = Math.floor((Date.now() - generatedAtMs) / 1000);
    syncTick(Math.max(elapsedS, 0));
    var remaining = Math.max(refreshS - elapsedS, 0);
    var m = Math.floor(remaining / 60), s = remaining % 60;
    el.textContent = m + ':' + (s < 10 ? '0' : '') + s;
    setTimeout(tick, 1000);
  }}
  tick();

  // Reload nur bei WIRKLICH neuem Stand. Frueher lief er an einem Countdown:
  // 60 Sekunden nach dem Erzeugungszeitpunkt der angezeigten Seite. Weil
  // zwischen Erzeugung und Sichtbarkeit aber rund 25 Sekunden liegen
  // (Workflow + Pages-Deploy), war die frisch geladene Seite oft schon
  // ueberfaellig - der Countdown stand sofort wieder auf 0 und loeste den
  // naechsten Reload aus. Gemessen waren das 3 Ladevorgaenge pro Minute,
  // sichtbar als Ruckeln in allen Dauerbewegungen.
  function standPruefen() {{
    fetch('wan_stand.txt?_=' + Date.now(), {{ cache: 'no-store' }})
      .then(function (r) {{ return r.ok ? r.text() : null; }})
      .then(function (text) {{
        if (!text) return;
        var standMs = new Date(text.trim()).getTime();
        // Nur bei ECHT neuerem Stand neu laden. Ein aelterer Wert kann aus
        // einem CDN-Zwischenspeicher kommen und darf nichts ausloesen.
        if (standMs > generatedAtMs) {{
          window.location.href = window.location.pathname + '?_=' + Date.now();
        }}
      }})
      .catch(function () {{ /* Netz weg: einfach beim naechsten Versuch wieder */ }});
  }}
  setInterval(standPruefen, standPruefMs);
}})();
</script>"""


def _console_status(c):
    """Einheitliche Status-Einstufung einer Konsole fuer Uebersicht, Schema
    und Ereignisprotokoll: 'failover' > 'offline' > 'nominal'. Failover hat
    Vorrang, is_offline setzt is_failover in compute_stats() aber ohnehin
    schon zurueck, beides gleichzeitig kommt also nicht vor."""
    if c["is_failover"]:
        return "failover"
    if c["is_offline"]:
        return "offline"
    return "nominal"


def _segments_html(total, lit, mode=""):
    """Segment-Messer (HUD-Motiv statt durchgehendem Balken): total Segmente,
    die ersten lit davon 'leuchten'. mode ('warn'/'crit') faerbt die
    leuchtenden Segmente amber/rot."""
    lit = max(0, min(int(lit), total))
    lit_cls = "lit" + (f" {mode}" if mode else "")
    cells = "".join(f'<i class="{lit_cls}"></i>' if i < lit else "<i></i>" for i in range(total))
    return f'<div class="segments">{cells}</div>'


# Latenz-Radar: Vollausschlag am aeusseren Ring und Zahl der Ringe. 40 ms
# deckt die real gemessenen 5-25 ms mit Reserve ab, ohne dass alle Punkte in
# der Mitte kleben.
RADAR_MAX_MS = 40.0
RADAR_RINGE = 4


def _radar_html(consoles, latency):
    """Latenz-Radar fuer den Statuspylon.

    Winkel = Standort (ein Sektor je Konsole, Reihenfolge wie im Bus),
    Abstand von der Mitte = Latenz. Das ist bewusst KEIN Dekor: jede Position
    steht fuer einen gemessenen Wert, und eine Konsole ohne Messwert bekommt
    gar keinen Punkt - einer in der Mitte hiesse 'null Millisekunden'."""
    latency = latency or {}
    blips = []
    for i, c in enumerate(consoles):
        lat = latency.get(c["device"]) or {}
        ms = lat.get("cur")
        if ms is None:
            continue
        kurz = html.escape(c["device"].split("--")[0])
        cls = {"failover": "alert", "offline": "lost"}.get(_console_status(c), "ok")
        winkel = math.radians(-90 + i * 360.0 / max(len(consoles), 1))
        # 6 % Totzone innen, damit Punkte nahe 0 ms nicht im Fadenkreuz sitzen
        r = 6 + min(ms / RADAR_MAX_MS, 1.0) * 40
        x = 50 + r * math.cos(winkel)
        y = 50 + r * math.sin(winkel)
        blips.append(f'<i class="blip {cls}" style="left:{x:.1f}%;top:{y:.1f}%" '
                     f'title="{kurz}: {int(ms)} ms"><b>{kurz}</b></i>')
    ringe = "".join(
        f'<i class="ring" style="width:{(i + 1) * 92.0 / RADAR_RINGE:.1f}%;'
        f'height:{(i + 1) * 92.0 / RADAR_RINGE:.1f}%" '
        f'title="{int((i + 1) * RADAR_MAX_MS / RADAR_RINGE)} ms"></i>'
        for i in range(RADAR_RINGE))
    return (f'<div class="radar" title="Latenz-Radar &middot; Winkel = Standort, '
            f'Abstand von der Mitte = Latenz, aeusserer Ring {int(RADAR_MAX_MS)} ms">'
            f'{ringe}<i class="sweep"></i>{"".join(blips)}'
            f'<b class="rlabel">{int(RADAR_MAX_MS)} ms</b></div>')


def _split_unit(text):
    """'31.5 GB' -> ('31.5', 'GB') fuer getrennt gestylte Einheit in den
    Telemetrie-Kacheln. Kein Leerzeichen -> Einheit leer."""
    parts = text.rsplit(" ", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else (text, "")


def _loss_zeigen(lat):
    """Ist der Paketverlust haeufig genug, um als Prozentzahl zu erscheinen?

    Braucht die Zaehlung aus fetch_latency (loss_punkte/punkte). Fehlt sie -
    etwa bei einem Zwischenspeicher aus einer aelteren Fassung -, wird die
    Zahl gezeigt wie bisher: lieber einmal zu viel anzeigen als einen
    echten Verlust verschweigen."""
    punkte = lat.get("punkte")
    betroffen = lat.get("loss_punkte")
    if not punkte or betroffen is None:
        return True
    return betroffen >= LOSS_MIN_PUNKTE or betroffen / punkte > LOSS_MIN_ANTEIL


def _loss_nicks(series, max_marks=4, min_abstand=0.14):
    """Rote Kerben auf der geraden Abzweigleitung, dort wo es in den letzten
    24 Stunden Paketverlust gab - oben vor 24 h, unten jetzt.

    Vorgaenger war eine Schlangenlinie, deren Auslenkung die Latenz zeigte.
    Die ist raus (Nutzerwunsch): sie liess die Leitung am Knoten vorbeilaufen,
    und der wandernde Punkt bekam ueber den Selektor '.snode.ok .pulse'
    zusaetzlich zur SMIL-Bewegung noch die CSS-Animation des alten
    HTML-Elements ab - zwei ueberlagerte Bewegungen, der Punkt fiel sichtbar
    neben der Linie herunter.

    Begrenzt auf max_marks Kerben mit Mindestabstand: bei 30 Stuetzpunkten auf
    20 px Leitung saehen zehn Marken wie eine gestrichelte Linie aus und
    saegten die Aussage kaputt. Gezeigt werden die staerksten Ereignisse."""
    treffer = []
    n = max(len(series) - 1, 1)
    for i, punkt in enumerate(series):
        try:
            pl = float(punkt[1])
        except (TypeError, ValueError, IndexError):
            continue
        if pl > 0:
            treffer.append((pl, i / n))
    if not treffer:
        return "", 0, 0.0
    gesamt = len(treffer)
    treffer.sort(key=lambda t: -t[0])
    gewaehlt = []
    for pl, pos in treffer:
        if all(abs(pos - p) >= min_abstand for _, p in gewaehlt):
            gewaehlt.append((pl, pos))
        if len(gewaehlt) >= max_marks:
            break
    return "".join(f'<i class="nick" style="top:{pos * 100:.0f}%"></i>'
                   for _, pos in sorted(gewaehlt, key=lambda t: t[1])), gesamt, max(pl for pl, _ in treffer)


def _schema_html(consoles, latency=None):
    """Systemschema als Bus-Diagramm: links der Site-Manager (die UniFi-
    Cloud-API, ueber die ALLE Konsolen gepollt werden - das ist die reale
    Topologie dieses Monitors, kein erfundener 'WAN-Kern'), davon eine
    Bus-Linie mit einem Abzweig je Konsole. Knotenfarbe = aktueller Status
    wie in den Panels; auf nominalen Abzweigen wandert ein Puls (rein
    dekorativ, respektiert prefers-reduced-motion).

    Bewusst HTML/CSS statt SVG: ein SVG braucht eine feste viewBox, und die
    passt nie zur tatsaechlichen Containerbreite - bei 'meet' entstehen
    breite Leerraender und der letzte Knoten rutscht aus dem (per
    overflow-x:hidden abgeschnittenen) Bild, bei 'none' werden die runden
    Knoten zu Ellipsen. Als Flex-Zeile verteilt sich das Schema dagegen bei
    jeder Breite korrekt, ohne dass Schrift oder Knoten mitskalieren."""
    latency = latency or {}
    nodes = []
    for i, c in enumerate(consoles):
        status = _console_status(c)
        cls = {"failover": "alert", "offline": "lost"}.get(status, "ok")
        short = html.escape(c["device"].split("--")[0])
        zustand = {"alert": "Failover", "lost": "Link Lost"}.get(cls, "Nominal")
        lat = latency.get(c["device"]) or {}

        # Gerade Leitung (Nutzerwunsch, die Latenz-Schlangenlinie ist raus).
        # Paketverlust erscheint als rote Kerbe an der Stelle, an der er
        # auftrat - oben vor 24 h, unten jetzt. Bei erloschenen Standorten
        # keine Kerben: der letzte bekannte Verlauf waere dort veraltet und
        # wuerde Aktualitaet vortaeuschen.
        nicks, n_loss, max_loss = ("", 0, 0.0) if cls == "lost" else _loss_nicks(lat.get("series") or [])
        pulse = f'<i class="pulse" style="animation-delay:{i * 0.35:.2f}s"></i>' if cls == "ok" else ""
        drop = f'<span class="drop">{pulse}{nicks}</span>'

        # Zahl NEBEN das Kuerzel, nicht darunter: eine zusaetzliche Zeile macht
        # die Leiste 19px hoeher (gemessen), und die Hoehe fehlt unten direkt
        # den Charts.
        ms = lat.get("cur")
        zahl = f'<b class="lat num">{int(ms)}<i class="ms">ms</i></b>' if ms is not None else ""
        # Paketverlust als Prozentzahl - der HOECHSTWERT der letzten 24
        # Stunden, aber NUR wenn der Verlust nicht bloss ein einzelner
        # Ausreisser war (siehe _loss_zeigen). Ein einzelner
        # Fuenf-Minuten-Punkt von 151 ergab sonst '1 %', was sich wie ein
        # Dauerzustand liest - der UniFi Site Manager meldete fuer
        # dieselbe Konsole 'None'. Die rote Kerbe erscheint unabhaengig
        # davon: sie zeigt, DASS und WANN etwas war, ohne Dauer zu
        # behaupten. Steht als eigene Zeile mittig unter dem Knoten
        # (Nutzerwunsch).
        if max_loss and _loss_zeigen(lat):
            zahl += f'<b class="pl num">{max_loss:g}<i class="ms">%</i></b>'
        titel = f'{c["device"]}: {zustand}'
        if ms is not None:
            titel += f" · {int(ms)} ms"
        if n_loss:
            # Der Tooltip nennt ALLE Verlustereignisse, auch die, fuer die auf
            # der kurzen Leitung keine eigene Kerbe mehr Platz hatte.
            titel += (f" · Paketverlust in {n_loss} von {len(lat.get('series') or [])} "
                      f"Messpunkten (24 h), höchstens {max_loss:g} %")
        nodes.append(f'<div class="snode {cls}" title="{html.escape(titel)}">'
                     f'{drop}<span class="bulb"></span>'
                     f'<span class="name">{short}{zahl}</span></div>')
    return (f'<div class="bus" role="img" '
            f'aria-label="Systemschema: Site-Manager und {len(consoles)} Konsolen, '
            f'rote Kerben markieren Paketverlust der letzten 24 Stunden">'
            f'<div class="hub">SITE-MANAGER</div>'
            f'<div class="nodes">{"".join(nodes)}</div></div>')


def _update_event_log(state, consoles, now):
    """Haelt Statuswechsel je Konsole in state['events'] fest (rollierend,
    EVENT_LOG_KEEP) und merkt sich den zuletzt gesehenen Status in
    state['status'], damit beim naechsten Poll (neuer Prozess!) der Wechsel
    erkannt wird. Beim allerersten Lauf ohne gespeicherten Status gibt es
    EINEN Ausgangslage-Eintrag statt sechs Pseudo-Wechseln."""
    events = state.setdefault("events", [])
    prev = state.get("status")
    current = {c["device"]: _console_status(c) for c in consoles}

    def add(console, kind, msg):
        events.append({"ts": now.isoformat(), "console": console, "kind": kind, "msg": msg})

    if prev is None:
        n_nom = sum(1 for s in current.values() if s == "nominal")
        n_fo = sum(1 for s in current.values() if s == "failover")
        n_off = sum(1 for s in current.values() if s == "offline")
        add("MONITOR", "info",
            f"PROTOKOLL AKTIVIERT — AUSGANGSLAGE: {n_nom} NOMINAL, {n_fo} FAILOVER, {n_off} LINK LOST")
    else:
        for c in consoles:
            name, status, old = c["device"], current[c["device"]], prev.get(c["device"])
            if old == status:
                continue
            if status == "failover":
                add(name, "crit",
                    f"UPLOAD-Ø {human_kbps(c['last_rate_kbps'])} > "
                    f"{_console_failover_threshold(name):.0f} KBPS "
                    f"({FAILOVER_CONSECUTIVE} POLLS) — FAILOVER BESTÄTIGT")
            elif status == "offline":
                add(name, "warn", f"KEIN POLL SEIT > {OFFLINE_THRESHOLD_S} S — STATUS: LINK LOST")
            elif old == "failover":
                add(name, "ok", f"UPLOAD-Ø {human_kbps(c['last_rate_kbps'])} WIEDER UNTER SCHWELLE — FAILOVER BEENDET")
            elif old == "offline":
                add(name, "ok", "VERBINDUNG WIEDERHERGESTELLT — LINK NOMINAL")
            else:
                add(name, "ok", "NEU IM MONITOR — STATUS: NOMINAL")
    del events[:-EVENT_LOG_KEEP]
    state["status"] = current


def _event_log_html(events, now):
    """Die juengsten EVENT_LOG_SHOW Eintraege, neuester zuerst. Datum nur,
    wenn der Eintrag nicht von heute ist (spart Platz in der Zeitspalte)."""
    today = now.astimezone().date()
    items = []
    for ev in list(events)[-EVENT_LOG_SHOW:][::-1]:
        try:
            ts_local = datetime.fromisoformat(ev["ts"]).astimezone()
        except (KeyError, ValueError, TypeError):
            continue
        stamp = ts_local.strftime("%H:%M:%S") if ts_local.date() == today else ts_local.strftime("%d.%m. %H:%M")
        kind = ev.get("kind", "info")
        msg = html.escape(str(ev.get("msg", "")))
        # title: die Meldung wird einzeilig abgeschnitten (siehe .log li .msg),
        # per Mouseover bleibt der volle Text erreichbar.
        items.append(f'<li class="{html.escape(kind)}"><span class="t num">{stamp}</span>'
                     f'<span class="fn">{html.escape(str(ev.get("console", "")))}</span>'
                     f'<span class="msg" title="{msg}">{msg}</span></li>')
    if not items:
        items.append('<li class="info"><span class="t num">--:--:--</span><span class="fn">MONITOR</span>'
                     '<span class="msg">NOCH KEINE STATUSWECHSEL AUFGEZEICHNET</span></li>')
    return "\n      ".join(items)


# Zusatz-CSS der Uebersichtsseite: Leitstand-/HUD-Optik (aus der Stilstudie
# uebernommen, Nutzerwunsch). Baut auf BASE_CSS auf (Chart-Innereien,
# Tooltip, Footer) und ueberschreibt nur, was fuer das HUD anders ist.
# Die Chart-Zeichenflaeche (140px Hoehe, identische viewBox) sowie Raster-
# Abstand und Kachel-Innenabstand sind UNVERAENDERT gegenueber dem
# vorherigen Layout - ausdruecklicher Nutzerwunsch: maximale Hoehen/Laengen
# der Darstellung sollen gleich bleiben.
HUD_CSS = """
  :root { --hull-2: #171f26; --phosphor-dim: #2d6b64; --alert-dim: #7a2b25; }

  /* ---------------------------------------------------------------------
     Passt-auf-einen-Bildschirm-Layout (harte Vorgabe: KEIN Scrollbalken).
     Die Seite ist eine Flex-Spalte ueber die volle Viewport-Hoehe; alles
     ausser dem Kachelraster hat seine natuerliche Hoehe, das Raster bekommt
     den Rest und gibt Ueberschuss/Mangel an die Charts weiter (max 140px =
     unveraenderte Zeichenflaeche wie vorher, min 64px, damit auf niedrigen
     Fenstern die Seite schrumpft statt zu scrollen). Reine Hoehenverteilung
     per Flexbox - ohne JavaScript, ohne feste Pixelannahmen ueber die
     Bildschirmgroesse des Betrachters.
     --------------------------------------------------------------------- */
  body { font-family: "JetBrains Mono", ui-monospace, Consolas, monospace; font-size: 15px;
    /* Der Geraeterahmen (siehe .canopy::before) liegt ueber der Seite und
       verdeckt, was unter ihm sitzt - die Seite haelt deshalb genau seine
       Staerke plus etwas Luft Abstand. Diese Hoehe fehlt den Kacheln:
       gemessen 310 -> 304px je Kachel. */
    padding: 24px 30px 20px;
    background-image: repeating-linear-gradient(180deg, rgba(95,224,209,.025) 0px,
      rgba(95,224,209,.025) 1px, transparent 1px, transparent 3px); }
  .wrap { max-width: 1840px; width: 100%; position: relative; }
  /* EINE Glasflaeche unter allem statt neun einzelnen. Sie traegt
     Kopfbereich, Bank und Kachelraster gemeinsam - laesst man den Kopf aus,
     ist es gerade keine durchgehende Flaeche mehr. */
  .wrap::before {
    content: ""; position: absolute; inset: -12px -18px -10px -18px; z-index: 0;
    pointer-events: none;
    -webkit-backdrop-filter: blur(7px) saturate(1.5) brightness(1.12);
    backdrop-filter: blur(7px) saturate(1.5) brightness(1.12);
    background:
      linear-gradient(115deg, rgba(232,253,255,.055) 0%, transparent 38%, transparent 62%, rgba(198,240,255,.022) 100%),
      linear-gradient(158deg, rgba(140,205,220,.040), rgba(96,170,190,.010) 45%, rgba(120,195,210,.026));
    border-top: 1px solid rgba(216, 250, 255, .38);
  }
  header, .bank, .overview-grid, footer { position: relative; z-index: 1; }
  /* Nur bei dreispaltigem Raster (zwei Kachelreihen) auf Bildschirmhoehe
     einpassen. Bei zwei oder einer Spalte sind es drei bzw. sechs Reihen -
     die in die Fensterhoehe zu zwingen wuerde die Kacheln zu Streifen
     stauchen; dort ist Scrollen richtig. */
  @media (min-width: 901px) {
    html, body { height: 100%; }
    body { display: flex; flex-direction: column; }
    .wrap { flex: 1 1 auto; min-height: 0; display: flex; flex-direction: column; }
  }
  h1, .chip, .fn, .hub-lbl { font-family: "Rajdhani", "Segoe UI", sans-serif; letter-spacing: .04em; }
  .num { font-variant-numeric: tabular-nums; }

  /* Eckklammern: das wiederkehrende HUD-Motiv */
  .bracketed { position: relative; }
  .bracketed::before, .bracketed::after, .bracketed .bk-tr, .bracketed .bk-bl {
    content: ""; position: absolute; width: 14px; height: 14px;
    border: 2px solid var(--down); opacity: .75; pointer-events: none; }
  .bracketed::before { top: -1px; left: -1px; border-right: none; border-bottom: none; }
  .bracketed::after { bottom: -1px; right: -1px; border-left: none; border-top: none; }
  .bracketed .bk-tr { top: -1px; right: -1px; border-left: none; border-bottom: none; }
  .bracketed .bk-bl { bottom: -1px; left: -1px; border-right: none; border-top: none; }
  .bracketed.failover::before, .bracketed.failover::after,
  .bracketed.failover .bk-tr, .bracketed.failover .bk-bl {
    border-color: var(--alert); animation: bk-pulse 1.4s ease-in-out infinite; }
  @keyframes bk-pulse { 0%, 100% { opacity: .5; } 50% { opacity: 1; } }

  /* Kopfzeile */
  header { border-bottom: none; margin-bottom: 0; padding-bottom: 0; flex: 0 0 auto; }
  .boot-line { font-size: 11.5px; color: var(--phosphor-dim); letter-spacing: .12em;
    margin-bottom: 6px; display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
  .boot-line .dot { width: 6px; height: 6px; border-radius: 50%; background: var(--down);
    box-shadow: 0 0 6px var(--down); animation: blink 2s steps(1) infinite; margin: 0; }
  .boot-line .sync { margin-left: auto; display: flex; align-items: center; gap: 6px;
    color: var(--dim); cursor: help; }
  .sync-dot { width: 6px; height: 6px; border-radius: 50%; background: var(--down);
    box-shadow: 0 0 5px var(--phosphor-dim); flex: none; }
  .sync-dot.aging { background: var(--up); box-shadow: 0 0 5px var(--up); }
  .sync-dot.stale { background: var(--alert); box-shadow: 0 0 5px var(--alert); animation: blink 1s steps(1) infinite; }
  #refresh-cd { color: inherit; font-weight: 600; }
  .brand-logo { height: 32px; }
  h1 { font-size: 24px; font-weight: 700; margin: 0; text-transform: uppercase;
    letter-spacing: .04em; text-wrap: balance; }
  h1 .accent { color: var(--down); }
  .subhead { color: var(--dim); font-size: 12.5px; letter-spacing: .05em; }
  .subhead b { color: var(--text); font-weight: 600; }

  /* Statuspylon: die beiden Kennzahlen stehen OHNE Kasten auf dem Seiten-
     grund, damit sie nicht wie eine dritte Kachel neben Schema und Protokoll
     wirken. Sie sind Ableseinstrumente, keine Panele. */
  /* Der Pylon ist das einzige Element ohne eigene Kachel. Mit den blassen
     Label-Farben, die INNERHALB einer Platte funktionieren, ging er auf dem
     gemusterten Hintergrund unter. Statt ihm doch eine Kachel zu geben
     (Nutzerwunsch: soll frei stehen) bekommt er Eigenkontrast - und eine
     sehr weiche lokale Abdunklung, die den Untergrund unter ihm beruhigt,
     ohne eine Kante zu setzen. */
  .pylon { display: flex; gap: 16px; padding: 2px 0 0; min-width: 0; align-items: stretch;
    position: relative; }
  .pylon::before {
    content: ""; position: absolute; left: -22px; right: -18px; top: -14px; bottom: -14px;
    pointer-events: none; z-index: -1;
    background: radial-gradient(58% 62% at 42% 50%, rgba(4, 9, 13, .72), transparent 76%);
  }
  .pylon .werte { display: flex; flex-direction: column; justify-content: space-between;
    gap: 10px; min-width: 0; }
  .pylon .label { font-size: 10.5px; color: var(--text); opacity: .72;
    text-transform: uppercase; letter-spacing: .1em; font-weight: 500; }
  .pylon .label .lfoot { color: var(--down); opacity: .85; }
  .pylon .val { font-family: "Rajdhani", sans-serif; font-size: 38px; font-weight: 600;
    line-height: 1.02; color: var(--strong); }
  .pylon .val .unit { font-size: 15px; color: var(--text); opacity: .7; margin-left: 3px; }
  .pylon .val.alert { color: var(--alert); }
  .pylon .fuss { font-size: 10.5px; color: var(--down); opacity: .8; letter-spacing: .08em;
    text-transform: uppercase; margin-top: 2px; }

  .mlabel { font-size: 10px; color: var(--dim); letter-spacing: .1em; text-transform: uppercase; }
  .segments { display: flex; gap: 3px; height: 9px; }
  .segments i { flex: 1; background: var(--hull-2); border-top: 1px solid var(--line); }
  /* Latenz-Radar im Pylon (siehe _radar_html). Der senkrechte Monatsbalken,
     der hier vorher stand, ist dafuer gewichen: die Tagesangabe steht
     ohnehin im Label der Monatszahl. */
  /* Kontrast wie beim Pylon daneben: das Radar steht ebenfalls frei auf dem
     gemusterten Hintergrund, nicht auf einer Platte. Die Werte, mit denen es
     gebaut war, stammen aus dem Inneren einer Glaskachel und gingen hier
     unter - die Standortkuerzel (7,5px in --dim) waren praktisch nicht zu
     lesen. Eine eigene dunkle Scheibe im Radarfeld hebt den Untergrund ab,
     ohne dass das Radar zur Kachel wird. */
  .radar { position: relative; flex: 0 0 auto; width: 122px; height: 122px;
    border-radius: 50%; align-self: center; overflow: hidden;
    border: 1px solid rgba(127, 240, 228, .58);
    box-shadow: 0 0 16px rgba(127, 240, 228, .16), inset 0 0 22px rgba(4, 9, 13, .75);
    background:
      linear-gradient(0deg, transparent calc(50% - .5px), rgba(127,240,228,.32) calc(50% - .5px) calc(50% + .5px), transparent calc(50% + .5px)),
      linear-gradient(90deg, transparent calc(50% - .5px), rgba(127,240,228,.32) calc(50% - .5px) calc(50% + .5px), transparent calc(50% + .5px)),
      radial-gradient(circle at 50% 50%, rgba(127,240,228,.13), transparent 74%),
      radial-gradient(circle at 50% 50%, rgba(4, 9, 13, .82), rgba(4, 9, 13, .62) 78%); }
  .radar .ring { position: absolute; left: 50%; top: 50%; transform: translate(-50%, -50%);
    border: 1px solid rgba(127,240,228,.36); border-radius: 50%; pointer-events: none; }
  .radar .sweep { position: absolute; inset: 0; border-radius: 50%; pointer-events: none;
    background: conic-gradient(from 0deg, rgba(127,240,228,.62) 0deg, rgba(127,240,228,.20) 28deg, transparent 60deg, transparent 360deg);
    animation: radar-sweep __RADAR__s linear infinite; }
  /* GEGEN den Uhrzeigersinn: der helle Keil des Suchstrahls liegt bei 0-60
     Grad, laeuft also dem verblassenden Schweif hinterher. Bei Drehung im
     Uhrzeigersinn kaeme dadurch der Schweif zuerst und der Strahl danach -
     genau verkehrt. Andersherum laeuft der Strahl voran und zieht den
     Schweif hinter sich her, wie es sein soll. */
  @keyframes radar-sweep { to { transform: rotate(-360deg); } }
  .radar .blip { position: absolute; width: 6px; height: 6px; margin: -3px 0 0 -3px;
    border-radius: 50%; background: var(--strong);
    box-shadow: 0 0 4px var(--down), 0 0 11px var(--down); }
  .radar .blip.alert { background: #fff;
    box-shadow: 0 0 4px var(--alert), 0 0 13px var(--alert); }
  .radar .blip.lost { background: var(--dim); box-shadow: none; }
  /* Die Kuerzel sind der eigentliche Grund, warum das Radar schwer zu lesen
     war: heller, eine Spur groesser und mit dunklem Saum gegen das Muster. */
  .radar .blip b { position: absolute; left: 8px; top: -6px; font-size: 8.5px; font-weight: 600;
    letter-spacing: .06em; color: var(--text);
    text-shadow: 0 0 7px rgba(4, 9, 13, .95), 0 1px 3px rgba(4, 9, 13, .95); }
  .radar .blip.alert b { color: var(--alert); }
  .radar .rlabel { position: absolute; right: 5px; bottom: 4px; font-size: 8.5px;
    letter-spacing: .06em; color: var(--down); opacity: .85; font-weight: 500;
    text-shadow: 0 0 7px rgba(4, 9, 13, .95); }
  .segments i.lit { background: var(--down); box-shadow: 0 0 5px var(--phosphor-dim); border-top-color: var(--down); }
  .segments i.lit.warn { background: var(--warn); box-shadow: 0 0 4px var(--warn); border-top-color: var(--warn); }
  .segments i.lit.crit { background: var(--alert); box-shadow: 0 0 4px var(--alert); border-top-color: var(--alert); }

  /* Konsolenbank: Pylon, Schema und Protokoll in EINER Zeile. Vorher waren
     das drei gestapelte Baender mit drei verschiedenen Schnitten - keine
     senkrechte Kante ging durch. Als ein Band gibt es keine zweite Zeile
     mehr, an der etwas nicht fluchten koennte, und der Kopfbereich schrumpft
     von 365 auf 249px (gemessen bei 1600x950); die gewonnene Hoehe geht an
     die Konsolenkacheln, wo die Flow-Kurven sitzen. */
  .bank { display: grid; grid-template-columns: minmax(175px, .62fr) minmax(0, 1.45fr) minmax(0, 1.6fr);
    gap: 20px; margin-top: 14px; flex: 0 0 auto; }
  @media (max-width: 900px) {
    .bank { grid-template-columns: 1fr; }
    /* Senkrecht ergibt der Balken nur neben zwei uebereinanderstehenden
       Zahlen Sinn; einspaltig stehen sie nebeneinander. */
    .pylon { display: block; }
    .radar { display: none; }
    .pylon .werte { flex-direction: row; justify-content: flex-start; gap: 34px; }
  }

  /* Systemschema (HTML/CSS statt SVG, siehe _schema_html) */
  .schema { background: var(--panel); border: 1px solid var(--line); padding: 10px 16px 12px; }
  .schema .mlabel { margin-bottom: 8px; }
  .bus { display: flex; align-items: flex-start; padding: 2px 0 0; }
  /* --hub-h/2: die Trunk-Linie dockt genau an der Mittelachse des Hubs an. */
  .bus { --hub-h: 25px; --drop-h: 20px; }
  .bus .hub { flex: 0 0 auto; font-family: "Rajdhani", "Segoe UI", sans-serif;
    font-size: 11px; font-weight: 600; letter-spacing: .08em; line-height: 1;
    color: var(--text); background: var(--hull-2); border: 1px solid var(--down);
    padding: 8px 12px; white-space: nowrap; }
  .bus .nodes { flex: 1 1 auto; min-width: 0; display: flex;
    position: relative; padding-top: calc(var(--hub-h) / 2); }
  .bus .nodes::before { content: ""; position: absolute; left: 0; right: 0;
    top: calc(var(--hub-h) / 2); border-top: 2px solid var(--line); }
  .snode { flex: 1 1 0; min-width: 0; display: flex; flex-direction: column; align-items: center; }
  /* Die Leitung ist ein eigenes, mittig ausgerichtetes Element - NICHT der
     linke Rahmen des Kastens. Grund: absolut positionierte Kinder richten
     sich nach der INNENkante, also um die Rahmenbreite verschoben. Mit der
     frueheren Rahmen-Loesung lief der Tropfen dadurch 0,5px und die
     Verlustkerbe 1,0px neben der Linie her (nachgemessen an allen sechs
     Knoten). Jetzt teilen sich Linie, Tropfen und Kerbe dieselbe Mitte bei
     left:50%, unabhaengig von Strichstaerken. */
  .snode .drop { position: relative; width: 9px; height: var(--drop-h); border: none; }
  .snode .drop::before { content: ""; position: absolute; left: 50%; margin-left: -1px;
    top: 0; bottom: 0; width: 2px; background: var(--phosphor-dim); }
  .snode.alert .drop::before { background: var(--alert); }
  /* Offline war bisher die schwaechste der drei Darstellungen: gestrichelte
     Leitung und fehlendes Leuchten, aber die Beschriftung blieb exakt wie bei
     Nominal. Aus ein paar Metern Entfernung - und genau dafuer ist das Bild
     da - war "Standort tot" damit kaum von "Standort in Ordnung" zu
     unterscheiden, obwohl es inhaltlich nicht harmloser ist als ein Failover.
     Deshalb jetzt zusaetzlich ein durchgestrichener, leerer Knoten: das Kreuz
     ist ein reines FORM-Signal und traegt auch dann, wenn die Farbe nicht
     ankommt (Entfernung, Farbsehschwaeche). Bewusst grau statt rot -
     ausgefallen ist nicht dasselbe wie Alarm. */
  .snode.lost .drop { opacity: .55; }
  .snode.lost .drop::before { background: repeating-linear-gradient(180deg,
    var(--dim) 0 3px, transparent 3px 6px); }
  .snode .pulse { position: absolute; left: 50%; margin-left: -2.5px; top: 0; width: 5px; height: 5px;
    border-radius: 50%; background: var(--down); opacity: 0; }
  .snode .bulb { width: 15px; height: 15px; border-radius: 50%; border: 1.5px solid var(--down);
    background: var(--ink); box-shadow: 0 0 6px var(--phosphor-dim); }
  .snode.alert .bulb { border-color: var(--alert); background: var(--alert-dim); box-shadow: 0 0 7px var(--alert); }
  .snode.lost .bulb { border-color: var(--dim); background: none; box-shadow: none; position: relative; }
  .snode.lost .bulb::before, .snode.lost .bulb::after {
    content: ""; position: absolute; left: 50%; top: 50%; width: 12px; height: 1px;
    background: var(--dim); }
  .snode.lost .bulb::before { transform: translate(-50%, -50%) rotate(45deg); }
  .snode.lost .bulb::after { transform: translate(-50%, -50%) rotate(-45deg); }
  /* Paketverlust als rote Kerbe quer ueber die gerade Abzweigleitung (siehe
     _loss_nicks). Die Leitung bleibt gerade und behaelt ihre alte Hoehe - die
     frueher hier stehende Latenz-Schlangenlinie ist raus. */
  /* left:-4px bei 9px Breite setzt die Kerbenmitte auf x=+0.5 - genau die
     Mitte der 1px starken Leitung, die bei x=0..1 liegt. */
  .snode .nick { position: absolute; left: 50%; margin-left: -4.5px; width: 9px; height: 2px;
    background: var(--alert); box-shadow: 0 0 5px var(--alert); opacity: .95;
    border-radius: 0; margin-top: -1px; }
  .snode .name { font-size: 11px; color: var(--dim); margin-top: 5px; letter-spacing: .06em;
    white-space: nowrap; }
  .snode .lat { font-weight: 500; color: var(--text); margin-left: 6px; letter-spacing: .02em; }
  .snode .lat .ms { font-size: 8.5px; font-style: normal; color: var(--dim); margin-left: 1px; }
  /* Die Prozentzahl steht UNTER der Latenz, nicht daneben (so passt es am
     Monitor besser, und nebeneinander ueberlappten sich die Beschriftungen
     benachbarter Knoten: gemessen bei 1060px Breite bis -24px im
     schlimmsten Fall, dreistellige Latenz plus zweistelliger Verlust an
     allen sechs Knoten). Die zweite Zeile entsteht nur, wenn es wirklich
     Verlust gab - und der gezeigte Wert ist das 24-Stunden-Maximum, aendert
     sich also im Stundentakt und nicht bei jedem Poll. Ein Hoehensprung im
     Minutentakt ist dadurch ausgeschlossen. */
  /* text-align:center hebt das rechtsbuendige .num auf. Ohne das klebte die
     Prozentzahl am rechten Rand der Beschriftung, stand also unter der
     ms-Zahl statt unter dem Knoten. Da die Beschriftung selbst mittig unter
     dem Knoten sitzt (align-items:center), liegt die zentrierte Zahl damit
     genau unter der Bubble. */
  .snode .pl { display: block; font-weight: 600; color: var(--alert);
    margin-left: 0; letter-spacing: .02em; line-height: 1.35; text-align: center; }
  .snode .pl .ms { font-size: 8.5px; font-style: normal; color: var(--alert); opacity: .75; margin-left: 1px; }
  /* Die Knotenbeschriftung bricht nicht um (sonst waere die Leiste hoeher),
     kann bei schmalen Fenstern aber ueber ihre Spalte hinauslaufen und die
     Nachbarn ueberlappen. Gemessen bei 950px Breite: nur Kuerzel +27px Luft,
     mit Latenz schon -3px, mit Latenz UND Prozentzahl bis -20px. Deshalb
     zwei Stufen. In der zweiten fallen die Einheiten weg - die Bedeutung
     traegt dort die Farbe (teal = ms, rot = %), und der volle Text steht
     ohnehin im Tooltip. */
     Bei schmalen Fenstern zusaetzlich verkleinert - sechs Kuerzel plus
     Messwerte brauchen dort mehr Platz, als die Spalte hergibt. */
  @media (max-width: 1250px) {
    .snode .name { font-size: 10px; }
    .snode .lat { margin-left: 4px; }
  }
  /* Noch schmaler: die Einheit "ms" faellt weg. Sie ist die entbehrlichere
     der beiden - die Latenz steht direkt beim Kuerzel und ist teal, waehrend
     die rote Prozentzahl ihre Einheit behaelt, weil sie seltener auftritt
     und sonst als blosse Zahl missverstaendlich waere. Voller Text im
     Tooltip. */
  @media (max-width: 1050px) {
    .snode .name { font-size: 9px; }
    .snode .lat .ms { display: none; }
  }
  .snode.lost .lat { opacity: .5; }
  .snode.alert .name { color: var(--alert); }
  /* Zurueckgenommen statt nur andersfarbig: der ausgefallene Standort soll
     sichtbar aus der Reihe fallen, nicht um Aufmerksamkeit mit dem Failover
     konkurrieren. */
  .snode.lost .name { opacity: .55; }
  @media (prefers-reduced-motion: no-preference) {
    .snode.ok .pulse { animation: pulse-travel __PULSE__s linear infinite; } }
  @keyframes pulse-travel {
    0% { transform: translateY(0); opacity: 0; }
    12% { opacity: 1; }
    88% { opacity: 1; }
    100% { transform: translateY(var(--drop-h)); opacity: 0; } }

  /* Konsolen-Panels: Raster-Abstand wie zuvor (20px). Das Raster bekommt die
     Resthoehe und gibt sie an die Charts weiter (siehe .mini-chart-col). */
  /* minmax(0, 1fr) statt 1fr, und min-width:0 an der Kachel: ein "1fr"-Track
     darf sonst nicht unter seinen min-content schrumpfen. Da Statuszeile und
     Protokollmeldung bewusst NICHT umbrechen (white-space:nowrap, siehe
     unten), setzt deren volle Textbreite sonst die Mindestbreite der Kachel -
     die Spalten werden breiter als der Container und die Seite scrollt
     seitlich. Mit minmax(0,...) darf der Track schrumpfen und die
     text-overflow-Ellipse greift wie vorgesehen. */
  .overview-grid { display: grid; gap: 20px; grid-template-columns: repeat(3, minmax(0, 1fr));
    grid-auto-rows: 1fr; margin-top: 16px; }
  @media (min-width: 901px) { .overview-grid { flex: 1 1 auto; min-height: 0; } }
  @media (max-width: 900px) { .overview-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); grid-auto-rows: auto; } }
  @media (max-width: 600px) { .overview-grid { grid-template-columns: minmax(0, 1fr); } }
  .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 0;
    clip-path: polygon(0 0, calc(100% - 18px) 0, 100% 18px, 100% 100%, 18px 100%, 0 calc(100% - 18px));
    padding: 14px 18px 12px; display: flex; flex-direction: column; gap: 8px;
    min-height: 0; min-width: 0; }
  .panel.failover { border-color: var(--alert); background: linear-gradient(180deg, var(--panel) 0%, var(--failover-bg) 100%); }
  .panel.offline { opacity: .6; }
  .panel.offline .panel-head .fn { color: var(--dim); }
  .panel-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
  .panel-head .fn { font-size: 17px; font-weight: 600; color: var(--text); margin: 0; }
  .panel-head .idx { color: var(--phosphor-dim); font-size: 12px; margin-right: 6px; }
  .chip { font-size: 10.5px; text-transform: uppercase; letter-spacing: .09em; font-weight: 600;
    padding: 2px 8px; border: 1px solid currentColor; white-space: nowrap; }
  .chip.nominal { color: var(--down); }
  .chip.failover { color: var(--alert); background: rgba(255,90,77,.12); }
  .chip.lost { color: var(--dim); }
  .panel .subhead.alert { color: var(--alert); }
  /* Einzeilig halten: bricht die Statuszeile um, wird die ganze Kachelreihe
     hoeher und die Hoehe fehlt den Charts. Voller Text im title-Attribut. */
  .panel .subhead { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  /* Seit nur noch der Monatswert hier steht (30 Tage und Gesamt sind
     entfallen), braucht es keine Spaltenaufteilung mehr - der Wert nimmt
     seine natuerliche Breite. */
  /* Label und Wert nebeneinander statt uebereinander: die eingesparte
     Zeile geht direkt an den Flow-Chart darunter (gemessen 14px je Kachel). */
  .readouts { display: flex; gap: 18px; }
  .readouts .r { min-width: 0; display: flex; align-items: baseline; gap: 8px; }
  .readouts .rl { font-size: 10px; color: var(--dim); text-transform: uppercase; letter-spacing: .09em; }
  /* Einzeilig halten wie Statuszeile und Protokoll: bricht ein Messwert um
     (typisch "11.5GB / 9.0 GB" bei schmalen Kacheln), wird die ganze
     Kachelreihe 25px hoeher und die Hoehe fehlt direkt den Charts - gemessen
     bei 1100px Breite: 5 von 18 Werten umbrochen, Charts dadurch auf dem
     Minimum von 48px statt 140px. Abgeschnitten wird von hinten, also
     zuerst der Schwellwert-Zusatz; die eigentliche Zahl bleibt stehen.
     Zusammen mit min-width:0 unten und minmax(0,1fr) am Raster, sonst
     wuerde das nowrap die Spalten aufblaehen. */
  .readouts .rv { font-size: 16px; font-weight: 600; color: var(--text);
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .readouts .rv .unit { font-size: 11px; }
  /* Ohne diese zwei Regeln verlieren die Schwellwert-Farben: '.value-alert'
     (0,1,0) unterliegt '.readouts .rv' (0,2,0) und der Monatswert bliebe
     immer normal eingefaerbt. */
  .readouts .rv.value-alert { color: var(--alert); }
  .readouts .rv.value-warn { color: var(--warn); }
  .readouts .threshold-ref { font-size: 10.5px; }
  .mini-meter .segments { height: 6px; }
  /* Eine Spalte: der Stundenchart ist entfallen, der Flow-Chart hat seine
     Breite bekommen. Er zeigt dieselben Messpunkte einzeln, die der
     Stundenchart nur zu 24 Balken summiert hat - seit die Ausduennung die
     Spitzen behaelt (siehe _thin_keeping_peaks) geht dabei nichts mehr
     verloren. */
  .mini-charts { display: grid; grid-template-columns: minmax(0, 1fr); gap: 14px;
    flex: 1 1 auto; min-height: 0; }
  .mini-chart-col { display: flex; flex-direction: column; min-height: 0; }
  /* Die Zeichenflaeche nimmt die volle Resthoehe der Kachel. Frueher stand
     hier ein Deckel von 140px - seit der Kopfbereich zur Konsolenbank
     geschrumpft ist, wurden die Kacheln hoeher, der Chart aber nicht: unter
     der Kurve stand einfach leerer Raum. Kein max-height mehr; nach unten
     bleibt min-height als Schutz, damit auf flachen Fenstern nicht die Seite
     in einen Scrollbalken laeuft. */
  .mini-chart-col .chart { flex: 1 1 auto; height: auto; min-height: 64px; }
  /* Legende sitzt in der Chart-Beschriftungszeile statt in einer eigenen
     Zeile darunter - eine Zeile weniger je Kachel spart ueber zwei
     Kachelreihen rund 50px Seitenhoehe. */
  .mini-chart-label { color: var(--dim); font-size: 9.5px; text-transform: uppercase;
    letter-spacing: .09em; margin-bottom: 2px; flex: 0 0 auto;
    display: flex; justify-content: space-between; align-items: center; gap: 8px;
    white-space: nowrap; overflow: hidden; }
  .mini-chart-label .lg { color: var(--dim); letter-spacing: .05em; }
  .mini-chart-label .dot { width: 7px; height: 7px; border-radius: 0; margin: 0 3px 0 5px; }
  .chart .flow-down-line { filter: drop-shadow(0 0 2px var(--phosphor-dim)); }

  /* Ereignisprotokoll - sitzt neben dem Schema (siehe .bank) */
  .log { background: var(--panel); border: 1px solid var(--line); padding: 0 0 2px;
    display: flex; flex-direction: column; min-height: 0; }
  .log .log-head { display: flex; justify-content: space-between; align-items: center;
    padding: 7px 16px; border-bottom: 1px solid var(--line); flex: 0 0 auto;
    font-size: 10px; color: var(--dim); text-transform: uppercase; letter-spacing: .1em; }
  .log ol { list-style: none; margin: 0; padding: 4px 0; overflow-y: auto; flex: 1 1 auto; min-height: 0; }
  .log li { display: grid; grid-template-columns: 78px 105px minmax(0, 1fr); gap: 10px;
    padding: 3px 16px; font-size: 11.5px; color: var(--dim); align-items: baseline; }
  .log li .t { color: var(--phosphor-dim); }
  .log li .fn { color: var(--text); font-weight: 600; }
  /* Einzeilig abschneiden statt umbrechen: eine umbrechende Meldung macht den
     ganzen Deck-Block hoeher, und die Hoehe fehlt dann direkt den Charts
     (gemessen: 180px statt 133px bei vier Eintraegen). Vollstaendiger Text
     haengt im title-Attribut, siehe _event_log_html(). */
  .log li .msg { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .log li.crit .msg { color: var(--alert); }
  .log li.warn .msg { color: var(--up); }
  .log li.ok .msg { color: var(--down); }
  @media (max-width: 640px) { .log li { grid-template-columns: 80px 90px minmax(0, 1fr); font-size: 11px; } }

  footer { margin-top: 12px; padding-top: 8px; font-size: 10.5px; letter-spacing: .04em;
    line-height: 1.5; flex: 0 0 auto; }

  /* Niedrige Fenster (kleine Notebooks, Browser mit vielen Leisten): die
     sechs Konsolenkacheln haben Vorrang, also wird zuerst die Bank gestaucht.
     Ziel bleibt: kein Scrollbalken. */
  @media (max-height: 820px) {
    /* Nicht unter die Rahmenstaerke (20px) gehen: der Rahmen liegt ueber
       der Seite und verdeckt sonst Inhalt, und die Traegerflaeche ragt
       seitlich 18px ueber .wrap hinaus - zu wenig Polster erzeugt einen
       Querlauf. */
    body { padding: 24px 26px 20px; }
    .bank { max-height: 104px; margin-top: 10px; }
    .pylon .val { font-size: 25px; }
    .pylon .fuss { display: none; }
    .overview-grid { margin-top: 10px; gap: 14px; }
    .panel { padding: 10px 14px 9px; gap: 6px; }
    .mini-chart-col .chart { min-height: 48px; }
    footer { margin-top: 8px; padding-top: 6px; }
  }
  /* Noch niedriger: Schema und Protokoll weichen ganz. Beides ist eine
     Zusammenfassung dessen, was die Kacheln darunter ohnehin zeigen - lieber
     weglassen als die Kacheln in einen Scrollbalken draengen. Der Pylon
     BLEIBT stehen: seine beiden Zahlen stehen sonst nirgends auf der Seite. */
  @media (max-height: 700px) {
    .bank { grid-template-columns: 1fr; max-height: none; }
    .bank .schema, .bank .log { display: none; }
    .pylon { display: block; }
    .radar { display: none; }
    .pylon .werte { flex-direction: row; justify-content: flex-start; gap: 34px; }
    .mini-chart-col .chart { min-height: 40px; }
    footer { font-size: 10px; }
  }

  /* Schmale Bildschirme scrollen ohnehin (siehe min-width:901px oben), also
     hier die volle Zeichenflaeche und alle Bausteine zurueckholen. MUSS nach
     den beiden Hoehen-Stufen stehen: gleiche Spezifitaet, spaeter gewinnt -
     sonst wuerde auf einem hochkant gehaltenen Telefon (Fensterhoehe < 820px)
     faelschlich die Stauch-Variante fuer flache Fenster greifen. */
  @media (max-width: 900px) {
    body { padding: 24px 26px 20px; }
    .bank { max-height: none; }
    .bank .schema, .bank .log { display: block; }
    .pylon .val { font-size: 33px; }
    .pylon .fuss { display: block; }
    .overview-grid { margin-top: 16px; gap: 20px; }
    .panel { padding: 14px 18px 12px; gap: 8px; }
    .mini-chart-col .chart { height: 140px; min-height: 0; flex: 0 0 auto; }
  }
  @keyframes blink { 50% { opacity: .15; } }
  @media (prefers-reduced-motion: reduce) {
    .boot-line .dot, .sync-dot.stale, .bracketed.failover::before, .bracketed.failover::after,
    .bracketed.failover .bk-tr, .bracketed.failover .bk-bl { animation: none; } }
""".replace("__RADAR__", str(RADAR_SWEEP_S)).replace("__PULSE__", str(PULSE_TRAVEL_S))


# Glasprojektion - setzt auf HUD_CSS auf und wird NACH ihm eingebunden (gleiche
# Spezifitaet, spaeter gewinnt). Nur aktiv, solange COLOR_THEME == "glas";
# beim Zurueckschalten auf "hud"/"default" faellt dieser Block komplett weg,
# ohne dass am uebrigen Stylesheet etwas zu aendern waere.
#
# Die drei Dinge, die den Glas-Eindruck ueberhaupt erst tragen:
#   1. .depth  - eine Ebene HINTER dem Inhalt. Durchsichtig vor Reinschwarz
#                sieht aus wie schwarz; erst wenn dahinter etwas liegt, wird
#                Transparenz sichtbar.
#   2. .plate  - die Kacheln mattieren, was hinter ihnen liegt
#                (backdrop-filter), haben eine von oben angeleuchtete Kante
#                und lassen Streulicht darunter austreten.
#   3. .canopy - die Scheibe selbst: EINE bildschirmfeste Ebene ueber allem,
#                mit dem wandernden Lichtstreifen, Staub und Randabfall. Ein
#                Projektor, eine Scheibe - deshalb ausdruecklich nicht je
#                Kachel (Nutzerwunsch).
# Umlaufdauer des Lichtstreifens. Steht an EINER Stelle, weil CSS-Animation
# und das Phasen-Skript in CANOPY_HTML denselben Wert brauchen - liefen sie
# auseinander, waere der Streifen nach jedem Reload an der falschen Stelle.
CANOPY_SWEEP_S = 82
# Die zweite Schwade laeuft bewusst nicht im selben Takt: sonst wiederholt
# sich das Bild periodisch und wird als Muster erkennbar. 82 zu 112 ist kein
# glattes Verhaeltnis, die beiden treffen sich erst nach gut zwei Stunden
# wieder in derselben Stellung.
CANOPY_SWEEP_THIN_S = 112

GLASS_CSS = """
  :root {
    /* Tiefenebene: durchgehend kalt. Die beiden Leuchten sind bewusst
       nahezu weisses Blau mit wenig Saettigung - sie lesen sich als LICHT,
       nicht als Farbe, und lassen damit die drei bedeutungstragenden Farben
       des Dashboards (Teal = Download, Amber = Upload, Rot = Failover)
       unangetastet. Eine gesaettigte Leuchte wuerde die Platten toenen und
       Teal/Amber dahinter mitverschieben. */
    --nebula: #1b3147; --hull-far: #16283a;
    --lamp-a: rgba(198, 230, 255, .24); --lamp-b: rgba(150, 200, 255, .20);
    --glass: rgba(120, 195, 210, .085); --glass-2: rgba(96, 170, 190, .03);
    --glass-edge: rgba(198, 240, 255, .20);
    --phosphor-dim: #2a7d75; --hull-2: rgba(120, 195, 210, .10); --alert-dim: rgba(255, 106, 88, .18);
  }

  /* Scanlinien des HUD-Themes weichen der Tiefenebene */
  body { background-image: none; }

  /* Bewusst OHNE die frueheren Rumpfstreben (Nutzerwunsch). Sie waren das
     Einzige mit harten Kanten und damit das, woran die Mattierung am besten
     ablesbar war - den Glaseindruck tragen jetzt Kantenbevel, Reflexion und
     Frost-Koernung der Platten selbst. */
  /* Zwei Wabenebenen hintereinander: eine feine, naehere und eine grosse,
     sehr blasse, gegeneinander versetzt. Tiefe entsteht dadurch, dass das
     Auge zwei Entfernungen liest - nicht dadurch, dass mehr Ornament da
     waere. 64 zu 148px ist bewusst kein glattes Vielfaches, sonst entsteht
     ein Moire.

     Das Muster liegt HIER und nicht in den Kacheln: nur so laeuft es unter
     den Platten weiter, statt an ihrer Kante abzureissen - und nur so hat
     der backdrop-filter der Platten ueberhaupt etwas zum Verschleifen. Im
     Spalt zwischen zwei Kacheln steht es scharf, darunter weich. Genau
     dieser Kontrast macht aus einer dunklen Flaeche eine Scheibe. */
  .depth {
    position: fixed; inset: 0; z-index: 0; pointer-events: none;
    background:
      url(\"data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='64' height='55' viewBox='0 0 56 48'><path d='M14 0 L28 8 L28 24 L14 32 L0 24 L0 8 Z M42 0 L56 8 L56 24 L42 32 L28 24 L28 8 Z M14 32 L28 40 L28 48 M42 32 L28 40' fill='none' stroke='%237ff0e4' stroke-width='0.8' stroke-opacity='0.28'/></svg>\"),
      url(\"data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='148' height='127' viewBox='0 0 56 48'><path d='M14 0 L28 8 L28 24 L14 32 L0 24 L0 8 Z M42 0 L56 8 L56 24 L42 32 L28 24 L28 8 Z M14 32 L28 40 L28 48 M42 32 L28 40' fill='none' stroke='%237ff0e4' stroke-width='0.6' stroke-opacity='0.12'/></svg>\"),
      radial-gradient(18% 22% at 73% 16%, var(--lamp-a) 0%, transparent 72%),
      radial-gradient(15% 19% at 26% 84%, var(--lamp-b) 0%, transparent 72%),
      radial-gradient(62% 52% at 14% 8%, var(--nebula) 0%, transparent 62%),
      radial-gradient(52% 48% at 92% 88%, var(--hull-far) 0%, transparent 58%),
      var(--ink);
    background-size: 64px 55px, 148px 127px, auto, auto, auto, auto, auto;
    background-position: 0 0, 18px 12px, 0 0, 0 0, 0 0, 0 0, 0 0;
  }
  .wrap { position: relative; z-index: 1; }

  /* Die Scheibe. z-index ueber allem, pointer-events:none - sie faengt keine
     Klicks ab und stoert die Chart-Tooltips nicht. */
  .canopy { position: fixed; inset: 0; z-index: 50; pointer-events: none; overflow: hidden; }
  /* Scanlinien und Lichtkegel: das Bild kommt sichtbar von einem Projektor
     statt aus dem Bildschirm. Bewusst ENG (3px) und schwach - je feiner das
     Raster, desto weniger liest es sich als Streifen und desto mehr als
     Materialstruktur. Ein groberer Abstand kippt sofort ins Retro-Roehren-
     hafte. Die Linien binden Kacheln und Hintergrund zu EINER
     Projektionsflaeche zusammen; ohne sie wirken die Platten aufgelegt. */
  .canopy::after {
    content: ""; position: fixed; inset: 0; pointer-events: none; z-index: 3;
    background-image:
      repeating-linear-gradient(0deg, rgba(127,240,228,.020) 0 1px, transparent 1px 3px),
      radial-gradient(70% 55% at 50% -12%, rgba(127, 240, 228, .09), transparent 72%);
  }
  /* Nebelschwaden statt Lichtstreifen. Vorher zwei schmale Baender mit
     sauberen Verlaufskanten - das las sich als Scheinwerfer, der ueber die
     Scheibe faehrt. Jetzt breite, weiche Wolken, die mit einem Rauschmuster
     MASKIERT sind: dadurch fransen sie aus und sind stellenweise dichter,
     statt ein gleichmaessig runder Fleck zu sein. Genau daran erkennt das
     Auge Dunst statt Licht. Dasselbe feTurbulence-Rauschen benutzt die Seite
     ohnehin schon als Frost-Koernung im Glas. */
  .canopy .band {
    position: absolute; top: -40%; left: -80%; width: 120%; height: 180%;
    transform: rotate(9deg) translateX(-50%);
    background: radial-gradient(60% 50% at 50% 50%, rgba(214, 245, 255, .16) 0%,
      rgba(198, 240, 255, .08) 44%, transparent 76%);
    -webkit-mask-image: url(\"data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='1400' height='900'><filter id='f'><feTurbulence type='fractalNoise' baseFrequency='0.010' numOctaves='4' seed='7'/></filter><rect width='100%25' height='100%25' filter='url(%23f)' opacity='0.62'/></svg>\");
    mask-image: url(\"data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='1400' height='900'><filter id='f'><feTurbulence type='fractalNoise' baseFrequency='0.010' numOctaves='4' seed='7'/></filter><rect width='100%25' height='100%25' filter='url(%23f)' opacity='0.62'/></svg>\");
    -webkit-mask-size: 1400px 900px; mask-size: 1400px 900px;
    filter: blur(14px);
  }
  /* Zweite Schwade, schmaler und feiner gerauscht: eine Kanzel hat mehrere
     Scheiben, der Dunst dahinter ist nicht eine einzige Schicht. */
  .canopy .band.thin {
    width: 60%; transform: rotate(9deg) translateX(-180%);
    background: radial-gradient(60% 50% at 50% 50%, rgba(214, 245, 255, .10) 0%, transparent 74%);
    -webkit-mask-image: url(\"data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='1400' height='900'><filter id='f'><feTurbulence type='fractalNoise' baseFrequency='0.017' numOctaves='3' seed='7'/></filter><rect width='100%25' height='100%25' filter='url(%23f)' opacity='0.55'/></svg>\");
    mask-image: url(\"data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='1400' height='900'><filter id='f'><feTurbulence type='fractalNoise' baseFrequency='0.017' numOctaves='3' seed='7'/></filter><rect width='100%25' height='100%25' filter='url(%23f)' opacity='0.55'/></svg>\");
    -webkit-mask-size: 900px 700px; mask-size: 900px 700px;
    filter: blur(10px);
  }
  .canopy .grime {
    position: absolute; inset: 0;
    background:
      radial-gradient(90px 26px at 18% 24%, rgba(198, 240, 255, .035), transparent 72%),
      radial-gradient(140px 40px at 63% 68%, rgba(198, 240, 255, .028), transparent 72%),
      radial-gradient(60px 20px at 88% 18%, rgba(198, 240, 255, .032), transparent 72%),
      radial-gradient(110px 30px at 40% 88%, rgba(198, 240, 255, .022), transparent 72%);
  }
  /* Randabfall des Projektionsfelds - gehoert auf die Scheibe, nicht auf die
     einzelne Kachel. */
  .canopy .vignette {
    position: absolute; inset: 0;
    background: radial-gradient(82% 74% at 50% 44%, transparent 52%, rgba(4, 7, 10, .58) 100%);
  }
  @media (prefers-reduced-motion: no-preference) {
    /* Deutlich langsamer als die frueheren 26s: Nebel zieht, er fegt nicht.
       Die beiden Schwaden laufen unterschiedlich schnell, damit sich das
       Bild nicht periodisch wiederholt. */
    .canopy .band { animation: sweep __SWEEP__s linear infinite; }
    .canopy .band.thin { animation: sweep-thin __SWEEP_THIN__s linear infinite; }
  }
  @keyframes sweep {
    from { transform: rotate(9deg) translateX(-60%); }
    to   { transform: rotate(9deg) translateX(180%); } }
  @keyframes sweep-thin {
    from { transform: rotate(9deg) translateX(-190%); }
    to   { transform: rotate(9deg) translateX(400%); } }

  /* ---- Glasplatten: Telemetriezellen, Schema, Protokoll, Kacheln ----
     Drei Dinge machen aus der getoenten Flaeche eine Scheibe mit Dicke:
       1. Kantenbevel - innen oben/links Licht, unten/rechts Schatten. Das
          ist es, was Dicke suggeriert; ohne den bleibt es Toenung.
       2. Reflexion quer ueber die Platte, wie ein Fenster, das den Himmel
          spiegelt.
       3. Frost-Koernung als feTurbulence-Rauschen direkt im Glas (inline
          als data-URI, kein externer Abruf - der waere von der CSP der
          Seite ohnehin geblockt).
     Die Mattierung selbst (backdrop-filter) traegt weniger als man denkt,
     seit die Streben im Hintergrund weg sind: sie braucht harte Kanten zum
     Verschleifen, und die gibt es dahinter nicht mehr. */
  /* Deutlich weniger Deckkraft als frueher und ein schwaecherer Blur: die
     Platte soll das Muster dahinter DURCHLASSEN, nicht verdecken. Mit den
     alten Werten (dreifache Deckkraft, blur 13px) blieb vom Hintergrund
     nichts uebrig und die Kachel las sich als eigenes Objekt. */
  .schema, .log, .panel {
    -webkit-backdrop-filter: blur(7px) saturate(1.5) brightness(1.12);
    backdrop-filter: blur(7px) saturate(1.5) brightness(1.12);
    background:
      url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg'><filter id='n'><feTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='2'/></filter><rect width='100%25' height='100%25' filter='url(%23n)' opacity='0.06'/></svg>"),
      linear-gradient(115deg, rgba(232,253,255,.06) 0%, transparent 34%, transparent 62%, rgba(198,240,255,.025) 100%),
      linear-gradient(158deg, rgba(140,205,220,.042), rgba(96,170,190,.010) 45%, rgba(120,195,210,.028));
    border: 1px solid rgba(150,210,230,.15);
    border-top-color: rgba(216,250,255,.42);
    border-left-color: rgba(198,240,255,.24);
    border-bottom-color: rgba(6,14,20,.55);
    box-shadow:
      inset 0 1px 0 rgba(236,254,255,.34),
      inset 1px 0 0 rgba(210,245,255,.14),
      inset 0 -1px 0 rgba(0,0,0,.40),
      inset -1px 0 0 rgba(0,0,0,.25),
      inset 0 18px 30px -22px rgba(236,254,255,.32),
      0 20px 42px -26px rgba(0,0,0,.92);
    position: relative;
  }
  /* Abgeschraegte Ecken und Eckklammern des HUD-Themes entfallen: eine
     Glasplatte hat eine durchgehende, angeleuchtete Kante - das Motiv ersetzt
     die Klammern, statt mit ihnen zu konkurrieren. */
  .panel { clip-path: none; }
  .bracketed::before, .bracketed::after,
  .bracketed .bk-tr, .bracketed .bk-bl { display: none; }

  /* Streulicht-Pfuetze: Licht blutet unter der Platte aus. Bleibt bewusst AN
     DER KACHEL (nicht auf der Scheibe) - es entsteht ja dort, wo das Licht
     auf die Platte trifft. */
  .schema::after, .log::after, .panel::after {
    content: ""; position: absolute; left: 6%; right: 6%; bottom: -15px; height: 22px;
    background: radial-gradient(58% 100% at 50% 0%, rgba(127, 240, 228, .26), transparent 74%);
    pointer-events: none;
  }
  /* Die Failover-Pfuetze ist breiter, hoeher und doppelt so kraeftig wie die
     normale: sie ist das Hauptsignal, an dem eine rote Kachel aus der
     Entfernung erkannt wird. */
  .panel.failover::after {
    left: 2%; right: 2%; bottom: -20px; height: 34px;
    background: radial-gradient(62% 100% at 50% 0%, rgba(255, 106, 88, .52), transparent 74%);
  }
  .panel.offline::after { display: none; }

  /* Die Failover-Platte bleibt eine Glasplatte: Frost-Koernung und Reflexion
     werden mit uebernommen, nur die unterste Farbschicht und die Kanten
     werden warm. Wuerde hier nur 'background' gesetzt, verloere ausgerechnet
     die auffaelligste Kachel ihre Glaswirkung. */
  .panel.failover {
    background:
      url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg'><filter id='n'><feTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='2'/></filter><rect width='100%25' height='100%25' filter='url(%23n)' opacity='0.06'/></svg>"),
      linear-gradient(115deg, rgba(255,232,228,.07) 0%, transparent 34%, transparent 62%, rgba(255,200,190,.03) 100%),
      linear-gradient(158deg, rgba(255,130,115,.065), rgba(255,106,88,.018) 45%, rgba(255,120,105,.045));
    /* Kante und Licht tragen das Signal, nicht die Flaeche: die Toenung
       oben bleibt schwach, damit die Flow-Kurven ihren neutralen Untergrund
       behalten und voll lesbar bleiben. */
    border-color: rgba(255, 106, 88, .62);
    border-top-color: rgba(255, 200, 190, .85);
    border-left-color: rgba(255, 170, 158, .45);
    border-bottom-color: rgba(20, 6, 4, .55);
    box-shadow:
      inset 0 1px 0 rgba(255, 214, 206, .40),
      inset 1px 0 0 rgba(255, 180, 170, .14),
      inset 0 -1px 0 rgba(0,0,0,.40),
      inset -1px 0 0 rgba(0,0,0,.25),
      inset 0 0 26px rgba(255, 106, 88, .22),
      inset 0 18px 30px -22px rgba(255, 214, 206, .28),
      0 0 0 1px rgba(255, 106, 88, .30),
      0 0 22px rgba(255, 106, 88, .30),
      0 20px 42px -26px rgba(0,0,0,.92);
  }
  .panel.offline { opacity: .55; }

  /* Farbsaum an den Ziffern, wie aus einer billigen Projektionsoptik. Per
     Selektor statt per Zusatzklasse, damit am erzeugten HTML nichts haengt. */
  /* Eine Projektion strahlt selbst, sie wird nicht angestrahlt: die Schrift
     bekommt einen echten Lichthof, nicht nur Farbe. Der Farbsaum daneben
     bleibt - eine billige Projektionsoptik trennt die Farben leicht auf. */
  .pylon .val, .panel-head .fn, .readouts .rv, .log li .fn {
    text-shadow: 0 0 12px rgba(127, 240, 228, .45), -.6px 0 rgba(255, 70, 120, .30),
      .6px 0 rgba(90, 220, 255, .30);
  }
  /* Staerker als in den Kacheln: dort traegt die Platte den Kontrast mit,
     hier steht die Zahl frei auf dem Muster. */
  .pylon .val {
    text-shadow: 0 0 22px rgba(127, 240, 228, .55), 0 1px 12px rgba(0, 0, 0, .9),
      -.6px 0 rgba(255, 70, 120, .30), .6px 0 rgba(90, 220, 255, .30);
  }
  .pylon .label, .pylon .fuss { text-shadow: 0 1px 8px rgba(0, 0, 0, .85); }
  .panel .subhead, .mini-chart-label, .readouts .rl { text-shadow: 0 0 8px rgba(127, 240, 228, .22); }
  .chip.nominal { text-shadow: 0 0 9px rgba(127, 240, 228, .5); }
  .chip.failover { text-shadow: 0 0 9px rgba(255, 106, 88, .6); }

  /* Schema-Knoten leuchten wie Lichtpunkte auf der Scheibe */
  .bus .hub { background: rgba(127, 240, 228, .06); border-color: rgba(127, 240, 228, .35);
    box-shadow: 0 0 14px rgba(127, 240, 228, .12); }
  .bus .nodes::before { border-top-width: 1px; border-top-color: rgba(160, 220, 235, .2); }
  .snode .drop::before { width: 1px; margin-left: -0.5px; }
  .snode .bulb { width: 14px; height: 14px; border-width: 1px;
    background: rgba(127, 240, 228, .10); box-shadow: 0 0 9px rgba(127, 240, 228, .45); }
  .snode.alert .bulb { background: var(--alert-dim); box-shadow: 0 0 11px rgba(255, 106, 88, .55); }
  .snode.lost .bulb { background: none; box-shadow: none; }
  .snode .pulse { box-shadow: 0 0 6px var(--down); }

  .segments i.lit { background: rgba(127, 240, 228, .55); box-shadow: 0 0 6px rgba(127, 240, 228, .45); }
  .segments i.lit.warn { background: rgba(255, 196, 122, .6); box-shadow: 0 0 6px rgba(255, 196, 122, .5); }
  .segments i.lit.crit { background: rgba(255, 106, 88, .65); box-shadow: 0 0 6px rgba(255, 106, 88, .5); }

  .chart .down { fill: rgba(127, 240, 228, .6); }
  .chart .up { fill: rgba(255, 196, 122, .55); }
  .chart .flow-down-line { filter: drop-shadow(0 0 4px rgba(127, 240, 228, .5)); }
  .chart .grid { stroke: rgba(150, 210, 220, .09); }
  .chart .baseline { stroke: rgba(150, 210, 220, .2); }

  /* ------------------------------------------------------------------
     Eine Flaeche statt neun (siehe .wrap::before): die Bloecke brauchen
     jetzt keine eigene Glasflaeche, keinen Rahmen und keine eigene
     Lichtpfuetze mehr. Gegliedert wird nur noch durch Haarlinien, Abstand
     und die helle Lichtkante oben.
     ------------------------------------------------------------------ */
  .panel, .schema, .log {
    background: none; box-shadow: none; filter: none;
    -webkit-backdrop-filter: none; backdrop-filter: none;
    border-color: transparent;
    border-top-color: rgba(216, 250, 255, .42);
  }
  .panel::after, .schema::after, .log::after { content: none; }
  .panel.failover { border-top-color: rgba(255, 180, 168, .75); }

  /* Kacheln als Felder EINES Instruments: 1px Fuge statt 20px Abstand, und
     die Trennlinien enden am Rasterrand statt jede Kachel zu umschliessen. */
  .overview-grid { gap: 1px; }
  .panel {
    border-right: 1px solid rgba(160, 220, 235, .10);
    border-bottom: 1px solid rgba(160, 220, 235, .10);
    padding: 16px 20px 14px;
  }
  .panel:nth-child(3n) { border-right: none; }
  .panel:nth-child(n+4) { border-bottom: none; }
  /* Die Failover-Kachel kann keinen roten Rahmen mehr haben - sie bekommt
     stattdessen einen roten Lichtschein von oben und faellt dadurch eher
     mehr auf als vorher. */
  .panel.failover {
    background: radial-gradient(120% 90% at 50% 0%, rgba(255, 106, 88, .17), transparent 72%);
  }
  .panel .bk-tr, .panel .bk-bl { display: none; }
  .bank { gap: 16px; }
  .schema, .log { border-left: 1px solid rgba(160, 220, 235, .10); padding-left: 16px; }
  /* Der Pylon brauchte eine lokale Abdunklung, solange er als einziges
     Element frei auf dem gemusterten Hintergrund stand. Jetzt liegt er auf
     derselben Flaeche wie alles andere. */
  .pylon::before { content: none; }

  /* Geraeterahmen: fasst die Anzeige ein, statt sie am Monitorrand einfach
     aufhoeren zu lassen. Liegt als Auflage UEBER der Seite (fixed auf der
     Scheiben-Ebene) - so verhaelt sich auch ein echter Rahmen. Der
     Innenabstand der Seite ist an seine Staerke gekoppelt, siehe body. */
  .canopy::before {
    content: ""; position: fixed; inset: 0; pointer-events: none; z-index: 55;
    border: 20px solid rgba(12, 22, 29, .96);
    border-radius: 24px;
    box-shadow:
      inset 0 0 0 1px rgba(150, 205, 220, .34),
      inset 0 1px 0 1px rgba(216, 250, 255, .26),
      inset 0 0 40px rgba(4, 9, 13, .55),
      0 0 0 100vmax rgba(2, 4, 6, .96);
  }

  /* ------------------------------------------------------------------
     HUD-Struktur in der Flaeche (Nutzerwunsch: gerastert, diagonal,
     sechseckig). Alles als CSS-Verlauf bzw. SVG-Muster im data-URI, kein
     externer Abruf - den blockt die CSP der Seite ohnehin.

     Die Maske laesst das Muster nach unten auslaufen, BEVOR es die
     Flow-Kurven erreicht: ueber einer Kurve gelesen wird ein Gitter sofort
     zum Stoerer.
     ------------------------------------------------------------------ */
  .panel, .schema, .log { position: relative; }
  .panel > *:not(.bk-tr):not(.bk-bl), .schema > *, .log > * { position: relative; z-index: 1; }
  /* display/width/height/border/opacity ausdruecklich zuruecksetzen: diese
     Elemente tragen .bracketed, und dessen ::before ist 14x14px mit 2px Rand
     (die Eckklammern) bzw. im Glas-Thema display:none. Kaskadiert wird pro
     Eigenschaft - ohne das Zuruecksetzen bleibt das Muster unsichtbar oder
     14px gross. */
  /* Das Muster liegt jetzt im Hintergrund (siehe .depth), nicht mehr in der
     Kachel - sonst riss es an jeder Kachelkante ab und fing an der naechsten
     wieder an, was die Platten als aufgelegte Objekte lesbar machte. */
  .panel::before, .schema::before, .log::before { content: none; }

  /* Messskala an der Kachel-Oberkante */
  .panel::after {
    content: ""; display: block; position: absolute; left: 10px; right: 10px; top: 0;
    width: auto; height: 4px; border: none; opacity: 1;
    background-image: repeating-linear-gradient(90deg, rgba(127,240,228,.55) 0 1px, transparent 1px 9px);
    background-repeat: repeat-x; pointer-events: none; z-index: 2;
  }
  .panel.failover::after {
    background-image: repeating-linear-gradient(90deg, rgba(255,106,88,.85) 0 1px, transparent 1px 9px);
  }

  /* Gefaste Ecken. WICHTIG: eine Abdeckung in Seitenfarbe waere falsch -
     hinter den Kacheln liegt .depth mit vier Lichtverlaeufen, ein Dreieck in
     var(--ink) sitzt dort als dunkler Fleck. Also echtes clip-path.
     Das schneidet allerdings auch alles AUSSERHALB der Kachel ab, und dort
     sass die Streulicht-Pfuetze (.panel::after im Glas-Thema). Sie ist
     deshalb hier ein filter: drop-shadow - der wird NACH dem Clipping
     angewandt, folgt der geschnittenen Silhouette und leuchtet dadurch auch
     entlang der Schraege mit. */
  .panel {
    clip-path: polygon(0 0, calc(100% - 26px) 0, 100% 26px, 100% 100%, 26px 100%, 0 calc(100% - 26px));
    filter: drop-shadow(0 12px 18px rgba(127, 240, 228, .30)) drop-shadow(0 14px 26px rgba(0,0,0,.85));
  }
  .panel.failover { filter: drop-shadow(0 12px 20px rgba(255, 106, 88, .45)) drop-shadow(0 14px 26px rgba(0,0,0,.85)); }
  /* Die Schnittkante selbst, auf den ohnehin vorhandenen leeren Divs. */
  .panel .bk-tr, .panel .bk-bl { display: block; position: absolute; width: 38px; height: 2px;
    border: none; opacity: 1; pointer-events: none; z-index: 4;
    transform: rotate(-45deg); transform-origin: 50% 50%; }
  .panel .bk-tr { top: 12px; right: -5px;
    background: linear-gradient(90deg, transparent, rgba(216,250,255,.85)); }
  .panel .bk-bl { bottom: 12px; left: -5px;
    background: linear-gradient(90deg, rgba(216,250,255,.85), transparent); }
  .panel.failover .bk-tr { background: linear-gradient(90deg, transparent, rgba(255,150,135,.9)); }
  .panel.failover .bk-bl { background: linear-gradient(90deg, rgba(255,150,135,.9), transparent); }

  /* Sechseckige Statusknoten im Bus - sechs Standorte, sechs Ecken. */
  .snode .bulb { border-radius: 0;
    clip-path: polygon(25% 0%, 75% 0%, 100% 50%, 75% 100%, 25% 100%, 0% 50%); }

""".replace("__SWEEP__", str(CANOPY_SWEEP_S)).replace("__SWEEP_THIN__", str(CANOPY_SWEEP_THIN_S))

# Die Scheibe als Markup - eine einzige Ebene ueber dem GESAMTEN Bild
# (Nutzerwunsch: nicht je Kachel). Rein dekorativ, deshalb aria-hidden.
#
# Das Skript haelt den Lichtstreifen ueber den Seiten-Reload hinweg in Phase.
# Ohne das faengt die CSS-Animation bei JEDEM Laden wieder bei 0 an - und da
# die Seite sich jede Minute selbst neu laedt (refresh_countdown_script),
# sprang der Streifen einmal pro Minute sichtbar an seinen Startpunkt zurueck
# (nachgemessen: x -589px vor dem Reload, -725px danach, Animationszeit
# 3150ms -> 117ms). Ein NEGATIVES animation-delay aus der Wanduhr modulo
# Animationsdauer laesst ihn dort weiterlaufen, wo er war: alle Betrachter
# rechnen aus derselben Uhrzeit dieselbe Phase aus, unabhaengig davon, wann
# ihre Seite zuletzt geladen hat.
CANOPY_HTML = """<div class="canopy" aria-hidden="true">
  <div class="band"></div><div class="band thin"></div>
  <div class="grime"></div><div class="vignette"></div>
</div>
<script>
(function () {
  // Alle Dauerbewegungen der Seite bekommen ihre Phase aus der UHRZEIT, per
  // negativem animation-delay. Ohne das beginnt jede Bewegung bei jedem
  // Neuladen (einmal pro Minute) wieder von vorn - der Suchstrahl des Radars
  // springt zurueck, die Tropfen im Bus setzen neu an. Mit Phase laeuft
  // alles durch, als waere die Seite nie neu geladen worden.
  var jetzt = Date.now() / 1000;

  function phase(el, dauer, versatz) {
    if (!dauer) return;
    // versatz: die Staffelung, die das Element schon mitbringt (die Tropfen
    // starten je Knoten 0,35s spaeter). Sie muss erhalten bleiben, deshalb
    // wird sie zur Uhrzeit-Phase addiert statt sie zu ersetzen.
    el.style.animationDelay = (-((jetzt + (versatz || 0)) % dauer)).toFixed(2) + 's';
  }

  // Die beiden Nebelschwaden laufen unterschiedlich schnell - jede braucht
  // deshalb ihre eigene Phase, aus ihrer eigenen Dauer.
  document.querySelectorAll('.canopy .band').forEach(function (el) {
    phase(el, parseFloat(getComputedStyle(el).animationDuration) || __SWEEP__);
  });

  var radar = document.querySelector('.radar .sweep');
  if (radar) phase(radar, __RADAR__);

  document.querySelectorAll('.snode .pulse').forEach(function (el) {
    // Die Staffelung steht als inline-Style im Markup (siehe _schema_html)
    // und wird hier durch den berechneten Wert ersetzt - vorher auslesen.
    var versatz = parseFloat(getComputedStyle(el).animationDelay) || 0;
    phase(el, __PULSE__, versatz);
  });
})();
</script>""".replace("__SWEEP__", str(CANOPY_SWEEP_S)).replace("__SWEEP_THIN__", str(CANOPY_SWEEP_THIN_S)).replace("__RADAR__", str(RADAR_SWEEP_S)).replace("__PULSE__", str(PULSE_TRAVEL_S))


def render_overview_html(consoles, start, now, events=(), latency=None):
    """Übersichtsseite in Leitstand-/HUD-Optik: Telemetrie-Leiste, Monats-
    Segmentmesser, Systemschema, ein Panel pro Konsole (Kernzahlen + Mini-
    Charts) und das Ereignisprotokoll. consoles = Liste von compute_stats()-
    dicts, events = state['events'] (siehe _update_event_log)."""
    days_elapsed_month_calendar = consoles[0]["days_elapsed_month_calendar"] if consoles else 1
    days_in_month = consoles[0]["days_in_month"] if consoles else 30
    day_of_month = min(int(days_elapsed_month_calendar) + 1, days_in_month)

    total_month_all = sum(c["total_month"] for c in consoles)
    n_failover = sum(1 for c in consoles if c["is_failover"])

    def instrument(label, text, alert=False, foot=""):
        """Eine Kennzahl im Statuspylon - ohne Kasten, bewusst gross.

        Die beiden Zahlen sassen frueher in eigenen Kacheln in einer
        Telemetrie-Leiste ueber dem Deck. Als Instrumente auf dem Seitengrund
        konkurrieren sie nicht mehr optisch mit Schema und Protokoll, neben
        denen sie jetzt stehen."""
        val, unit = _split_unit(text)
        fuss = f'<div class="fuss">{foot}</div>' if foot else ""
        return (f'<div class="p-zahl"><div class="label">{label}</div>'
                f'<div class="val num{" alert" if alert else ""}">{val}'
                f'<span class="unit">{unit}</span></div>{fuss}</div>')

    # "Gesamt diesen Monat" (Nutzerwunsch) steht auf DIESEM Instrument, weil es
    # den Monatswert bereits fuehrt. Ein frueheres drittes "Gesamt seit Start"
    # ist entfallen: dort stuenden 494 GB ueber zwei Kalendermonate unter einem
    # Monats-Label - und mit dem Monatswert waere es ein exaktes Duplikat.
    pylon = "\n        ".join([
        instrument("Gesamt diesen Monat", human_bytes(total_month_all),
                   foot=f"Tag {day_of_month}/{days_in_month}"),
        instrument("Aktive Failover", f"{n_failover} / {len(consoles)}", alert=n_failover > 0),
    ])

    cards = []
    for i, c in enumerate(consoles, 1):
        status = _console_status(c)
        threshold = _console_alert_threshold(c["device"])
        ratio = c["total_month"] / threshold if threshold else 0.0
        meter_mode = "crit" if ratio > 1.0 else ("warn" if ratio > WARN_THRESHOLD_FACTOR else "")
        # Bewusst kurze, EINZEILIGE Statuszeilen: ein Umbruch hier kostet
        # ueber zwei Kachelreihen hinweg sofort ~30px Seitenhoehe, und die
        # Seite muss ohne Scrollbalken auf einen Bildschirm passen.
        if status == "failover":
            chip = '<span class="chip failover">Failover</span>'
            sub = (f'Upload-Ø {human_kbps(c["last_rate_kbps"])} &middot; Schwelle '
                   f'{_console_failover_threshold(c["device"]):.0f} kbps &middot; LTE trägt Last')
            sub_cls = " alert"
        elif status == "offline":
            chip = '<span class="chip lost">Link Lost</span>'
            if c["last_seen"] is not None:
                age_min = int((now - c["last_seen"]).total_seconds() // 60)
                sub = f'Kein Poll seit {age_min} Min &middot; Stand eingefroren'
            else:
                sub = 'Noch kein einziger Messpunkt'
            sub_cls = ""
        else:
            chip = '<span class="chip nominal">Nominal</span>'
            sub = f'Akt. Upload {human_kbps(c["last_rate_kbps"])} &middot; unter Schwelle'
            sub_cls = ""
        month_val, month_unit = _split_unit(human_bytes(c["total_month"]))
        panel_cls = {"failover": " failover", "offline": " offline"}.get(status, "")
        cards.append(f"""<div class="panel bracketed{panel_cls}">
      <div class="bk-tr"></div><div class="bk-bl"></div>
      <div class="panel-head"><h3 class="fn"><span class="idx num">{i:02d}·</span>{html.escape(c['device'])}</h3>{chip}</div>
      <div class="subhead{sub_cls}" title="{html.escape(sub.replace('&middot;', '·').replace('&gt;', '>').replace('&Oslash;', 'Ø'))}">{sub}</div>
      <div class="readouts">
        <div class="r"><div class="rl">Monat</div><div class="rv num{total_alert_class(c['total_month'], c['device'])}">{month_val}<span class="unit">{month_unit}</span> <span class="threshold-ref">/ {alert_threshold_label(c['device'])}</span></div></div>
      </div>
      <div class="mini-meter" title="Monatsvolumen im Verhältnis zur Rot-Schwelle ({alert_threshold_label(c['device'])})">{_segments_html(12, round(min(ratio, 1.0) * 12), meter_mode)}</div>
      <div class="mini-charts">
        <div class="mini-chart-col">
          <div class="mini-chart-label">Flow &middot; 24 h <span class="lg">Spitze {human_bytes(c['peak'])}/h<span class="dot" style="background:var(--down)"></span>Down<span class="dot" style="background:var(--up)"></span>Up</span></div>
          {c['flow_chart_mini']}
        </div>
      </div>
    </div>""")
    cards_html = "\n    ".join(cards) or '<p class="dim">Keine Konsolen konfiguriert.</p>'

    return f"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WAN-Failover Übersicht</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;600;700&family=JetBrains+Mono:wght@400;500;600;700&display=swap">
<style>
{BASE_CSS}
{HUD_CSS}
{GLASS_CSS if COLOR_THEME == "glas" else ""}
</style>
</head>
<body>
{'<div class="depth"></div>' if COLOR_THEME == "glas" else ""}
<div class="wrap">
  <header>
    <div class="header-top">
      <div class="header-info">
        <div class="boot-line">
          <span class="dot"></span> LIVE &middot; STAND {now.astimezone().strftime('%d.%m.%Y %H:%M:%S')}
          &middot; NÄCHSTER REFRESH <span id="refresh-cd">{REPORT_REFRESH_S // 60}:00</span>
          <span class="sync" title="Alter des angezeigten Datenstands. Teal bis {FRESH_WARN_S // 60} Min, amber bis {FRESH_STALE_S // 60} Min, danach rot: die Seite wird nicht mehr aktualisiert (Workflow prüfen).">
            <i class="sync-dot"></i> SYNC <span id="sync-val" class="num">0s</span>
          </span>
        </div>
        <h1>WAN-Failover <span class="accent">Übersicht</span></h1>
      </div>
      {_logo_html()}
    </div>

  </header>

  <div class="bank">
    <div class="pylon">
      {_radar_html(consoles, latency)}
      <div class="werte">
        {pylon}
      </div>
    </div>

    <div class="schema bracketed">
      <div class="bk-tr"></div><div class="bk-bl"></div>
      <div class="mlabel">Systemschema &middot; Site-Manager-Bus</div>
      {_schema_html(consoles, latency)}
    </div>

    <div class="log bracketed">
      <div class="bk-tr"></div><div class="bk-bl"></div>
      <div class="log-head"><span>Ereignisprotokoll</span><span>Statuswechsel</span></div>
      <ol>
        {_event_log_html(events, now)}
      </ol>
    </div>
  </div>

  <div class="overview-grid">
    {cards_html}
  </div>

  <footer>Dauerbetrieb seit {start.astimezone().strftime('%d.%m.%Y %H:%M')} &middot;
    Datenquelle: SIM-Datenzähler der LTE-Modems (via Site-Manager-Connector-Proxy), {len(consoles)} Konsolen &middot;
    Seite aktualisiert sich alle {REPORT_REFRESH_S // 60} Minute{'n' if REPORT_REFRESH_S // 60 != 1 else ''} selbst &middot;
    Failover ab Upload-Ø &gt; {FAILOVER_THRESHOLD_KBPS:.0f} kbps über {FAILOVER_CONSECUTIVE} Polls
    ({", ".join(f"{n.split('--')[0]} {v:.0f}" for n, v in FAILOVER_THRESHOLD_KBPS_BY_CONSOLE.items())}) &middot;
    Link Lost ab {OFFLINE_THRESHOLD_S // 60} Min ohne Messpunkt.</footer>
</div>
{CANOPY_HTML if COLOR_THEME == "glas" else ""}
{refresh_countdown_script(now)}
</body>
</html>"""


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


def write_reports(rows, start, console_names, state=None, record_events=True):
    """Schreibt die Übersichtsseite (wan_report.html) - reine Übersichtskacheln.
    Einzelkonsolen-Detailseiten gab es frueher zusaetzlich; sie waren von der
    Uebersicht nie verlinkt, wurden nicht genutzt und sind mitsamt ihrem Code
    entfernt (Nutzerwunsch).

    Performance: compute_stats() ist mit wachsender CSV der teuerste Teil
    (scannt Zeilen je Konsole fuer Monatssumme und Charts). rows wird VORAB
    einmal per _group_by_console() aufgeteilt, statt dass jeder der 6
    compute_stats()-Aufrufe die komplette Liste erneut scannt.

    state: monitor_state.json-dict. Wird IN-PLACE um Ereignisprotokoll und
    zuletzt gesehenen Status je Konsole ergaenzt (siehe _update_event_log);
    der Aufrufer muss es danach sichern. Ohne state (z.B. Test/Diagnose)
    wird die Seite ohne Protokoll-Eintraege gerendert.

    record_events=False zeigt ein vorhandenes Protokoll an, schreibt es aber
    NICHT fort - fuer --report (reiner Bericht aus der CSV, ohne API-Zugriff),
    das sonst Pseudo-Statuswechsel erzeugen wuerde, die der naechste echte
    Poll dann ein zweites Mal meldet."""
    now = datetime.now(timezone.utc)
    buckets = _group_by_console(rows, console_names)
    consoles = [compute_stats(buckets[name], start, site_filter=name, rows_prefiltered=True)
                for name in console_names]

    if state is None:
        events, latency = [], {}
    else:
        if record_events:
            _update_event_log(state, consoles, now)
        events = state.get("events", [])
        # Latenz kommt aus dem Zwischenspeicher, nicht aus einem eigenen
        # Abruf - so funktioniert auch --report (ohne API-Zugriff) mit dem
        # zuletzt geholten Stand.
        latency = (state.get("latency") or {}).get("sites") or {}

    overview_html = render_overview_html(consoles=consoles, start=start, now=now,
                                         events=events, latency=latency)
    _atomic_write(HTML_PATH, lambda handle: handle.write(overview_html))
    # Standdatei fuer die Reload-Pruefung der Seite (siehe STAND_PATH).
    _atomic_write(STAND_PATH, lambda handle: handle.write(now.isoformat()))
    return HTML_PATH


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

    # Die Netzwerk-Abfrage (get_sim_bytes) ist der Flaschenhals - laut
    # Laufzeit-Diagnose im Workflow ~30-45s bei 6 Konsolen NACHEINANDER
    # abgefragt, waehrend Checkout/Setup/Git-Operationen zusammen unter 2s
    # liegen. Deshalb parallel per Thread-Pool statt sequenziell: waehrend
    # ein Thread auf die HTTP-Antwort wartet, gibt Python das GIL frei, ein
    # simpler Thread-Pool reicht also (kein echtes CPU-paralleles
    # Multiprocessing noetig). Die Gesamtlaufzeit naehert sich dadurch der
    # langsamsten Einzelabfrage an statt der Summe aller sechs.
    def fetch(target):
        console_name, host_id, site_name, mac = target
        try:
            return console_name, get_sim_bytes(host_id, site_name, mac), None
        except Exception as exc:
            return console_name, None, exc

    fetched = {}
    with ThreadPoolExecutor(max_workers=len(targets) or 1) as executor:
        futures = [executor.submit(fetch, t) for t in targets]
        for future in as_completed(futures):
            console_name, rxtx, exc = future.result()
            fetched[console_name] = (rxtx, exc)

    points = []
    # Verarbeitung (Delta-Berechnung, Baseline-Update) bleibt bewusst
    # sequenziell UND in der urspruenglichen targets-Reihenfolge
    # (deterministische Log-Ausgabe) - reine CPU-Arbeit, dauert nur
    # Millisekunden, eine Parallelisierung wuerde hier nichts bringen, aber
    # das gemeinsam genutzte sim_baseline-Dict unnoetig verkomplizieren.
    for console_name, host_id, site_name, mac in targets:
        rxtx, fetch_exc = fetched[console_name]
        if fetch_exc is not None:
            print(f"  {console_name}: Poll-Fehler, ueberspringe diesen Durchlauf - {fetch_exc}")
            continue
        rx, tx = rxtx
        # Der GESAMTE restliche Block fuer eine Konsole steht bewusst im
        # try/except: eine unerwartet fehlerhafte Baseline darf nicht den
        # kompletten Poll-Durchlauf (und damit die Daten ALLER anderen
        # Konsolen) zum Absturz bringen - hier nur diese Konsole ueberspringen.
        try:
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
    if args.report:
        rows = load_rows()
        index_path = write_reports(rows, start, console_names,
                                   state=state, record_events=False)
        print(f"Übersicht geschrieben: {index_path}")
        return

    # Zielermittlung parallel, aus demselben Grund wie in poll(): je Konsole
    # sind das ZWEI HTTP-Anfragen (/sites und die Geraeteliste), bei sechs
    # Konsolen also zwoelf nacheinander. Der Schritt laeuft bei jedem
    # Workflow-Lauf neu, weil --once jedes Mal einen neuen Prozess startet.
    # Waehrend ein Thread auf die Antwort wartet, gibt Python das GIL frei -
    # ein einfacher Thread-Pool genuegt.
    def _ziel(eintrag):
        name, host_id = eintrag
        try:
            sites = connector_get(host_id, "/sites")
            site_id = sites["data"][0]["id"]
            site_name = sites["data"][0]["internalReference"]
            return name, (name, host_id, site_name, find_lte_modem(host_id, site_id)), None
        except Exception as exc:
            return name, None, exc

    # Reihenfolge der Ausgabe bleibt die der Konfiguration, nicht die des
    # Antworteingangs - sonst wechselt die Log-Reihenfolge bei jedem Lauf.
    ergebnisse = {}
    with ThreadPoolExecutor(max_workers=len(targets_cfg) or 1) as executor:
        for name, ziel, exc in executor.map(_ziel, targets_cfg):
            ergebnisse[name] = (ziel, exc)

    targets = []
    for name, _host_id in targets_cfg:
        ziel, exc = ergebnisse.get(name, (None, None))
        if ziel is None:
            # Wie in poll(): eine einzelne offline Konsole darf nicht
            # verhindern, dass die restlichen ueberhaupt Daten bekommen.
            print(f"Ziel: {name}: uebersprungen, nicht erreichbar - {exc}")
            continue
        targets.append(ziel)
        print(f"Ziel: {name} (site={ziel[2]}, lte_mac={ziel[3]})")

    rows = load_rows()
    while True:
        rows, added = poll(rows, targets, args.interval, sim_baseline)
        # Latenz fuer das Systemschema. Eigener Aufruf, aber hoechstens alle
        # LATENCY_REFRESH_S - die API liefert ohnehin nur 5-Minuten-Punkte.
        # Fehlschlaege sind hier folgenlos (siehe refresh_latency).
        refresh_latency(state, {h: n for n, h in targets_cfg}, datetime.now(timezone.utc))
        # Baseline sofort sichern: scheitert die Berichtserzeugung danach,
        # waere der frisch geholte SIM-Zaehlerstand sonst verloren und der
        # naechste Lauf muesste neu baselinen (ein Intervall ohne Delta).
        save_state()
        # write_reports() ergaenzt state um die Ereignisprotokoll-Eintraege -
        # die brauchen ein ZWEITES save_state() danach, sonst waeren die
        # Statuswechsel beim naechsten Prozessstart wieder weg (--once startet
        # je Poll einen neuen Prozess) und wuerden endlos neu gemeldet.
        path = write_reports(rows, start, console_names, state=state)
        save_state()
        stamp = datetime.now().astimezone().strftime("%H:%M:%S")
        print(f"[{stamp}] {added} neue Messpunkte, {len(rows)} gesamt -> {path}")
        if args.once:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
