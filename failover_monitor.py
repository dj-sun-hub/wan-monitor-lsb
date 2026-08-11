#!/usr/bin/env python3
"""
UDM Failover-Live-Monitor
==========================

Rein lokaler Dienst (gedacht fuer ein NAS/einen Server im LAN, NICHT fuer
GitHub Pages): pollt alle paar Sekunden die Live-Uplink-Rate der UDM-eigenen
WAN-Schnittstelle (LTE-Backup) jeder Konsole und schaltet eine Kachel auf
"FAILOVER" (rot), sobald die Rate ueber mehrere Polls in Folge einen
Schwellwert ueberschreitet - Indiz dafuer, dass echter Standort-Traffic ueber
die LTE-Leitung umgeleitet wird, weil die Sophos-Hauptleitung (Glasfaser)
ausgefallen ist.

Hintergrund/Einschraenkungen (siehe Chat-Verlauf):
- Sophos Central haette den saubereren Trigger geliefert (VPN-tunnel-down-
  Events), war aber mangels API-Rechten nicht erreichbar.
- Die UniFi Network-Integration-API liefert keine Port-genauen Byte-Zaehler,
  nur eine aggregierte Uplink-Rate pro Konsole - das ist hier der Trigger.
- Der Schwellwert ist ein Startwert und MUSS nach dem ersten echten
  Failover-Ereignis kalibriert werden (siehe FAILOVER_THRESHOLD_KBPS unten).

Der UI_API_KEY wird ausschliesslich aus der Umgebungsvariable gelesen und
niemals in eine Datei geschrieben. Diese Seite ist nur fuers lokale Netz
gedacht - niemals oeffentlich (z.B. via Port-Weiterleitung) freigeben, da der
API-Key server-seitig im Prozess liegt (aber nicht im HTML landet).

Aufruf
------
  python3 failover_monitor.py
      Laeuft endlos, pollt alle POLL_INTERVAL_S Sekunden, schreibt bei jedem
      Durchlauf status.html neu. Mit Strg+C oder Prozess-Kill beenden.

Auf dem NAS einrichten: siehe Chat-Anleitung (Task Scheduler "Beim Systemstart"
oder Container Manager).
"""

import http.server
import json
import os
import socketserver
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

CONNECTOR_BASE = "https://api.ui.com/v1/connector/consoles"

# Dieselben Konsolen wie beim WAN-Volumen-Monitor (WSG bewusst aussen vor).
# LSB ist vorerst ausgenommen: dauerhaft erhoehte, noch ungeklaerte Grundlast
# wuerde staendige Fehlalarme erzeugen, solange die Ursache nicht gefunden ist.
MONITORED_HOSTS = [
    ("HAN--UDM-1", "6C63F8AA761300000000093BE1DD0000000009BB150D00000000685B037F:1493605141"),
    ("KLO--UDM-1", "6C63F8E2993E000000000941189A0000000009C0A86600000000686B5BD1:662409267"),
    ("KNZ--UDM-1", "0CEA14D5BB63000000000899F5C200000000090F78C6000000006763F2E2:1427694241"),
    ("NID--UDM-1", "6C63F8AB54A900000000093C22300000000009BB5AAA00000000685B7889:144919651"),
    ("WTB--UDM-1", "0CEA146F1C450000000008887B2B0000000008FCF87300000000674692E9:927254559"),
]

POLL_INTERVAL_S = int(os.environ.get("FAILOVER_POLL_INTERVAL_S", "5"))
# Schwellwert in kbps (kombiniert Down+Up), ueber dem "vermutlich Failover
# aktiv" angenommen wird. STARTWERT - nach dem ersten echten Failover-
# Ereignis anhand der beobachteten Rate kalibrieren.
FAILOVER_THRESHOLD_KBPS = float(os.environ.get("FAILOVER_THRESHOLD_KBPS", "500"))
# So viele Polls IN FOLGE ueber dem Schwellwert, bevor wirklich auf rot
# geschaltet wird (verhindert Fehlalarm durch kurze Spitzen).
FAILOVER_CONSECUTIVE = int(os.environ.get("FAILOVER_CONSECUTIVE", "3"))
HTTP_PORT = int(os.environ.get("FAILOVER_HTTP_PORT", "8765"))

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
HTML_PATH = os.path.join(DATA_DIR, "failover_status.html")

STATE = {name: {"streak": 0, "failover": False, "rate_kbps": 0.0, "last_ok": None}
         for name, _ in MONITORED_HOSTS}
STATE_LOCK = threading.Lock()


def api_key():
    key = os.environ.get("UI_API_KEY", "").strip()
    if not key:
        sys.exit(
            "UI_API_KEY ist nicht gesetzt.\n"
            "  Linux/Synology: export UI_API_KEY='...'  (oder im Task Scheduler als Umgebungsvariable)\n"
        )
    return key


def connector_get(host_id, path, timeout=15):
    url = (CONNECTOR_BASE + "/" + urllib.parse.quote(host_id, safe=":")
           + "/proxy/network/integration/v1" + path)
    req = urllib.request.Request(url, headers={
        "X-API-KEY": api_key(),
        "Accept": "application/json",
        "User-Agent": "udm-failover-monitor/1.0",
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def find_gateway_device(host_id):
    sites = connector_get(host_id, "/sites")
    site_id = sites["data"][0]["id"]
    devices = connector_get(host_id, f"/sites/{site_id}/devices")
    mac_target = host_id.split(":")[0][:12].upper()
    for dev in devices.get("data", []):
        if dev.get("macAddress", "").replace(":", "").upper() == mac_target:
            return site_id, dev["id"]
    raise RuntimeError(f"Gateway-Geraet fuer Host {host_id} nicht gefunden.")


def get_uplink_rates(host_id, site_id, device_id):
    stats = connector_get(host_id, f"/sites/{site_id}/devices/{device_id}/statistics/latest")
    uplink = stats.get("uplink", {})
    return uplink.get("rxRateBps", 0) or 0, uplink.get("txRateBps", 0) or 0


def poll_loop():
    print("Ermittle Geraete-IDs ...")
    targets = []
    for name, host_id in MONITORED_HOSTS:
        try:
            site_id, device_id = find_gateway_device(host_id)
            targets.append((name, host_id, site_id, device_id))
            print(f"  {name}: site_id={site_id} device_id={device_id}")
        except Exception as exc:
            print(f"  {name}: FEHLER bei Geraete-Suche - {exc}")

    while True:
        started = time.monotonic()
        for name, host_id, site_id, device_id in targets:
            try:
                rx_bps, tx_bps = get_uplink_rates(host_id, site_id, device_id)
                rate_kbps = (rx_bps + tx_bps) / 1000.0
                with STATE_LOCK:
                    s = STATE[name]
                    s["rate_kbps"] = rate_kbps
                    s["last_ok"] = datetime.now(timezone.utc)
                    if rate_kbps > FAILOVER_THRESHOLD_KBPS:
                        s["streak"] += 1
                    else:
                        s["streak"] = 0
                    s["failover"] = s["streak"] >= FAILOVER_CONSECUTIVE
            except Exception as exc:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {name}: Poll-Fehler - {exc}")
        write_html()
        elapsed = time.monotonic() - started
        time.sleep(max(POLL_INTERVAL_S - elapsed, 0.5))


def write_html():
    with STATE_LOCK:
        snapshot = {k: dict(v) for k, v in STATE.items()}
    now = datetime.now(timezone.utc)
    cards = []
    any_failover = False
    for name, s in snapshot.items():
        cls = "ok"
        label = "Hauptleitung"
        if s["failover"]:
            cls = "alert"
            label = "FAILOVER / LTE-BACKUP AKTIV"
            any_failover = True
        elif s["streak"] > 0:
            cls = "warn"
            label = f"Rate erhoeht ({s['streak']}/{FAILOVER_CONSECUTIVE})"
        age = "-"
        if s["last_ok"]:
            age = f"{(now - s['last_ok']).total_seconds():.0f}s"
        cards.append(f"""<div class="card {cls}">
      <h2>{name}</h2>
      <div class="rate">{s['rate_kbps']:.1f} kbps</div>
      <div class="label">{label}</div>
      <div class="age">zuletzt aktualisiert vor {age}</div>
    </div>""")

    page_title = "ALLES OK" if not any_failover else "FAILOVER AKTIV"
    body_class = "alert-bg" if any_failover else ""

    html = f"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="{POLL_INTERVAL_S}">
<title>Failover-Status - {page_title}</title>
<style>
  :root {{ --ok: #2e7d4f; --warn: #b8860b; --alert: #c0392b; --bg: #101418; --panel: #1b2128; }}
  * {{ box-sizing: border-box; }}
  body {{ margin: 0; background: var(--bg); color: #eee; font-family: system-ui, sans-serif; padding: 24px; }}
  body.alert-bg {{ background: #2a0f0f; }}
  h1 {{ margin: 0 0 4px; font-size: 22px; }}
  .sub {{ color: #9aa5b1; font-size: 13px; margin-bottom: 20px; }}
  .grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; }}
  .card {{ background: var(--panel); border-radius: 10px; padding: 20px; border: 2px solid #333; }}
  .card h2 {{ margin: 0 0 8px; font-size: 17px; }}
  .card .rate {{ font-size: 26px; font-weight: 600; font-family: ui-monospace, monospace; }}
  .card .label {{ margin-top: 6px; font-weight: 600; font-size: 13px; text-transform: uppercase; }}
  .card .age {{ margin-top: 8px; color: #9aa5b1; font-size: 11px; }}
  .card.ok {{ border-color: var(--ok); }}
  .card.ok .label {{ color: var(--ok); }}
  .card.warn {{ border-color: var(--warn); }}
  .card.warn .label {{ color: var(--warn); }}
  .card.alert {{ border-color: var(--alert); background: #3a1414; animation: blink 1s infinite; }}
  .card.alert .label {{ color: #ff6b6b; }}
  @keyframes blink {{ 50% {{ border-color: #ff6b6b; }} }}
</style>
</head>
<body class="{body_class}">
  <h1>UDM Failover-Live-Status</h1>
  <div class="sub">Stand {now.astimezone().strftime('%d.%m.%Y %H:%M:%S')} &middot;
    Schwellwert {FAILOVER_THRESHOLD_KBPS:.0f} kbps &middot;
    {FAILOVER_CONSECUTIVE} Polls in Folge noetig &middot; Poll-Intervall {POLL_INTERVAL_S}s</div>
  <div class="grid">
    {"".join(cards)}
  </div>
</body>
</html>"""
    with open(HTML_PATH, "w", encoding="utf-8") as handle:
        handle.write(html)


class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DATA_DIR, **kwargs)

    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        if self.path in ("/", ""):
            self.path = "/failover_status.html"
        return super().do_GET()


def main():
    api_key()  # frueh pruefen, klare Fehlermeldung statt spaeterem Absturz
    write_html()  # sofort eine erste (leere) Seite, damit der Server nicht 404 wirft
    threading.Thread(target=poll_loop, daemon=True).start()
    with socketserver.TCPServer(("0.0.0.0", HTTP_PORT), QuietHandler) as httpd:
        print(f"Live-Status unter http://<NAS-IP>:{HTTP_PORT}/ erreichbar.")
        httpd.serve_forever()


if __name__ == "__main__":
    main()
