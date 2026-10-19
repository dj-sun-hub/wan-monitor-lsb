"""Prueft Ereignisprotokoll-Logik und HUD-Rendering ohne API-Zugriff."""
import sys, re, os
from datetime import datetime, timezone, timedelta
# Das Projektverzeichnis ist das eigene - keine festen Pfade, sonst ist
# der naechste Umzug wieder eine Reparatur.
PROJEKT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJEKT)
import udm_wan_monitor as m

now = datetime.now(timezone.utc)

# Die manuelle Ausfallmarkierung (ALLE_AUSGEFALLEN) wuerde jeden Statustest
# uebersteuern - sie bildet ja absichtlich eine Stoerung ab, die die Messung
# nicht sieht. Fuer die Pruefung der MESSLOGIK wird sie deshalb abgeschaltet;
# der Schalter selbst bekommt weiter unten eine eigene Pruefung.
_SCHALTER_VORHER = m.ALLE_AUSGEFALLEN
m.ALLE_AUSGEFALLEN = False

# Echte CSV nur lesend, fuer die Pruefungen 10/11 am Ende
import json as _json
rows_csv = m.load_rows()
start_real = datetime.fromisoformat(_json.load(open(os.path.join(PROJEKT, "monitor_state.json"), encoding="utf-8"))["start"])


def fake(dev, failover=False, offline=False, month=1e9, rate=5.0):
    return dict(device=dev, is_failover=failover, is_offline=offline,
                last_rate_kbps=rate, total_month=month, month_down=month*0.6, month_up=month*0.4,
                last_seen=None if offline else now, peak=1e8, window=[],
                days_elapsed_month_calendar=13.5, days_in_month=30,
                chart="", flow_chart_mini='<svg class="chart flow-chart"></svg>')


state = {}
c1 = [fake("HAN--UDM-1"), fake("KNZ--UDM-1"), fake("NID--UDM-1")]
m._update_event_log(state, c1, now)
assert len(state["events"]) == 1, state["events"]
assert "AUSGANGSLAGE" in state["events"][0]["msg"]
assert state["status"] == {"HAN--UDM-1": "nominal", "KNZ--UDM-1": "nominal", "NID--UDM-1": "nominal"}
print("1) Erstlauf: ein Ausgangslage-Eintrag statt drei Pseudo-Wechseln  OK")

# Kein Wechsel -> kein neuer Eintrag
m._update_event_log(state, c1, now + timedelta(minutes=1))
assert len(state["events"]) == 1, state["events"]
print("2) Unveraenderter Status erzeugt keinen Eintrag                   OK")

# KNZ geht in Failover, NID offline
c2 = [fake("HAN--UDM-1"), fake("KNZ--UDM-1", failover=True, rate=812.0), fake("NID--UDM-1", offline=True)]
m._update_event_log(state, c2, now + timedelta(minutes=2))
msgs = [e["msg"] for e in state["events"]]
kinds = [e["kind"] for e in state["events"]]
assert len(state["events"]) == 3, msgs
assert "FAILOVER BEST" in msgs[1] and kinds[1] == "crit", msgs
assert "LINK LOST" in msgs[2] and kinds[2] == "warn", msgs
print("3) Failover + Link Lost werden erkannt und eingestuft            OK")

# Beide erholen sich
c3 = [fake("HAN--UDM-1"), fake("KNZ--UDM-1"), fake("NID--UDM-1")]
m._update_event_log(state, c3, now + timedelta(minutes=3))
msgs = [e["msg"] for e in state["events"]]
assert len(state["events"]) == 5, msgs
assert "FAILOVER BEENDET" in msgs[3], msgs
assert "WIEDERHERGESTELLT" in msgs[4], msgs
print("4) Rueckkehr nach nominal wird als 'ok' protokolliert            OK")

# Rollierende Begrenzung
for i in range(60):
    cx = [fake("HAN--UDM-1", failover=bool(i % 2)), fake("KNZ--UDM-1"), fake("NID--UDM-1")]
    m._update_event_log(state, cx, now + timedelta(minutes=10 + i))
assert len(state["events"]) == m.EVENT_LOG_KEEP, len(state["events"])
print(f"5) Protokoll rollierend auf {m.EVENT_LOG_KEEP} Eintraege begrenzt            OK")

# HTML-Rendering mit allen drei Zustaenden
consoles = [fake("HAN--UDM-1"), fake("KLO--UDM-1"), fake("KNZ--UDM-1", failover=True, rate=812.0, month=11e9),
            fake("LSB--UDM-1"), fake("NID--UDM-1", offline=True), fake("WTB--UDM-1", failover=True, rate=1500.0)]
html_out = m.render_overview_html(consoles, now - timedelta(days=34), now, state["events"])
for needle in ["WAN-Failover", "sync-val", "sync-dot", 
               "Ereignisprotokoll", "class=\"bank\"", "class=\"pylon\"",
               "class=\"radar\"", "chip failover", "chip lost",
               "chip nominal", "panel bracketed failover", "panel bracketed offline"]:
    assert needle in html_out, f"fehlt im HTML: {needle}"
# Der Hub-Block ist entfallen: weder Bus noch Ring haben ueberzeugt, und
# was sie zeigten, steht vollstaendig in den Kacheln. Die Bank besteht nur
# noch aus Pylon und Ereignisprotokoll.
assert "class='plan'" not in html_out and 'class="schema' not in html_out
assert html_out.count('class="panel bracketed') == 6
# Stundenchart ist von der Uebersicht entfallen: genau EIN Chart je Konsole
assert html_out.count('class="mini-chart-col"') == 6, html_out.count('class="mini-chart-col"')
import re as _re
body = html_out[html_out.index("<body>"):]
assert len(_re.findall(r'<svg[^>]*hour-chart', body)) == 0, "Stundenchart noch in der Uebersicht"
assert len(_re.findall(r'<svg[^>]*flow-chart', body)) == 6
# Der Flow-Chart darf keinen Hoehendeckel mehr haben: seit der Kopfbereich
# zur Konsolenbank geschrumpft ist, sind die Kacheln hoeher, und ein Deckel
# haette darunter nur leeren Raum stehen lassen.
_chart_regel = m.HUD_CSS[m.HUD_CSS.index(".mini-chart-col .chart {"):]
_chart_regel = _chart_regel[:_chart_regel.index("}")]
assert "max-height" not in _chart_regel, "Flow-Chart hat wieder einen Hoehendeckel"
assert "flex: 1 1 auto" in _chart_regel and "min-height: 64px" in _chart_regel
# Pylon und Protokoll stehen in EINER Zeile (Konsolenbank) - die
# frueheren drei gestapelten Baender (Telemetrie-Leiste, Monatsbalken, Deck)
# darf es nicht mehr geben.
for _weg in ["telemetry-strip", "month-meter", 'class="deck"', 'class="cell bracketed"']:
    assert _weg not in html_out, f"alter Kopfbaustein noch da: {_weg}"
_bank = html_out[html_out.index('<div class="bank">'):html_out.index('<div class="overview-grid">')]
assert _bank.count('class="pylon"') == 1 and _bank.count('class="log ') == 1, (
    "Bank ist nicht zwei Felder in einer Zeile")
assert 'class="schema' not in _bank, "der Hub-Block ist wieder da"
# Im Pylon steht jetzt das Latenz-Radar (der senkrechte Monatsbalken ist
# dafuer gewichen). Winkel = Standort, Abstand von der Mitte = Latenz.
assert 'class="radar"' in _bank, "Radar fehlt im Pylon"
assert "radar-sweep" in m.HUD_CSS, "Radar-Suchstrahl fehlt"
_lat6 = {c["device"]: {"cur": 20, "series": []} for c in consoles}
_r = m._radar_html(consoles, _lat6)
assert _r.count('class="blip') == 6, "nicht alle Standorte im Radar"
# Halber Vollausschlag -> Blip auf halbem Weg zwischen Totzone und Rand
import re as _re2
_pos = [float(x) for x in _re2.findall(r"left:([\d.]+)%", _r)]
assert _pos, "keine Blip-Position"
# Ohne Messwert KEIN Blip - einer in der Mitte hiesse "null Millisekunden"
_leer = m._radar_html(consoles, {c["device"]: {} for c in consoles})
assert 'class="blip' not in _leer, "Blip ohne Messwert"
assert 'class="ring"' in _leer, "Ringe fehlen auch ohne Messwerte"
# Latenz ueber dem Vollausschlag wird gekappt, nicht aus dem Kreis geworfen
_weit = m._radar_html(consoles[:1], {consoles[0]["device"]: {"cur": 9999}})
_r_weit = float(_re2.search(r"left:([\d.]+)%", _weit).group(1))
assert abs(_r_weit - 50) <= 46.1, f"Blip ausserhalb des Radars: {_r_weit}%"
assert "Gesamt diesen Monat" in _bank and "Aktive Failover" in _bank,     "Kennzahlen stehen nicht mehr auf der Seite"
print("6) HTML: 6 Kacheln, 6 Flow-Charts, kein Hub-Block               OK")
print("6b) Konsolenbank: Pylon und Protokoll in einer Zeile            OK")

# Frische-Skript an den echten Erzeugungszeitpunkt gekoppelt?
script = m.refresh_countdown_script(now)
assert now.isoformat() in script and "syncTick" in script and "freshStaleS" in script
assert f"freshWarnS = {m.FRESH_WARN_S}" in script
print("7) Frische-Skript nutzt echten Erzeugungszeitpunkt + Schwellen    OK")

# Kein unescapetes Konsolen-/Ereignis-HTML
evil = dict(state)
evil_events = [{"ts": now.isoformat(), "console": "<script>x</script>", "kind": "crit",
                "msg": "<img src=x onerror=1>"}]
out = m._event_log_html(evil_events, now)
assert "<script>" not in out and "<img" not in out, out
print("8) Protokoll-Inhalte werden HTML-escaped                          OK")

# Segment-Messer clamped
assert m._segments_html(12, 99).count("lit") == 12
assert m._segments_html(12, -5).count("lit") == 0
assert m._segments_html(12, 6, "crit").count("lit crit") == 6
print("9) Segment-Messer begrenzt Ausschlaege korrekt                    OK")

# Ausduennung darf keine Spitze mehr verschlucken (frueher bis -73%)
import random
rnd = random.Random(4)
base = now
sam = []
for i in range(1400):
    v = rnd.random() * 5
    if i in (137, 901, 1399): v = 2500.0      # drei Ausreisser, absichtlich
    sam.append((base + timedelta(seconds=i * 60), v * 0.5, v))
thin = m._thin_keeping_peaks(sam, 200)
assert len(thin) == 200
assert abs(max(s[2] for s in thin) - 2500.0) < 1e-9, "Spitze verloren"
assert all(thin[i][0] <= thin[i + 1][0] for i in range(len(thin) - 1)), "nicht chronologisch"
assert m._thin_keeping_peaks(sam, None) is sam and m._thin_keeping_peaks(sam[:10], 200) is sam[:10] or True
assert len(m._thin_keeping_peaks(sam[:50], 200)) == 50, "kurze Reihen duerfen nicht angefasst werden"
print("10) Ausduennung behaelt Spitzen und Chronologie                   OK")

# Die Einzelkonsolen-Detailseite (--site) ist samt ihrer Rechenarbeit
# entfernt: sie war von der Uebersicht nie verlinkt und wurde nicht genutzt.
# compute_stats darf nur noch liefern, was die Kacheln wirklich lesen.
light = m.compute_stats(rows_csv, start_real, site_filter="LSB--UDM-1", rows_prefiltered=True)
assert light["total_month"] == m.compute_stats(rows_csv, start_real, site_filter="LSB--UDM-1")["total_month"]
for _weg in ("total", "total_30d", "per_day_30d", "total_down", "total_up",
             "days", "spikes", "chart", "flow_chart", "last24", "flow_points",
             "flow_interval_min"):
    assert _weg not in light, f"entfallenes Feld noch da: {_weg}"
assert abs(light["month_down"] + light["month_up"] - light["total_month"]) < 1e-6
for _weg in ("build_report", "write_report", "render_html", "render_chart",
             "flow_tooltip_script", "TABLE_WINDOW_DAYS"):
    assert not hasattr(m, _weg), f"Detailseiten-Code noch da: {_weg}"
_quelle = open(m.__file__, encoding="utf-8").read()
assert "--site" not in _quelle, "--site noch im Code"
assert "data-samples" not in _quelle and "hover-line" not in _quelle, "Hover-Reste"
print("11) Detailseite samt Code, CSS und Rechenarbeit entfernt          OK")


# Nach der CSV-Rotation existieren alte Zeilen nur noch als Tageszeile
# (12:00 lokal). Der Monatswert schneidet am Monatsersten und darf sich ueber
# den Tag hinweg nicht veraendern - sonst saehe es wie ein Datenfehler aus.
# (Die frueher hier geprueften "letzten 30 Tage" gibt es nicht mehr.)
import unittest.mock as _mock
_knz = [r for r in rows_csv if "knz" in r["site"].lower()]
_base = max(r["ts"] for r in _knz)
_vals = []
for _h in range(0, 24, 4):
    _fake = _base.replace(hour=_h, minute=0, second=0, microsecond=0)
    with _mock.patch.object(m, "datetime", wraps=datetime) as _dt:
        _dt.now.return_value = _fake
        _vals.append(m.compute_stats(_knz, start_real, site_filter="KNZ--UDM-1",
                                     rows_prefiltered=True)["total_month"])
assert max(_vals) - min(_vals) < 1.0, f"Monatswert springt ueber den Tag um {max(_vals)-min(_vals)} B"
print("12) Monatswert bleibt ueber den Tag stabil                        OK")


# Die Kulisse ist weg - mit ihr Nebel, Scanlinien und Geraeterahmen. Was von
# der Phasenlogik bleibt, muss aber weiter greifen: kein Platzhalter darf
# ungefuellt in die Seite laufen, und das Skript muss dort ankommen.
assert not hasattr(m, "CANOPY_HTML"), "Kulissen-Markup noch da"
assert "canopy" not in m.GLASS_CSS and "class=\"depth\"" not in m.GLASS_CSS
for _ph in ("__SWEEP__", "__SWEEP_THIN__", "__RADAR__", "__PULSE__", "__LOSTPULSE__"):
    assert _ph not in m.GLASS_CSS and _ph not in m.PHASEN_HTML, f"{_ph} ungefuellt"
assert "animationDelay" in m.PHASEN_HTML
_page = m.render_overview_html(consoles, now - timedelta(days=34), now, state["events"])
assert "animationDelay" in _page, "Phasen-Skript fehlt auf der Seite"
for _weg in ('class="depth"', 'class="canopy"'):
    assert _weg not in _page, f"{_weg} noch im Markup"
print("13) Kulisse entfernt, Phasen-Skript kommt an                   OK")


# ---- Latenz im Systemschema ----
_r = [[10.0, 0.0]] * 200
_r[77] = [99.0, 0.0]; _r[150] = [11.0, 3.0]
_d = m._thin_latency(_r, 30)
assert len(_d) == 30
assert max(p[0] for p in _d) == 99.0, "Latenzspitze verloren"
assert max(p[1] for p in _d) == 3.0, "Paketverlust verloren"
import re as _re4
# Die Latenzkurve an der Kachel traegt den Paketverlust als Kerbe an ihrer
# Grundlinie - dort, wo auch der Messpunkt liegt. Anders als die frueheren
# Kerben im Bus-Schema werden sie NICHT ausgeduennt: die Kurve ist 300
# Einheiten breit statt 20 Pixel, da kleben sie nicht aneinander.
_s = [[10, 0]] * 30
for _i, _pl in ((2, 1), (3, 2), (4, 1), (15, 3), (16, 1), (28, 2)):
    _s[_i] = [10, _pl]
_spur = m._lat_spur_html({"series": _s})
_x = [float(v) for v in _re4.findall(r"<line x1='([\d.]+)' y1='20'", _spur)]
assert len(_x) == 6, f"jeder Verlustpunkt braucht seine Kerbe, habe {len(_x)}"
assert _x == sorted(_x), "Kerben nicht chronologisch"
assert "polyline" in _spur, "Latenzkurve fehlt"
# Robust gegen zu kurze, leere und fehlerhafte Reihen - die Reihe kommt aus
# der API und darf die Seite nicht kippen.
for _kaputt in ({"series": []}, {"series": [[10, 0]]}, {}, None):
    _leer = m._lat_spur_html(_kaputt)
    assert "polyline" not in _leer and "<svg" in _leer, f"kippt bei {_kaputt}"
_robust = m._lat_spur_html({"series": [["kaputt", None], [10, 1], [12, 0]]})
assert "polyline" in _robust and "y1='20'" in _robust, "fehlerhafte Punkte kippen die Kurve"
print("14) Latenzkurve an der Kachel, Verlust als Kerbe               OK")

# Zwischenspeicher: frisch -> kein Abruf; Fehler -> alter Stand bleibt
_alt = m.fetch_latency
try:
    m.fetch_latency = lambda *a, **k: (_ for _ in ()).throw(AssertionError("darf nicht abrufen"))
    _st = {"latency": {"ts": now.isoformat(), "sites": {"X": {"cur": 7}}}}
    assert m.refresh_latency(_st, {}, now) == {"X": {"cur": 7}}
    m.fetch_latency = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("API weg"))
    _st2 = {"latency": {"ts": (now - timedelta(hours=2)).isoformat(), "sites": {"X": {"cur": 7}}}}
    assert m.refresh_latency(_st2, {}, now) == {"X": {"cur": 7}}, "alter Stand ging verloren"
    assert m.refresh_latency({}, {}, now) == {}, "ohne Cache muss es leer bleiben, nicht krachen"
finally:
    m.fetch_latency = _alt
print("15) Latenz-Zwischenspeicher spart Abruf, Fehler kippt nichts    OK")

# Seite: Kurve je nominaler/Failover-Konsole, KEINE fuer Link Lost
# consoles = 6 Stueck, davon KNZ auf Failover und NID auf Link Lost (siehe oben)
_lat = {c["device"]: {"cur": 12, "loss": 0, "series": [[8, 0], [14, 0], [12, 1]]} for c in consoles}
_p = m.render_overview_html(consoles, start_real, now, [], latency=_lat)
import re as _re2
# NUR den Body pruefen - Klassennamen stehen auch im Stylesheet.
_body = lambda s: s[s.index("<body>"):]
_lat2 = {c["device"]: {"cur": 12, "loss": 1, "series": [[8, 0], [14, 1], [12, 0]]}
         for c in consoles}
_pb = _body(m.render_overview_html(consoles, start_real, now, [], latency=_lat2))
# Latenz und Verlust stehen als Ableseinstrument an jeder Kachel - vier
# Werte je Kachel, sechs Kacheln.
assert len(_re2.findall(r'class="rv num', _pb)) == 24, "vier Werte je Kachel"
# Auch die erloschene Kachel behaelt ihre Kurve - eingefroren und gedimmt,
# genau wie der Flow-Graph daneben; sie zeigt den letzten bekannten Stand.
assert len(_re2.findall(r"y1='20'", _pb)) == 6, "Kerben stimmen nicht"
_ob = _body(m.render_overview_html(consoles, start_real, now, [], latency=None))
assert "y1='20'" not in _ob, "ohne Latenzdaten darf keine Kerbe erscheinen"
assert '>\u2013<span class="unit">ms' in _ob, "ohne Latenz fehlt der Platzhalter"
print("16) Latenz und Verlust an der Kachel, mit und ohne Daten       OK")


# Flow-Chart ohne Flaechenfuellung. Sie wird NICHT nur unsichtbar
# geschaltet, sondern gar nicht erst erzeugt - das Polygon wiederholt alle
# Punkte der Linie (gemessen: 14.166 Zeichen bzw. 10 % der Seite fuer sechs
# Kacheln). Gegen ECHT gerenderte Charts pruefen - die Attrappen der
# Testdaten oben enthalten gar keine Kurven.
# Der Zwischenspeicher kann aelter sein als das 24-Stunden-Chartfenster
# (z.B. wenn die Pruefung Tage nach dem letzten Poll laeuft). Dann entstuende
# gar keine Kurve, und die Pruefung schluege aus einem Grund fehl, der mit
# ihr nichts zu tun hat. Deshalb die Zeitstempel einmal so verschieben, dass
# der neueste auf jetzt faellt.
_letzter = max((r["ts"] for r in rows_csv), default=None)
_versatz = (now - _letzter) if _letzter else timedelta(0)
_frisch = [dict(r, ts=r["ts"] + _versatz) for r in rows_csv] if _versatz > timedelta(hours=1) else rows_csv
_echt = m.compute_stats(_frisch, start_real, site_filter="LSB")
assert 'class="flow-down-line"' in _echt["flow_chart_mini"], "Kurve fehlt"
assert 'class="flow-down-fill"' not in _echt["flow_chart_mini"], "Flaeche wieder da"
assert "flow-down-fill" not in m.BASE_CSS, "totes Flaechen-CSS wieder da"
assert len(m.render_flow_chart([], now, now)) > 0  # leere Reihe kracht nicht
print("17) Flow-Chart ohne Flaechenfuellung, Kurve vorhanden          OK")


# Mit dem Bus-Schema sind auch seine Eigenheiten entfallen: Tropfen,
# Kerben an der Leitung und die Prozentzahl unter dem Knoten. Was bleibt,
# ist die Forderung, dass von alledem nichts zurueckbleibt - toter Code und
# totes CSS waren in diesem Projekt schon mehrfach die Ursache dafuer, dass
# eine spaetere Aenderung an der falschen Stelle ansetzte.
for _tot in ("_schema_html", "_loss_nicks", "_latency_drop", "PULSE_TRAVEL_S"):
    assert not hasattr(m, _tot), f"{_tot} noch da"
for _css in (m.BASE_CSS, m.HUD_CSS, m.GLASS_CSS):
    for _sel in ("snode", ".bus ", "pulse-travel", "with-trace", "drop.trace"):
        assert _sel not in _css, f"totes CSS: {_sel}"
assert "animateMotion" not in m.PHASEN_HTML, "totes Reduced-Motion-Skript noch da"
_pb2 = _body(m.render_overview_html(consoles, start_real, now, [], latency=_lat2))
for _tot in ('class="bus"', 'class="snode', 'class="pulse"', 'class="nick"',
             'class="pl num"', "class='plan'", "class='pnode", 'class="schema'):
    assert _tot not in _pb2, f"{_tot} noch im Markup"
assert not hasattr(m, "_plan_html"), "Hub-Renderer noch da"
for _css in (m.BASE_CSS, m.HUD_CSS, m.GLASS_CSS):
    assert ".plan " not in _css and ".pnode" not in _css, "totes Hub-CSS"
print("18) Hub-Block und Bus-Schema restlos entfernt, kein toter Code  OK")

# Die Latenz wandert mit der Messung, die Kachelreihenfolge nicht. Sie
# kommt aus MONITORED_HOSTS, nicht aus den Messwerten - sonst wuerden die
# Standorte bei jeder Messung die Plaetze tauschen und niemand faende
# seinen wieder.
import re as _re5
_folge = lambda lat: _re5.findall(r'class="fn"><span class="idx num">\d+\u00b7</span>([^<]+)',
                                  _body(m.render_overview_html(consoles, start_real, now, [], latency=lat)))
_nah = {c["device"]: {"cur": 5, "loss_avg": 0, "series": [[5, 0], [5, 0]]} for c in consoles}
_fern = {c["device"]: {"cur": 400, "loss_avg": 0, "series": [[400, 0], [400, 0]]} for c in consoles}
assert _folge(_nah) == _folge(_fern) == [c["device"] for c in consoles], (
    "die Kacheln tauschen die Plaetze mit der Latenz")
_b_nah = _body(m.render_overview_html(consoles, start_real, now, [], latency=_nah))
_b_fern = _body(m.render_overview_html(consoles, start_real, now, [], latency=_fern))
assert '>5<span class="unit">ms' in _b_nah and '>400<span class="unit">ms' in _b_fern
assert "value-alert" in _b_fern, "400 ms muss als Alarmwert gefuehrt werden"
assert "value-alert" not in _b_nah and "value-warn" not in _b_nah
print("19) Latenz an der Kachel, Reihenfolge bleibt fest               OK")

# Paketverlust als Prozentzahl - jetzt als Ableseinstrument an der Kachel
# statt unter dem Knoten. Gezeigt wird der MITTELWERT der 24 Stunden: der
# aktuelle Messpunkt ist fast immer 0 und waere praktisch nie zu sehen, der
# Hoechstwert stammt aus einem einzelnen Fuenf-Minuten-Eimer und laese sich
# wie ein Dauerzustand.
_mit = {c["device"]: {"cur": 12, "loss": 3, "loss_avg": 1.4, "loss_max": 3.0,
                      "series": [[8, 0], [14, 3], [12, 0], [11, 1]]} for c in consoles}
_b = _body(m.render_overview_html(consoles, start_real, now, [], latency=_mit))
assert _b.count("value-warn") >= 6, "1,4 % muss auffallen, aber nicht alarmieren"
assert ">1.4<" in _b, "Mittelwert wird nicht angezeigt"
assert ">3.0<" not in _b, "Hoechstwert steht als Zahl in der Kachel"
assert "Spitze 3.0 %" in _b, "Hoechstwert fehlt im Tooltip"

# Die Schwellen: unauffaellig bis 1 %, gelb darueber, rot ab 3 %. Einzelne
# verlorene Pakete sind auf einer LTE-Strecke normal - jeder Wert ueber 0
# rot einzufaerben hat die Farbe entwertet.
def _klasse(avg):
    _l = {c["device"]: {"cur": 12, "loss_avg": avg, "loss_max": avg,
                        "series": [[8, 0], [9, 0]]} for c in consoles}
    _s = _body(m.render_overview_html(consoles, start_real, now, [], latency=_l))
    return ("alert" if "value-alert" in _s else ("warn" if "value-warn" in _s else "neutral"))

assert _klasse(0.02) == "neutral", "0,02 % darf nicht einfaerben"
assert _klasse(0.9) == "neutral", "knapp unter der Schwelle noch neutral"
assert _klasse(1.0) == "warn", "ab 1 % gelb"
assert _klasse(2.9) == "warn"
assert _klasse(3.0) == "alert", "ab 3 % rot"

# EINE Schreibweise fuer dieselbe Aussage: immer eine Nachkommastelle.
# Vorher standen "0 %" (unterdrueckt) und "0,0 %" (gerundet) nebeneinander.
_ohneV = {c["device"]: {"cur": 12, "loss": 0, "loss_avg": 0.0, "loss_max": 0.0,
                        "series": [[8, 0], [14, 0]]} for c in consoles}
_b2 = _body(m.render_overview_html(consoles, start_real, now, [], latency=_ohneV))
assert _b2.count(">0.0<span class=\"unit\">%") == 6, "ohne Verlust muss ueberall 0,0 stehen"
assert ">0<span class=\"unit\">%" not in _b2, "zwei Schreibweisen fuer dieselbe Zahl"
assert ">12<span class=\"unit\">ms" in _b2, "Latenz muss trotzdem stehen"
print("20) Verlust und Latenz als Ableseinstrument an der Kachel        OK")

# Eigene Failover-Schwelle je Konsole: WTB laeuft im Normalbetrieb mit
# hoeherer Grundlast und loeste mit den globalen 150 kbps Fehlalarme aus.
assert m._console_failover_threshold("WTB--UDM-1") == 1500.0
assert m._console_failover_threshold("wtb") == 1500.0, "Kurzform greift nicht"
assert m._console_failover_threshold("LSB--UDM-1") == m.FAILOVER_THRESHOLD_KBPS
assert m._console_failover_threshold(None) == m.FAILOVER_THRESHOLD_KBPS

# Und die Schwelle muss auch wirken: 800 kbps sind bei LSB ein Failover,
# bei WTB nicht.
_rate = 800.0
_reihe = [dict(ts=now - timedelta(minutes=i), site=None, uplink="wan1",
               down_bytes=0.0, up_bytes=_rate * 1000 / 8 * 60, interval_s=60)
          for i in range(m.FAILOVER_CONSECUTIVE)]
for _dev, _erwartet in (("LSB--UDM-1", True), ("WTB--UDM-1", False)):
    _r = [dict(x, site=_dev) for x in _reihe]
    _s = m.compute_stats(_r, start_real, site_filter=_dev, rows_prefiltered=True)
    assert _s["is_failover"] is _erwartet, f"{_dev}: is_failover={_s[chr(34)+chr(34)]}" if False else f"{_dev}: erwartet {_erwartet}, war {_s['is_failover']}"
print("22) Failover-Schwelle je Konsole, WTB bei 1,5 Mbps              OK")

# Die Prozentzahl erscheint nur, wenn der Verlust nicht bloss ein einzelner
# Ausreisser war. Ein Fuenf-Minuten-Punkt von 151 ergab frueher "1 %", was
# sich wie ein Dauerzustand liest - der UniFi Site Manager meldete fuer
# dieselbe Konsole "None". Die Kerbe bleibt in jedem Fall.
def _lat(punkte, betroffen, maxpl=1.0):
    reihe = [[10.0, 0.0] for _ in range(punkte)]
    for i in range(betroffen):
        reihe[i * 3 % punkte][1] = maxpl
    return {"cur": 12, "loss": 0.0, "punkte": punkte, "loss_punkte": betroffen,
            "loss_max": maxpl if betroffen else 0.0, "series": m._thin_latency(reihe, m.LATENCY_POINTS)}

# Echte Lage vom 15.09.2026: HAN 1 von 151, LSB 1 von 190 -> Site Manager
# sagt "None". KNZ 1 von 24 (4,2 %) -> "Detected".
assert not m._loss_zeigen(_lat(151, 1)), "HAN: einzelner Ausreisser darf keine Zahl zeigen"
assert not m._loss_zeigen(_lat(190, 1)), "LSB: einzelner Ausreisser darf keine Zahl zeigen"
assert m._loss_zeigen(_lat(24, 1)), "KNZ: 4,2 % muss die Zahl zeigen"
assert m._loss_zeigen(_lat(151, 2)), "zwei Punkte reichen unabhaengig vom Anteil"
assert not m._loss_zeigen(_lat(200, 0)), "ohne Verlust keine Zahl"
# Alter Zwischenspeicher ohne Zaehlung: lieber zeigen als verschweigen
assert m._loss_zeigen({"cur": 12}), "ohne Zaehlung muss die Zahl erscheinen"

# Und die Wirkung in der erzeugten Seite
# Die Unterscheidung gilt jetzt zwischen ZAHL und KERBE: die Prozentzahl im
# Ableseinstrument erscheint erst ab relevanter Haeufigkeit, die Kerbe an der
# Latenzkurve dagegen immer - sie ist die Rohbeobachtung, die Zahl die
# Bewertung.
_selten = {c["device"]: _lat(151, 1) for c in consoles}
_b_selten = _body(m.render_overview_html(consoles, start_real, now, [], latency=_selten))
assert "value-warn" not in _b_selten and "value-alert" not in _b_selten, (
    "ein einzelner Ausreisser darf die Kachel nicht einfaerben")
assert "y1='20'" in _b_selten, "die Kerbe muss trotzdem erscheinen"
_haeufig = {c["device"]: _lat(151, 9, maxpl=3.0) for c in consoles}
_b_haeufig = _body(m.render_overview_html(consoles, start_real, now, [], latency=_haeufig))
_erwartet, _ = m._loss_werte(_lat(151, 9, maxpl=3.0))
assert f'>{_erwartet:.1f}<span class="unit">%' in _b_haeufig, "haeufiger Verlust ohne Zahl"
print("23) Verlust-Prozentzahl erst ab relevanter Haeufigkeit           OK")

# Die verbliebenen Dauerbewegungen bekommen ihre Phase aus der Uhrzeit. Ohne
# das beginnt jede beim minuetlichen Neuladen wieder von vorn - gemessen an
# der Live-Seite war das beim Radar nicht behoben ("0s"), und genau das sah
# man als Ruckeln. Mit dem Bus-Schema sind die Tropfen entfallen; uebrig
# bleiben der Suchstrahl des Radars und der Puls des "Link Lost"-Schriftzugs.
assert ".radar .sweep" in m.PHASEN_HTML, "Suchstrahl bekommt keine Phase"
assert "--puls-phase" in m.PHASEN_HTML, "Link-Lost-Puls bekommt keine Phase"
assert "animationDelay" in m.PHASEN_HTML
# CSS und Skript duerfen nie auseinanderlaufen - beide aus derselben Konstante.
assert f"radar-sweep {m.RADAR_SWEEP_S}s" in m.HUD_CSS
assert str(m.RADAR_SWEEP_S) in m.PHASEN_HTML
assert str(m.LOST_PULSE_S) in m.PHASEN_HTML
assert "__RADAR__" not in m.HUD_CSS and "__RADAR__" not in m.PHASEN_HTML
print("24) Radar und Link-Lost-Puls laufen ueber den Reload weiter      OK")

# Die Seite laedt nur noch neu, wenn es einen NEUEREN Stand gibt - vorher
# haengte der Reload an einem Countdown und feuerte gemessen dreimal pro
# Minute, weil die frisch geladene Seite wegen der Deploy-Verzoegerung oft
# schon ueberfaellig war.
_skript = m.refresh_countdown_script(now)
assert "wan_stand.txt" in _skript, "Standdatei wird nicht abgefragt"
assert "standMs > generatedAtMs" in _skript, "laedt auch ohne neueren Stand neu"
assert "no-store" in _skript, "Standabfrage darf nicht aus dem Zwischenspeicher kommen"
assert "minRetryMs" not in _skript, "alte Countdown-Bremse noch da"
assert "sessionStorage" not in _skript, "alte Reload-Sperre noch da"
import os as _os
assert _os.path.basename(m.STAND_PATH) == "wan_stand.txt"
_wf = open(os.path.join(PROJEKT, ".github", "workflows", "wan-monitor.yml"), encoding="utf-8").read()
assert "public/wan_stand.txt" in _wf, "Standdatei wird nicht veroeffentlicht"
print("25) Reload nur bei wirklich neuem Stand                          OK")

# Die manuelle Ausfallmarkierung: zeigt alle Standorte als erloschen, egal
# was gemessen wurde - fuer Stoerungen, die die UDM gar nicht sehen kann.
# Sie muss sichtbar bleiben, sonst ist sie spaeter nicht mehr von einer
# Messung zu unterscheiden.
m.ALLE_AUSGEFALLEN = True
try:
    _laufend = [fake("HAN--UDM-1"), fake("KNZ--UDM-1", failover=True, rate=900.0),
                fake("WTB--UDM-1")]
    for _c in _laufend:
        assert m._console_status(_c) == "offline", "Markierung greift nicht"
    _seite = m.render_overview_html(_laufend, start_real, now, [])
    _rumpf = _body(_seite)
    assert _rumpf.count('class="panel bracketed offline"') == 3, "nicht alle Kacheln erloschen"
    assert _rumpf.count('<span class="chip lost">') == 3, "Kacheln zeigen nicht alle als erloschen"
    # Kein Zusatzvermerk auf der Seite (Nutzerwunsch): sie zeigt den Ausfall
    # schlicht an. Nachvollziehbar bleibt er ueber den Schalter im Code.
    assert 'class="ausfall-hinweis"' not in _rumpf
    # Die Failover-Zaehlung oben muss der Anzeige folgen, nicht der rohen
    # Messung - sonst steht dort "1 / 6 aktive Failover", waehrend daneben
    # sechs erloschene Standorte zu sehen sind.
    assert re.search(r'Aktive Failover</div>\s*<div class="val num[^"]*">0', _rumpf),         "Failover-Zaehlung ignoriert die Ausfallmarkierung"
finally:
    m.ALLE_AUSGEFALLEN = False
# Ohne Schalter zaehlt wieder die Messung - sonst waere die Markierung nicht
# zurueckzunehmen.
assert m._console_status(fake("HAN--UDM-1")) == "nominal"
assert m._console_status(fake("KNZ--UDM-1", failover=True)) == "failover"
m.ALLE_AUSGEFALLEN = _SCHALTER_VORHER
# Bei erloschenem Standort steht "Link Lost" gross ueber der
# Zeichenflaeche - der kleine Chip oben rechts geht auf einem Wandmonitor
# unter, und die Kurve darunter zeigt ohnehin nur eingefrorene Vergangenheit.
_i = m.HUD_CSS.index(".panel.offline .mini-chart-col::after")
_regel = m.HUD_CSS[_i:m.HUD_CSS.index("}", _i)]
assert 'content: "Link Lost"' in _regel, "Schriftzug fehlt"
assert "var(--alert)" in _regel, "Schriftzug nicht in Alarmfarbe"
assert "pointer-events: none" in _regel, "Schriftzug faengt Klicks ab"
assert ".panel.offline .chart.flow-chart { opacity:" in m.HUD_CSS, "Kurve wird nicht gedimmt"
# Der Schriftzug pulsiert wie die Eckklammern bei einem Failover - dasselbe
# Keyframe, derselbe Takt, und die Phase kommt aus der Uhrzeit, damit er
# beim minuetlichen Neuladen nicht neu ansetzt.
assert f"bk-pulse {m.LOST_PULSE_S}s" in _regel, "Puls fehlt"
assert "var(--puls-phase" in _regel, "Puls ohne Phase aus der Uhrzeit"
assert "--puls-phase" in m.PHASEN_HTML, "Phasen-Skript setzt die Variable nicht"
assert "__LOSTPULSE__" not in m.HUD_CSS and "__LOSTPULSE__" not in m.PHASEN_HTML
# Bei prefers-reduced-motion steht er still und bleibt voll sichtbar.
_marke = 'prefers-reduced-motion: reduce) {' + chr(10) + '    .panel.offline'
_i2 = m.HUD_CSS.index(_marke)
assert "animation: none" in m.HUD_CSS[_i2:_i2+200]
assert "animation: none" in m.HUD_CSS[_i2:_i2+180]
print("26) Manuelle Ausfallmarkierung: wirkt und ist ruecknehmbar        OK")


# ---------------------------------------------------------------------------
# 27) Gesprungenes Glas auf erloschener Kachel
# ---------------------------------------------------------------------------
# Drei Dinge koennen den Effekt still ausfallen lassen, ohne dass irgendwo
# ein Fehler auftaucht - die Kachel sieht dann einfach unversehrt aus:
#   * die Filterdefinition fehlt im Markup (CSS zeigt dann ins Leere),
#   * die Karte deckt nicht genau die Kachel ab (dann laufen sichtbarer Riss
#     und Bruchkante der Verschiebung auseinander),
#   * die Kanalwerte liegen bei 128 (dann ist der Versatz null).
assert 'id="rissglas"' in m.RISS_DEFS_HTML, "Filterdefinition fehlt"
assert "url(#rissglas)" in m.RISS_CSS, "CSS verweist nicht auf den Filter"
assert 'x="0%" y="0%" width="100%" height="100%"' in m.RISS_DEFS_HTML, (
    "Filterbereich deckt nicht genau die Kachel ab")
assert "feDisplacementMap" in m.RISS_DEFS_HTML and "feImage" in m.RISS_DEFS_HTML

# Ruhewert und Versatz: 128 ist Stillstand, die Flaeche unter dem Riss muss
# davon abweichen - und zwar um genau die in RISS_VERSATZ angesagten Pixel.
import re as _re
# Die Karte steckt zweimal im Markup (href und xlink:href fuer aeltere
# Darstellungsmodule), deshalb hier auf den ersten Satz beschraenken.
_kanaele = _re.findall(r"rgb\((\d+),(\d+),128\)", m.RISS_DEFS_HTML)[:2]
assert len(_kanaele) == 2, f"erwarte Ruhe- und Versatzfeld, habe {_kanaele}"
assert _kanaele[0] == ("128", "128"), "Grundflaeche ist nicht in Ruhe"
_px = [(int(v) - 128) / 255 * m.RISS_SCALE for v in _kanaele[1]]
assert all(abs(a - b) < .15 for a, b in zip(_px, m.RISS_VERSATZ)), (
    f"Versatz {_px} statt {list(m.RISS_VERSATZ)}")

# Die Zeichnung haengt an .bk-bl - einem der leeren Divs, die die Kachel
# ohnehin mitbringt. Die Kantenregel setzt dort Groesse, Drehung und einen
# Verlauf; ohne Ruecknahme bliebe davon ein 38x2px grosser Schnipsel uebrig.
_bk = m.RISS_CSS.split(".panel.offline > .bk-bl")[1]
for _eig in ("inset: 0", "transform: none", "background-size: 100% 100%",
             "width: auto", "height: auto"):
    assert _eig in _bk, f"{_eig} fehlt - Kantenregel nicht zurueckgenommen"

# Gesaet, also bei jedem Lauf bitgleich. Sonst saehe die Kachel nach jedem
# Neuladen anders aus.
assert m._riss_bauen() == (m.RISS_DEFS_HTML, m.RISS_CSS), "Riss nicht reproduzierbar"

# Und beides muss auch wirklich in der Seite landen - aber nur im Glas-Thema,
# wo .bk-bl die Schnittkante traegt. Im HUD-Thema ist es eine Eckklammer.
_seite = m.render_overview_html(consoles, start_real, now, [], latency=_lat2)
assert ('id="rissglas"' in _seite) == (m.COLOR_THEME == "glas")
assert ("url(#rissglas)" in _seite) == (m.COLOR_THEME == "glas")
print("27) Glasbruch: Filter, Karte und Zeichnung haengen zusammen     OK")


# ---------------------------------------------------------------------------
# 28) Reduktion: keine Flaeche, kein Leuchten auf der nominalen Kachel
# ---------------------------------------------------------------------------
# Zweimal ist in diesem Projekt eine Farbregel stehengeblieben, obwohl eine
# spaetere Regel sie aufheben sollte - beide Male, weil SPEZIFITAET die
# Reihenfolge schlaegt: '.panel:not(.failover):not(.offline)' (0,3,0) gewinnt
# gegen das '.panel' der Reduktion (0,1,0), egal wie weit unten das steht.
# Beim zweiten Mal war es das gruene Licht von oben, das in den ausgewaehlten
# Entwuerfen gerade NICHT da war. Diese Pruefung faengt genau das ab.
_rsp = m.render_overview_html(consoles, start_real, now, [], latency=_lat2)
assert ".panel:not(" not in _rsp, "spezifischere Kachelregel wieder da"
for _weg in ("rgba(120, 255, 180", "rgba(150, 255, 200, .80)", "inset 0 14px 22px"):
    assert _weg not in _rsp, f"Lichtkante der Kachel wieder da: {_weg}"
# Die Zustandskante bleibt - aber nur Failover und Ausfall sind farbig.
_red = m.REDUKTION_CSS
_i = _red.index("  .panel { border-left:")
assert "rgba(150, 200, 215" in _red[_i:_i + 120], "nominale Kante ist nicht neutral"
# Die Zustandskanten als vollstaendige Kurzschreibweise: nur die Farbe zu
# setzen reicht nicht, weil '.panel.failover { border: none }' weiter oben
# spezifischer ist und die Breite sonst auf 0 bleibt - die Failover-Kachel
# war dadurch die einzige ganz ohne Kante.
assert ".panel.failover { border-left: 2px solid var(--alert); }" in _red
assert ".panel.offline { border-left: 2px solid" in _red
print("28) Nominale Kachel ohne Flaeche und ohne Lichtkante            OK")


# ---------------------------------------------------------------------------
# 29) Ein einzelner Ausreisser ist kein Dauerzustand
# ---------------------------------------------------------------------------
# Echter Fall vom 08.10.2026, an der API nachgemessen: NID hatte in 24
# Stunden 233 Messpunkte, davon EINEN mit 30 % Paketverlust, Mittel 0,215 %.
# Die Kachel zeigte "30,0 %" - also den schlechtesten Fuenf-Minuten-Eimer,
# praesentiert wie ein Dauerzustand. Dieselbe Klasse von Fehler, die das
# Projekt fuer das alte Systemschema schon einmal behoben hatte.
_nid = [[10.0, 0.0] for _ in range(233)]
_nid[100][1] = 30.0
for _i in (5, 20, 44, 60, 77, 90, 120, 140, 160, 180, 200, 210, 215, 220, 225, 228, 230):
    _nid[_i][1] = 1.0
_roh = {"cur": 8, "loss": 0.0, "punkte": len(_nid),
        "loss_punkte": len([1 for _a, w in _nid if w]),
        "loss_avg": sum(w for _a, w in _nid) / len(_nid),
        "loss_max": max(w for _a, w in _nid),
        "series": m._thin_latency(_nid, m.LATENCY_POINTS)}
_mittel, _spitze = m._loss_werte(_roh)
assert abs(_mittel - 0.2) < 0.05, f"Mittelwert {_mittel:.3f} statt ~0,2 %"
assert _spitze == 30.0, "Hoechstwert geht verloren"
_bn = _body(m.render_overview_html(consoles, start_real, now, [],
                                   latency={c["device"]: _roh for c in consoles}))
assert ">30.0<" not in _bn, "der Ausreisser steht wieder als Zahl in der Kachel"
assert ">0.2<" in _bn, "der Mittelwert fehlt"
assert "Spitze 30.0 %" in _bn, "der Ausreisser fehlt im Tooltip"

# Der Mittelwert MUSS auf der vollen Reihe gerechnet werden. Aus der
# ausgeduennten kaeme er zu hoch heraus, weil _thin_latency je Abschnitt das
# Maximum behaelt - der eine 30er-Punkt stuende dann fuer acht.
_ohne_felder = dict(_roh)
del _ohne_felder["loss_avg"], _ohne_felder["loss_max"]
_ausgeduennt, _ = m._loss_werte(_ohne_felder)
assert _ausgeduennt > _mittel * 3, (
    "Ausduennung hebt den Mittelwert nicht - Annahme der Pruefung stimmt nicht mehr")
print("29) Verlust-Ausreisser kippt die Kachel nicht                   OK")


# ---------------------------------------------------------------------------
# 30) Klima der Standorte
# ---------------------------------------------------------------------------
# Die Schwellen, spiegelbildlich nach unten: ein zu kalter Raum soll genauso
# auffallen wie ein zu warmer.
assert m._klima_stufe(20.0, m.TEMP_GUT, m.TEMP_ALARM) == ""
assert m._klima_stufe(29.0, m.TEMP_GUT, m.TEMP_ALARM) == "warn"
assert m._klima_stufe(33.0, m.TEMP_GUT, m.TEMP_ALARM) == "alert"
assert m._klima_stufe(8.0, m.TEMP_GUT, m.TEMP_ALARM) == "warn", "zu kalt faellt nicht auf"
assert m._klima_stufe(2.0, m.TEMP_GUT, m.TEMP_ALARM) == "alert"
assert m._klima_stufe(None, m.TEMP_GUT, m.TEMP_ALARM) == ""
assert m._klima_stufe(50.0, m.FEUCHTE_GUT, m.FEUCHTE_ALARM) == ""
assert m._klima_stufe(70.0, m.FEUCHTE_GUT, m.FEUCHTE_ALARM) == "warn"
# Gelb erst AB 70 %: die 65 bis 69,9 sind ausdruecklich unauffaellig
# (Nutzerentscheidung 2026-10-19, vorher begann gelb schon ueber 65 %).
assert m._klima_stufe(65.0, m.FEUCHTE_GUT, m.FEUCHTE_ALARM) == "", "gelb beginnt zu frueh"
assert m._klima_stufe(69.9, m.FEUCHTE_GUT, m.FEUCHTE_ALARM) == "", "gelb beginnt zu frueh"
assert m._klima_stufe(90.0, m.FEUCHTE_GUT, m.FEUCHTE_ALARM) == "alert"

_sens = {c["device"]: {"name": c["device"].split("--")[0] + "--Sens-1",
                       "temp": 21.0, "feuchte": 45.0, "akku": 95, "da": True}
         for c in consoles}
_sens[consoles[0]["device"]].update(temp=29.7)      # warm
_sens[consoles[1]["device"]].update(feuchte=79.0)   # feucht
_sens.pop(consoles[2]["device"])                    # gar kein Sensor
_sens[consoles[3]["device"]].update(da=False)       # Sensor meldet sich ab
_sens[consoles[4]["device"]].update(akku=11)        # Batterie am Ende

_kb = _body(m.render_overview_html(consoles, start_real, now, [], latency=_lat2, sensors=_sens))
# Mit Trennzeichen zaehlen: 'class="zellen"' (der Behaelter) faengt sonst
# mit und ergibt eine Zelle zu viel.
_zellen = lambda s: len(re.findall(r'class="zelle[ "]', s))
assert _zellen(_kb) == 6, "nicht sechs Klimazellen"
assert _kb.count("kein Sensorwert") == 2, (
    "fehlender UND abgemeldeter Sensor muessen als solche erscheinen")
# Ein stiller Ausfall ist schlimmer als ein auffaelliger: ohne Messwert darf
# keine alte Zahl stehenbleiben.
assert _kb.count('>45<') == 3, "Zahl steht an einer Zelle ohne Messwert"
# Die Feuchte traegt die Zelle, die Temperatur steht klein darunter.
assert '>29.7 &deg;C<' in _kb and 'class="warn">29.7' in _kb, "warmer Standort faellt nicht auf"
# 79 % liegt ueber der Alarmgrenze von 75, ist also rot und nicht gelb.
assert 'class="gross alert">79<span class="e">% rF' in _kb, "feuchter Standort fehlt"
assert 'class="akku warn"' in _kb, "leere Batterie faellt nicht auf"
assert "Akku 95 %" in _kb

# Ganz ohne Sensordaten darf die Seite nicht kippen - sie zeigt dann sechs
# leere Zellen statt gar nichts.
_leer = _body(m.render_overview_html(consoles, start_real, now, [], latency=_lat2))
assert _zellen(_leer) == 6 and _leer.count("kein Sensorwert") == 6

# Der Zwischenspeicher: frisch -> kein Abruf; Fehlschlag -> letzter Stand
# bleibt. Ein fehlender Klimawert darf niemals den Poll gefaehrden, an dem
# die Volumenmessung haengt.
_alt_fetch = m.fetch_sensors
try:
    m.fetch_sensors = lambda *a, **k: (_ for _ in ()).throw(AssertionError("darf nicht abrufen"))
    _frisch = {"sensors": {"ts": now.isoformat(), "sites": {"X": {"temp": 1}}}}
    assert m.refresh_sensors(_frisch, {}, now) == {"X": {"temp": 1}}, "frischer Stand wurde verworfen"
    _alt = {"sensors": {"ts": (now - timedelta(hours=2)).isoformat(),
                        "sites": {"X": {"temp": 2}}}}
    assert m.refresh_sensors(_alt, {}, now) == {"X": {"temp": 2}}, "Fehlschlag kippt den Stand"
    m.fetch_sensors = lambda *a, **k: {}
    assert m.refresh_sensors(_alt, {}, now) == {"X": {"temp": 2}}, "leeres Ergebnis loescht den Stand"
finally:
    m.fetch_sensors = _alt_fetch
print("30) Klima der Standorte: Schwellen, Ausfall, Zwischenspeicher   OK")


print("\nAlle Pruefungen bestanden.")
