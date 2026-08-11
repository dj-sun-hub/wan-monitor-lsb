# Uebergabe an Claude Code

Diese Datei enthaelt den kompletten Kontext. Leg sie in den Projektordner, dann
muss nichts erneut erklaert werden.

## Ziel

Ermitteln, wieviel Datenvolumen das Netzwerksetup am Standort pro Monat ueber
den WAN-Uplink erzeugt. Am Standort haengen keine Clients, gemessen wird also
faktisch nur die Cloud-Anbindung der UDM und die Statusmeldungen der Switche.
Firmware- und Signatur-Updates sollen nicht mitgerechnet, sondern als Ausreisser
sichtbar gemacht werden.

## Umgebung

Gateway: UniFi Dream Machine Pro, Konsolenname LSB--UDM-1
WAN: nativer RJ45-WAN-Port, intern eth8. Der SFP+-WAN-Port waere eth9.
Weitere Geraete: rund 15 Switche, verwaltet ueber den UniFi Site Manager
Vor Ort ausserdem: Sophos XGS 2300 als Firewall, UniFi 5G Max als LTE-Backup

## Messvorgaben

Beginn: 07.08.2026, 12:00 Uhr lokale Zeit
Dauer: 3 Tage
Pollintervall: 15 Minuten
Zwischenergebnisse sind Pflicht, nicht erst eine Auswertung am Ende.

## Datenquelle

UniFi Site Manager API, https://api.ui.com/ea
Endpunkte: /isp-metrics/5m?duration=24h und /isp-metrics/1h?duration=7d
Authentifizierung ueber Header X-API-KEY.
Die API liefert rueckwirkend, ein spaeterer Start holt vergangene Stunden nach.
5-Minuten-Werte sind mindestens 24 Stunden verfuegbar, Stundenwerte mindestens
30 Tage.

## Umgang mit dem API-Key

Der Key wird ausschliesslich als Umgebungsvariable UI_API_KEY gesetzt. Er darf
nicht in eine Datei, nicht ins Repository, nicht in ein Skript und nicht in eine
Chatnachricht geschrieben werden. Der Nutzer setzt ihn selbst. Ein frueher
verwendeter Key wurde kompromittiert und ist zu ersetzen.

## Vorhandene Dateien

udm_wan_monitor.py         Collector plus HTML-Berichtsgenerator, nur Stdlib
wan-monitor.yml            GitHub-Actions-Workflow fuer den Betrieb ohne Rechner
SETUP-GITHUB.md            Einrichtung des Actions-Wegs
README.md                  Bedienung des Skripts
vorschau_beispieldaten.html  Layoutvorschau mit synthetischen Daten

Das Skript kann: --discover, --once, --loop, --report
Es schreibt wan_traffic.csv, wan_report.html, monitor_state.json.

## Offener Punkt

Der Parser ist schema-tolerant, aber ungetestet gegen die echte API-Antwort.
Erster Schritt ist deshalb `--discover`. Wenn dort null erkannte Messpunkte
stehen, muss der Parser an raw_sample.json angepasst werden.

## Auftrag an Claude Code

Der Nutzer kennt sich mit GitHub nicht aus und moechte moeglichst wenig selbst
einrichten. Gewuenscht ist:

1. Pruefen, ob UI_API_KEY gesetzt ist, sonst den Nutzer bitten, ihn zu setzen.
2. `--discover` ausfuehren und die Antwortstruktur pruefen.
3. Falls noetig, den Parser in udm_wan_monitor.py an das tatsaechliche Schema
   anpassen und mit den Rohdaten verifizieren.
4. Den Nutzer waehlen lassen, wo die Messung laufen soll, und den gewaehlten Weg
   vollstaendig einrichten:
   a) GitHub Actions ohne laufenden Rechner. Repository, Secret und Pages per
      gh CLI anlegen, den Workflow committen, einen Testlauf ausloesen und dem
      Nutzer die fertige URL nennen. Alles, was Anmeldung oder das Eintragen des
      Secrets erfordert, macht der Nutzer selbst, Claude fuehrt ihn dort hin.
   b) Alternativ auf einem vorhandenen Server oder einer VM des Nutzers per
      systemd-Timer.
   c) Alternativ lokal per --loop, wenn der Rechner ohnehin durchlaeuft.
5. Nach dem ersten erfolgreichen Lauf pruefen, ob der Bericht plausible Werte
   zeigt, und die Zwischenauswertung mit dem Nutzer durchgehen.
6. Nach drei Tagen Grundlast und Ausreisser trennen und den Monatswert nennen.

## Was Claude nicht tut

Passwoerter, API-Keys oder Tokens entgegennehmen oder eintragen. Accounts
anlegen. Sicherheitseinstellungen aendern. Der Nutzer erledigt diese Schritte
selbst, Claude bereitet sie vor und erklaert sie.
