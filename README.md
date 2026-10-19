# WAN-Volumen der UDM Pro messen

Misst, wieviel Datenvolumen das Netzwerksetup ohne Clients ueber den WAN-Uplink
erzeugt. Quelle sind die ISP-Metriken der UniFi Site Manager API. Der Bericht ist
schon nach dem ersten Poll nutzbar und wird alle 15 Minuten fortgeschrieben.

## 1. API-Key setzen

Den im Chat geposteten Key unter unifi.ui.com bei Account, API loeschen und einen
neuen erzeugen. Der neue Key wird nur lokal gesetzt, nie in eine Datei geschrieben.

    Linux / macOS      export UI_API_KEY='dein-neuer-key'
    PowerShell         $env:UI_API_KEY='dein-neuer-key'

Fuer den Dauerbetrieb dauerhaft hinterlegen: unter Linux in `~/.profile`, unter
Windows per `setx UI_API_KEY "dein-neuer-key"` und Terminal neu oeffnen.

## 2. Einmalig pruefen

    python3 udm_wan_monitor.py --discover

Zeigt Hosts, Sites und ein Rohdaten-Sample und legt `raw_sample.json` an. Wichtig
ist die Zeile "Erkannte Messpunkte". Steht dort 0, schick mir `raw_sample.json`,
dann passe ich den Parser an das tatsaechliche Schema deiner UniFi-OS-Version an.

## 3. Messung starten

Fenster: heute 12:00 Uhr, Laufzeit drei Tage.

    python3 udm_wan_monitor.py --loop --start 2026-08-07T12:00 --days 3

Das Fenster wird in `monitor_state.json` gemerkt, spaetere Aufrufe brauchen
`--start` nicht mehr. Die API liefert rueckwirkend Daten, ein Start um 14 Uhr
holt die Stunden ab 12 Uhr also automatisch nach.

Optionen: `--interval 900` fuer das Pollintervall, `--site "Name"` wenn dein Key
mehrere Sites sieht, `--once` fuer einen einzelnen Durchlauf.

## 4. Bericht ansehen

`wan_report.html` im selben Ordner, einfach im Browser oeffnen. Die Seite laedt
sich alle fuenf Minuten selbst neu, du kannst den Tab also offen liegen lassen.
Sie zeigt bisher gemessenes Volumen, laufenden Tagesmittelwert, Hochrechnung auf
30 Tage, das Stundenraster und die groessten Stunden.

Ohne API-Zugriff neu bauen, etwa nach dem Ende der Messung:

    python3 udm_wan_monitor.py --report

## 5. Als Dienst statt im Terminal

systemd Timer (Linux), Datei `/etc/systemd/system/udm-wan.service`:

    [Service]
    Type=oneshot
    Environment=UI_API_KEY=dein-neuer-key
    ExecStart=/usr/bin/python3 /pfad/udm_wan_monitor.py --once

Dazu ein Timer mit `OnCalendar=*:0/15`.

Windows Aufgabenplaner: Programm `python.exe`, Argumente
`C:\pfad\udm_wan_monitor.py --once`, Trigger alle 15 Minuten.

## Dateien

    wan_traffic.csv        alle Messpunkte, dedupliziert, wachsend
    wan_report.html        aktueller Bericht
    monitor_state.json     gemerktes Messfenster
    raw_sample.json        Rohantwort des ersten Polls, nur zur Diagnose

## Einordnung der Ergebnisse

Da am Standort keine Clients haengen, ist das gemessene WAN-Volumen im Kern die
Cloud-Anbindung der UDM plus die Statusmeldungen der Switche. Einzelne Stunden
mit deutlich hoeherem Volumen stammen in der Regel von Speedtests oder von
Signatur- und Firmware-Downloads. Fuer die reine Grundlast diese Stunden aus der
Tageswertetabelle herausrechnen. Der Speedtest laesst sich in den
UniFi-Netzwerkeinstellungen unter Internet abschalten oder auf taeglich stellen.

## Gegenprobe lokal

Wenn du die API-Werte gegenpruefen willst, per SSH auf der UDM Pro fuer den
nativen RJ45-WAN-Port:

    cat /proc/net/dev | grep eth8

RX- und TX-Bytes zu Beginn und am Ende notieren, Differenz bilden. Vorher mit
`ip -br addr` bestaetigen, dass eth8 die oeffentliche IP traegt. Der SFP+-WAN-Port
waere eth9, den brauchst du hier nicht.
