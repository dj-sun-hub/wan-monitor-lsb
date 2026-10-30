# Einrichtung auf dem Leitstand-PC

Der Monitor lief bis Ende Oktober 2026 in GitHub Actions, ausgelöst von
cron-job.org, veröffentlicht über GitHub Pages. Drei Fehler in vier Tagen
hatten alle dieselbe Form: die Messwerte waren vollständig da, aber die
Veröffentlichung blieb hängen — einmal durch einen selbstverschuldeten
Livelock, einmal durch ein hängendes Pages-Deployment, einmal durch einen
Lauf, den GitHub nicht mehr beseitigen ließ. Die Seite stand still, obwohl
nichts fehlte.

Auf dem Thin Client am Leitstand-Bildschirm fällt die ganze Kette weg: kein
Auslöser von außen, kein Build, kein Deployment. Das Skript läuft, schreibt
die Seite daneben, der Browser zeigt sie.

---

## Was der Rechner braucht

| | |
|---|---|
| **Python** | 3.9 oder neuer, Standardinstallation. Keine Pakete — das Skript nutzt ausschließlich die Standardbibliothek |
| **Internet** | ausgehend https 443 zu `api.ui.com`. Nichts eingehend, kein offener Port |
| **Platz** | rund 50 MB. Die Messdatei wächst etwa 3,5 MB in drei Monaten |
| **Konto** | `j.eismann` (seit 30.10.2026). Der Monitor braucht keine Domänenrechte — er redet mit der UniFi-Cloud und schreibt Dateien |

> **Zum Konto:** Der Bildschirm lief anfangs unter einem
> Domänen-Administrator — dauerhaft angemeldet, an einem Bildschirm in einem
> Flur. Das ist am 30.10.2026 auf ein gewöhnliches Benutzerkonto geändert
> worden. Damit ist das Wesentliche erledigt.
>
> Zwei Punkte bleiben, weil es ein persönliches und kein Dienstkonto ist: ein
> Kennwortwechsel oder eine Sperrung hält den Monitor an, und die
> Schlüsseldatei ist an das Konto gebunden, also überall dort lesbar, wo
> dasselbe Profil angemeldet ist. Beides ist vertretbar; ein eigenes
> Dienstkonto wäre die saubere Form, falls der Leitstand dauerhaft bleibt.

## Verzeichnis

```
C:\WAN-Monitor\
    udm_wan_monitor.py      das Skript
    wan_traffic.csv         Messwerte (wächst)
    monitor_state.json      Zustand, Ereignisprotokoll, Zwischenspeicher
    wan_report.html         die Seite, wird jede Minute neu geschrieben
    wan_stand.txt           Zeitstempel für die Neuladeprüfung der Seite
```

Rechte: Vollzugriff nur für `j.eismann` und Administratoren.

## Der API-Schlüssel

Das Skript erwartet ihn in der Umgebungsvariablen `UI_API_KEY`.

Eine Aufgabe in der Windows-Aufgabenplanung bringt keine benutzerbezogene
Umgebung mit. Die Variable maschinenweit zu setzen wäre der bequeme Weg und
der falsche: sie stünde im Klartext in der Registrierung, für jeden lokalen
Administrator lesbar, und in der Prozessumgebung für jeden, der Prozesse
ausliest.

Stattdessen eine DPAPI-verschlüsselte Datei, nach dem Muster, das im
Schwesterprojekt WAC für `starface.xml` verwendet wird. Verschlüsselt an
Konto **und** Maschine gebunden: die Datei ist auf einem anderen Rechner
oder unter einem anderen Konto wertlos.

Anlegen, einmalig, **angemeldet als das Sitzungskonto auf dem
Leitstand-PC** (DPAPI bindet die Datei an das Konto — unter einem anderen
Konto angelegt ist sie dort wertlos):

```powershell
# Fragt den Schlüssel ab, ohne ihn auf den Bildschirm zu schreiben
$key = Read-Host -AsSecureString "UniFi API-Schlüssel"
$key | ConvertFrom-SecureString | Set-Content C:\WAN-Monitor\schluessel.xml
```

Die DPAPI-Runde (verschlüsseln, zurücklesen, Klartext vergleichen) ist auf
einem Windows-Rechner mit einem Probewert geprüft. Die Aufgabenplanung, der
Kioskbrowser und die Sicherung auf die Freigabe sind **nicht** erprobt — sie
stehen hier als Vorlage und gehören beim Einrichten einmal durchgespielt.

Der Starter liest sie und übergibt sie als Umgebungsvariable an das Skript —
nur in dessen Prozess, nicht maschinenweit:

```powershell
# C:\WAN-Monitor\starte-monitor.ps1
$sicher = Get-Content C:\WAN-Monitor\schluessel.xml | ConvertTo-SecureString
$env:UI_API_KEY = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sicher))
Set-Location C:\WAN-Monitor
python udm_wan_monitor.py --loop --serve
```

## Aufgabenplanung

Eine Aufgabe, die den Starter bei der Anmeldung startet:

| Feld | Wert |
|---|---|
| Trigger | Bei Anmeldung von `j.eismann` |
| Aktion | `powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:\WAN-Monitor\starte-monitor.ps1` |
| Ausführen als | `j.eismann`, **nicht** SYSTEM (DPAPI ist an das Konto gebunden) |
| Neu starten | bei Fehler, alle 5 Minuten, bis zu 999 Mal |
| Zeitlimit | keines (der Dauerlauf soll laufen) |

Das Skript taktet sich selbst (`--interval`, Standard 60 Sekunden) — die
Aufgabenplanung startet es einmal, nicht jede Minute.

Der Dauerlauf übersteht einen Fehlschlag: eine Ausnahme beendet den Monitor
nicht mehr, sondern wird gemeldet, und der nächste Takt läuft normal weiter.
Das ist erst hier wichtig geworden — in GitHub Actions war jeder Poll ein
eigener Prozess, ein Absturz kostete eine Minute. Ein Prozess, der Wochen
läuft, würde dagegen stehenbleiben, ohne dass es auffällt: die Seite ist ja
noch da, nur eingefroren.

## Anzeige

`--serve` liefert die Seite auf `http://127.0.0.1:8787/` aus — ausdrücklich
nur auf der Schleife, nicht im Netz.

Ein lokaler Webserver statt einer `file://`-Adresse ist nötig, weil die Seite
per `fetch('wan_stand.txt')` prüft, ob ein neuerer Stand vorliegt, und
Chromium das für lokale Dateien verweigert. Der Server ist ein Dutzend Zeilen
Standardbibliothek, kein IIS.

Browser im Kioskmodus, in derselben Aufgabe oder im Autostart des Sitzungskontos:

```
msedge.exe --kiosk http://127.0.0.1:8787/ --edge-kiosk-type=fullscreen --no-first-run
```

Die Seite lädt sich selbst neu, sobald `wan_stand.txt` einen neueren Zeitpunkt
trägt — kein fester Countdown, der neben dem echten Takt herläuft.

## Sicherung

Die Messdatei liegt auf einer Client-Festplatte ohne Sicherung. Eine Zeile
am Ende des Starters oder eine zweite Aufgabe um 03:00:

```powershell
Copy-Item C:\WAN-Monitor\wan_traffic.csv `
          \\WUS-DB01\Sicherung\WAN-Monitor\wan_traffic.csv -Force
```

Ohne das hängt die Messhistorie an einer einzelnen Platte.

## Umstellung

1. Skript und Daten auf den Rechner, Schlüsseldatei unter dem
   Sitzungskonto anlegen
2. Aufgabe und Kioskbrowser einrichten, laufen lassen
3. Ein paar Tage gegen GitHub Pages vergleichen — beide Seiten zeigen
   dieselben Messwerte, weil beide dieselbe API fragen
4. Erst dann den GitHub-Workflow abschalten und den Auftrag bei cron-job.org
   löschen

GitHub bleibt die Ablage für den Code. Wird die Seite weiter von außen
gebraucht, kann der Rechner sie nebenbei ins Repository schieben — scheitert
das, hinkt nur die entfernte Ansicht hinterher, der Bildschirm an der Wand
nicht. Genau das war an der alten Lösung falsch: dort hing die Anzeige an der
Veröffentlichung.

## Was danach nicht mehr gebraucht wird

- `.github/workflows/wan-monitor.yml` — Poll, Commit, Pages-Deploy, die
  Abbruchschritte für hängende Deployments und die Verkürzung der Historie
- der Auftrag bei cron-job.org
- GitHub Pages für dieses Repository
- die Messwerte in der Versionsverwaltung. Sie haben die Historie auf 865 MB
  anwachsen lassen — nicht durch ihre Größe (3,5 MB), sondern weil die Datei
  1440 Mal am Tag vollständig neu committet wurde
