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
| **Python** | 3.12.8 als *embeddable package* von python.org, entpackt in `C:\WAN-Monitor\python`. **Nicht installiert** — kein Eintrag in PATH, Registrierung oder Softwareliste; mit dem Ordner restlos weg. Es war auf dem Gerät keines vorhanden, und für ein Skript, das nur die Standardbibliothek nutzt, ist eine Installation unnötiger Eingriff |
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

## Aus der Ferne, ohne WinRM

WinRM ist auf dem Gerät nicht erreichbar (Port 5985 zu), und einschalten
wollten wir es nicht — der Dienst liefe dann dauerhaft und brächte eine
Firewall-Regel mit. Offen sind RPC, SMB und RDP.

RDP fällt aus: Windows-Clients erlauben nur **eine** interaktive Sitzung, eine
Anmeldung übernimmt also die Konsolensitzung, und nach dem Trennen zeigt der
Wandbildschirm den Anmeldeschirm — bis sich jemand örtlich anmeldet. Bei einem
Bildschirm unter der Decke heißt das: Leiter.

Also **PsExec** über SMB: legt für die Dauer des Aufrufs einen Dienst an und
entfernt ihn danach, keine bleibende Änderung, keine Sitzungsübernahme.

```powershell
# Auf dem Arbeitsplatz, als normaler Benutzer, OHNE Erhöhung.
# PsExec braucht die Rechte auf dem ZIEL, nicht hier - es fragt sie selbst ab.
& "<Pfad>\PsExec64.exe" \\<Leitstand-PC> -u <DOMAENE>\<Admin> -s -accepteula -nobanner `
    cmd /c "powershell -NoProfile -ExecutionPolicy Bypass -File C:\WAN-Monitor\einrichten-leitstand.ps1 ... > C:\WAN-Monitor\logs\aus.txt 2>&1"
```

Drei Dinge, die Zeit kosten, wenn man sie nicht weiß:

- **`-s` ist nötig.** Ohne läuft der Prozess als das angegebene Konto, und
  Richtlinien verbieten diesem Gerät den dafür nötigen Anmeldetyp. Mit `-s`
  läuft er als SYSTEM; die Anmeldung dient nur der Verbindung.
- **Ausgabe in eine Datei umleiten** und über die Freigabe lesen. PsExecs
  Ausgabe kommt sonst abgeschnitten zurück.
- **Windows PowerShell 5.1 ist die Gegenstelle.** Ein `if` als Ausdruck in
  einer Hashtabelle ist dort ein Syntaxfehler, der Block läuft gar nicht erst
  an.

## In einem Durchgang

`einrichten-leitstand.ps1` macht alles Folgende selbst — Verzeichnis, Rechte,
Schlüsseldatei, beide Starter, die zwei Aufgaben, Energieoptionen. Auf dem
Leitstand-PC ausführen, angemeldet als das Konto, unter dem der Monitor laufen
soll:

```powershell
cd C:\Sync\claude-projekte\Wan-LSB
.\einrichten-leitstand.ps1 -NurPruefen     # zeigt nur, was es täte
.\einrichten-leitstand.ps1                 # und dann wirklich
.\einrichten-leitstand.ps1 -MitAutologin   # dazu die Anleitung zur Anmeldung
```

Das Skript ist wiederholbar: es legt nur an, was fehlt, und fasst eine
vorhandene Messdatei nie an. `-NurStarter` erzeugt bloß die beiden Starter neu,
etwa nach einem geänderten Port.

Die Abschnitte darunter beschreiben, **was** es einrichtet — zum Nachlesen und
für den Fall, dass etwas von Hand nachzuziehen ist.

## Verzeichnis

```
C:\WAN-Monitor\
    udm_wan_monitor.py      das Skript
    wan_traffic.csv         Messwerte (wächst)
    monitor_state.json      Zustand, Ereignisprotokoll, Zwischenspeicher
    wan_report.html         die Seite, wird jede Minute neu geschrieben
    wan_stand.txt           Zeitstempel für die Neuladeprüfung der Seite
```

Rechte: Vollzugriff nur für `j.eismann`, SYSTEM und Administratoren, mit
Vererbung auf die Dateien. SYSTEM ist nötig, weil die Messung als Dienst
läuft und die Schlüsseldatei lesen muss.

**Die Reihenfolge beim Setzen ist wesentlich.** `icacls /reset /T` setzt auch
den Ordner selbst auf Vererbung zurück, und dann erbt er von `C:\` das Recht
„Authentifizierte Benutzer: Ändern" — die Schlüsseldatei wäre für jeden
angemeldeten Benutzer lesbar. Also erst zurücksetzen, dann den Ordner
schließen. Und die Vererbungs-Flags `(OI)(CI)` gehören **nur** auf den
Ordner: mit `/T` auch auf die Dateien angewendet ergeben sie Berechtigungen,
die ins Leere zeigen, und die Datei bleibt ohne jeden Zugriff — auch für
SYSTEM. Genau so ließ sich `python.exe` nicht mehr starten, lautlos. Das
Skript prüft deshalb am Ende gegen, ob eine Datei wieder lesbar ist.

## Der API-Schlüssel

Das Skript liest ihn aus der Umgebungsvariablen `UI_API_KEY` oder aus einer
`.env` neben sich (Format: `.env.beispiel`; die Variable sticht die Datei).

Auf dem Leitstand-PC kommt er aus `C:\WAN-Monitor\schluessel.xml`,
verschlüsselt mit DPAPI und an die **Maschine** gebunden — nicht an ein Konto.

Das war nicht die erste Wahl. Kontogebunden wäre enger, setzt aber voraus, dass
der Schlüssel in einer Sitzung des Zielkontos angelegt wird. Auf diesem Gerät
verbieten Richtlinien dem Konto den Prozessstart über die Fernverwaltung
(*„Der Benutzer besitzt nicht den benötigten Anmeldetyp auf diesem Computer"*),
die Einrichtung läuft dort als SYSTEM — und ein von SYSTEM kontogebunden
verschlüsselter Schlüssel wäre für die Anzeige wertlos.

**Was die Maschinenbindung kostet:** jeder, der auf diesem Rechner Code
ausführen kann und die Datei lesen darf, kann sie entschlüsseln. Der Schutz
liegt damit allein bei den Dateirechten — der Ordner ist auf das Zielkonto,
SYSTEM und Administratoren beschränkt, die Dateien erben das. Wer diese Rechte
lockert, hebt den Schutz auf. Die Festplatte ist mit BitLocker verschlüsselt,
der Schlüssel also nicht offline auslesbar.

### Setzen

`schluessel-setzen.ps1` überträgt ihn, ohne dass ihn jemand tippen oder kennen
muss — er kommt aus der Umgebungsvariablen des ausführenden Rechners:

```powershell
# Auf dem Arbeitsplatz, als normaler Benutzer, ohne Erhöhung
.\schluessel-setzen.ps1 -Rechner <Leitstand-PC> -AusUmgebung -PsExec <Pfad>
```

Ohne `-AusUmgebung` fragt es ihn verdeckt ab.

Zur Kontrolle gibt die Gegenseite nur Länge und die ersten acht Zeichen des
SHA-256 zurück. Daran lässt sich jederzeit vergleichen, ob beide Seiten
denselben Schlüssel haben, ohne ihn anzuzeigen.

**Zwei Wege, die nicht funktionieren** — beide geprüft, damit es niemand
wieder versucht:

- **Standardeingabe durch PsExec.** Wäre der sauberste Weg, der Schlüssel
  berührte keine Datei. PsExec reicht sie mit `-s` aber nicht durch: ein
  `findstr` am anderen Ende bekam weder über die PowerShell-Pipeline noch über
  eine echte OS-Pipe etwas zu sehen.
- **Am Gerät tippen, über PsExec.** `Read-Host -AsSecureString` verdeckt die
  Eingabe **nicht**, wenn die Konsole umgeleitet ist. Der Probewert stand im
  Klartext im Protokoll.

Deshalb der Umweg über eine Zwischendatei, die das Gegenstück als Erstes
überschreibt und löscht — wenige Sekunden, in einem Verzeichnis, auf das nur
das Zielkonto, SYSTEM und Administratoren Zugriff haben. Ein bewusster
Kompromiss, keine Nachlässigkeit.

## Aufgabenplanung

Zwei Aufgaben, mit **unterschiedlichen** Auslösern und Konten — das ist der
Punkt:

| | `WAN-Monitor` | `WAN-Monitor Kiosk` |
|---|---|---|
| Auslöser | **Systemstart** (+30 s) | Anmeldung des Zielkontos |
| Ausführen als | **SYSTEM** | das Zielkonto, interaktiv |
| Neu starten | bei Fehler, alle 5 Min, bis 999 Mal | ebenso |
| Zeitlimit | keines | keines |

**Warum die Messung als Dienst läuft:** sonst hinge sie daran, dass sich
jemand anmeldet. Ein Kennwortwechsel, der die automatische Anmeldung
aushebelt, hätte dann auch die Messwerte gestoppt — und das wäre niemandem
aufgefallen, weil die Seite ja noch dastünde, nur eingefroren. Jetzt laufen
die Messwerte über jeden Neustart hinweg; an der automatischen Anmeldung hängt
nur noch das Bild. Möglich ist das nur, weil die Schlüsseldatei an die
Maschine gebunden ist und SYSTEM sie lesen kann.

Das Skript taktet sich selbst (`--interval`, Standard 60 Sekunden) — die
Aufgabenplanung startet es einmal, nicht jede Minute.

### Zwei Dinge, die stillschweigend schiefgingen

**Das Protokoll muss vor allem anderen bestimmt werden.** Stand die Zeile
weiter unten, verschwand jeder Fehler davor spurlos: die Aufgabe meldete nur
„Ergebnis 1", die Protokolldatei blieb 0 Bytes groß. Beide Abschnitte des
Starters schreiben ihren Abbruch jetzt hinein.

**Python braucht `-u`.** Ohne den Schalter hält es seine Ausgabe zurück,
solange der Prozess läuft — bei einem Dauerläufer also für immer. Das
Protokoll blieb leer, obwohl jede Minute gemessen wurde. Ebenso schreibt
Windows PowerShell 5.1 mit `*>>` standardmäßig UTF-16, was das Protokoll mit
`tail` oder `Get-Content` unlesbar macht; der Starter legt UTF-8 fest.

## Anzeige

`--serve` liefert die Seite auf `http://127.0.0.1:8787/` aus — ausdrücklich
nur auf der Schleife, nicht im Netz.

Ein lokaler Webserver statt einer `file://`-Adresse ist nötig, weil die Seite
per `fetch('wan_stand.txt')` prüft, ob ein neuerer Stand vorliegt, und
Chromium das für lokale Dateien verweigert. Der Server ist ein Dutzend Zeilen
Standardbibliothek, kein IIS.

Chrome im Kioskmodus, gestartet von der zweiten Aufgabe. `--kiosk` ist ein
Schalter — die Adresse gehört als **eigenes** Argument dahinter. Als
`--kiosk=<Adresse>` geschrieben startet Chrome im Vollbild mit einer leeren
Registerkarte, und an einer Wandanzeige fällt das niemandem auf.

Der Kiosk-Starter wartet, bis die Seite antwortet, und startet Chrome erst
dann: ein Kioskfenster auf einer Fehlerseite lädt nicht von selbst nach. Nach
der Wartezeit startet er trotzdem — eine Fehlerseite am Bildschirm ist besser
als ein schwarzer Monitor, an dem niemand sieht, dass etwas fehlt.

Geprüft: Chrome im Kioskmodus öffnet die Seite, und die Seite fragt
anschließend selbst `wan_stand.txt` ab — der Neulademechanismus greift.

Die Seite lädt sich selbst neu, sobald `wan_stand.txt` einen neueren Zeitpunkt
trägt — kein fester Countdown, der neben dem echten Takt herläuft.

## Automatische Anmeldung

Damit der Bildschirm nach einem Neustart ohne Zutun wieder dasteht, muss sich
das Konto selbst anmelden. Das Skript setzt dafür **kein Kennwort** — der
gängige Weg über `Winlogon\DefaultPassword` legt es im Klartext in die
Registrierung, lesbar für jeden lokalen Administrator und bei Fernzugriff auf
die Registrierung auch von außerhalb des Raums.

Stattdessen [Sysinternals Autologon](https://learn.microsoft.com/sysinternals/downloads/autologon),
das es LSA-verschlüsselt ablegt:

```
Autologon.exe -accepteula <Benutzer> <Domäne> <Kennwort>
```

Dazu drei Punkte:

- **Nach einem Kennwortwechsel muss das Autologin neu gesetzt werden**, sonst
  bleibt der Bildschirm nach dem nächsten Neustart auf der Anmeldung stehen.
  Das ist der häufigste Grund, warum so eine Anzeige irgendwann schwarz ist
- Die Festplatte sollte verschlüsselt sein (BitLocker) — sonst ist das
  gespeicherte Kennwort offline auslesbar, unabhängig davon, wie gut der Raum
  gesichert ist
- Eine Bildschirmsperre aus einer Richtlinie überschreibt die Einstellungen des
  Skripts. Dann braucht dieses Gerät eine Ausnahme in der GPO, sonst sperrt der
  Bildschirm doch

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
