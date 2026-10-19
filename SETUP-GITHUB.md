# Messung ohne lokale Ausfuehrung

Alles laeuft in GitHub Actions. Kein Rechner muss an sein, nichts wird bei dir
installiert. Ergebnis ist eine Seite, die du jederzeit aufrufen kannst.

## Einrichtung, einmalig, etwa fuenf Minuten

1. Neues privates Repository anlegen, zum Beispiel `wan-monitor`.

2. Diese drei Dateien hineinlegen:

       udm_wan_monitor.py
       .github/workflows/wan-monitor.yml
       README.md            (optional)

3. Neuen API-Key erzeugen unter unifi.ui.com bei Account, API. Den im Chat
   geposteten Key vorher loeschen.

4. Im Repository unter Settings, Secrets and variables, Actions:
   Secret anlegen mit Namen `UI_API_KEY` und dem neuen Key als Wert.
   GitHub zeigt ihn danach nie wieder an und maskiert ihn in allen Logs.

5. Optional im selben Menue unter Variables anlegen, sonst gelten die Vorgaben
   aus dem Workflow:

       MESS_START    2026-08-07T12:00
       MESS_TAGE     3
       SITE_FILTER   LSB--UDM-1

6. Unter Settings, Pages als Source "GitHub Actions" waehlen.

7. Unter Actions den Workflow einmal manuell starten (Run workflow). Danach
   laeuft er alle 15 Minuten von selbst.

## Ergebnis

Die Berichtsseite liegt unter der Pages-URL des Repositories und aktualisiert
sich mit jedem Lauf. Die Rohdaten stehen als `wan_traffic.csv` im Repository und
lassen sich jederzeit herunterladen und in Excel weiterverarbeiten.

## Hinweise

Der Cron-Zeitplan von GitHub ist keine Echtzeitgarantie, bei Last kann ein Lauf
einige Minuten spaeter starten oder ausfallen. Das ist unkritisch, weil die API
rueckwirkend liefert: Der naechste Lauf holt die Luecke automatisch nach. Aus
demselben Grund verliert die Messung nichts, wenn du erst spaeter einrichtest.
Die 5-Minuten-Werte sind mindestens 24 Stunden verfuegbar, die Stundenwerte
mindestens 30 Tage.

Der Key liegt ausschliesslich im GitHub-Secret. Das Skript liest ihn aus der
Umgebung und schreibt ihn in keine Datei. Wenn das Repository oeffentlich waere,
haetten Fremde zwar keinen Zugriff auf das Secret, wohl aber auf deine
Verkehrsdaten, deshalb privat anlegen.

## Erster Lauf pruefen

Im Log des Workflow-Schritts steht, wieviele Messpunkte erkannt wurden. Steht
dort 0, weicht das JSON-Schema deiner UniFi-OS-Version ab. In dem Fall einmal
lokal oder im Actions-Log `--discover` laufen lassen und mir die Struktur
schicken, dann passe ich den Parser an.

## Alternativen, falls kein GitHub

Der Workflow laesst sich fast unveraendert nach GitLab CI, Azure DevOps oder in
eine Cloud-Function uebertragen. Notwendig ist nur ein Scheduler, Python und ein
Ort fuer das Secret. Wenn du bereits eine Monitoring-Instanz betreibst, ist auch
der direkte Weg ueber SNMP oder die Site-Manager-API in dein bestehendes System
sinnvoller als ein eigener Job.
