<#
.SYNOPSIS
    Richtet den Leitstand-PC ein: Monitor als Dauerlauf, Chrome im Kiosk,
    Bildschirm bleibt an, Anmeldung ueberlebt einen Neustart.

.BESCHREIBUNG
    Auf dem LEITSTAND-PC ausfuehren, angemeldet als das Konto, unter dem der
    Monitor laufen soll - nicht als ein anderes Konto und nicht remote. Der
    API-Schluessel wird mit DPAPI an Konto und Maschine gebunden; unter einem
    anderen Konto angelegt ist die Datei hier wertlos.

    Das Skript ist wiederholbar: es legt nur an, was fehlt, und aendert, was
    anders ist. Ein zweiter Lauf schadet nicht.

    Das Autologin-Kennwort fasst dieses Skript NICHT an. Siehe -MitAutologin.

.BEISPIEL
    .\einrichten-leitstand.ps1

.BEISPIEL
    .\einrichten-leitstand.ps1 -Verzeichnis D:\WAN-Monitor -Port 9000
#>
[CmdletBinding()]
param(
    # Wohin Skript, Messdaten und Seite gehoeren.
    [string]$Verzeichnis = "C:\WAN-Monitor",

    # Port des lokalen Webservers. Nur auf 127.0.0.1, nie im Netz.
    [int]$Port = 8787,

    # Sekunden, die der Kioskbrowser dem Monitor Zeit gibt. Das Skript
    # wartet ohnehin, bis die Seite antwortet - das hier ist die Obergrenze.
    [int]$WartenAufMonitor = 90,

    # Zeigt die Anleitung fuer das Autologin und prueft den jetzigen Stand,
    # ohne ein Kennwort zu beruehren.
    [switch]$MitAutologin,

    # Das Konto, unter dem Monitor und Kiosk laufen sollen, als
    # "DOMAENE\Benutzer". Standard ist das Konto, das dieses Skript ausfuehrt.
    #
    # Ausdruecklich angeben, wenn das Skript unter einem ANDEREN Konto laeuft
    # als die spaetere Anzeige - etwa bei der Ferneinrichtung, wo die
    # maschinenweiten Teile ein Administrator macht. Ohne diesen Parameter
    # bekaeme dort der Administrator die Aufgaben und die Dateirechte, und die
    # Anzeige liefe nie los.
    [string]$Konto = "$env:USERDOMAIN\$env:USERNAME",

    # Pfad zu python.exe. Ohne Angabe wird im PATH gesucht.
    #
    # Fuer ein mitgebrachtes "embeddable package" ausdruecklich angeben: das
    # ist nicht installiert und steht in keinem PATH - schon gar nicht in dem
    # von SYSTEM, unter dem die Ferneinrichtung laeuft.
    [string]$PythonPfad,

    # Den Schluessel nicht abfragen. Fuer einen Lauf ohne Konsole, bei dem
    # der Schluessel in einem eigenen Schritt gesetzt wird.
    [switch]$OhneSchluessel,

    # Nur pruefen und berichten, nichts aendern.
    [switch]$NurPruefen,

    # Nur die beiden Starter-Skripte neu erzeugen und aufhoeren. Fuer einen
    # geaenderten Port oder Pfad, ohne Aufgaben und Energieoptionen
    # anzufassen - und um das Erzeugte pruefen zu koennen, ohne etwas am
    # System zu veraendern.
    [switch]$NurStarter
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

# Fuer ProtectedData (Schluesseldatei). In Windows PowerShell 5.1 noetig, in
# PowerShell 7 schon vorhanden und der Aufruf dort harmlos.
Add-Type -AssemblyName System.Security

$IchSelbst = "$env:USERDOMAIN\$env:USERNAME"
# Laeuft das Skript unter einem anderen Konto als die spaetere Anzeige? Dann
# kann es die DPAPI-Schluesseldatei weder anlegen noch pruefen - das muss in
# der Sitzung des Zielkontos passieren.
$FremdesKonto = $Konto -ne $IchSelbst
$Adresse = "http://127.0.0.1:$Port/"
$StarterMonitor = Join-Path $Verzeichnis "starte-monitor.ps1"
$StarterKiosk = Join-Path $Verzeichnis "starte-kiosk.ps1"
$Schluesseldatei = Join-Path $Verzeichnis "schluessel.xml"
$Protokoll = Join-Path $Verzeichnis "logs"

function Schritt($text) { Write-Host "`n=== $text" -ForegroundColor Cyan }
function Gut($text) { Write-Host "  [ok] $text" -ForegroundColor Green }
function Hinweis($text) { Write-Host "  [--] $text" -ForegroundColor Yellow }
function Tun($text, [scriptblock]$block) {
    if ($NurPruefen) { Hinweis "wuerde tun: $text"; return $false }
    & $block
    Gut $text
    return $true
}

Write-Host "Leitstand-PC einrichten" -ForegroundColor White
Write-Host "  Konto       : $Konto"
if ($FremdesKonto) { Write-Host "  ausgefuehrt von: $IchSelbst" }
Write-Host "  Verzeichnis : $Verzeichnis"
Write-Host "  Adresse     : $Adresse"
if ($NurPruefen) { Hinweis "Nur-Pruefen-Lauf, es wird nichts geaendert." }


# ---------------------------------------------------------------- 0) Vorweg
Schritt "Voraussetzungen"

if ($NurStarter -and -not (Test-Path $Verzeichnis)) {
    New-Item -ItemType Directory -Path $Verzeichnis -Force | Out-Null
    New-Item -ItemType Directory -Path $Protokoll -Force | Out-Null
}

# Kein Dauerlauf unter einem Admin-Konto, ohne es gesagt zu haben. Der
# Monitor braucht keine Domaenenrechte - und ein Kiosk mit Autologin gibt
# jedem mit Zugang zum Raum genau die Rechte dieses Kontos.
if ($Konto -like "*\admin-*") {
    Hinweis "Dieses Konto sieht nach einem Administrationskonto aus."
    Hinweis "Der Monitor braucht keinerlei Domaenenrechte. Mit Autologin"
    Hinweis "bekommt jeder, der an das Geraet kommt, eine offene Sitzung"
    Hinweis "mit diesen Rechten. Lieber ein Einzweckkonto nehmen."
}

if ($PythonPfad) {
    if (-not (Test-Path $PythonPfad)) { throw "Python nicht gefunden: $PythonPfad" }
    $python = (Resolve-Path $PythonPfad).Path
} else {
    $python = (Get-Command python -ErrorAction SilentlyContinue).Source
}
if (-not $python) {
    throw "Python nicht gefunden. 3.9 oder neuer installieren und PATH pruefen, " +
          "oder ein mitgebrachtes python.exe mit -PythonPfad angeben."
}
$version = (& $python -c "import sys;print('%d.%d' % sys.version_info[:2])").Trim()
Gut "Python $version ($python)"

$chrome = @(
    "$env:ProgramFiles\Google\Chrome\Application\chrome.exe"
    "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe"
    "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe"
) | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $chrome) { throw "Chrome nicht gefunden. Installieren oder Pfad im Skript ergaenzen." }
Gut "Chrome: $chrome"


if ($NurStarter) { Gut "Nur die Starter werden erzeugt." }

# ----------------------------------------------------------- 1) Verzeichnis
if (-not $NurStarter) {
Schritt "Verzeichnis und Rechte"

foreach ($pfad in @($Verzeichnis, $Protokoll)) {
    if (Test-Path $pfad) { Gut "vorhanden: $pfad" }
    else { Tun "angelegt: $pfad" { New-Item -ItemType Directory -Path $pfad -Force | Out-Null } | Out-Null }
}

# Vererbung weg, dann nur dieses Konto, SYSTEM und Administratoren. Sonst
# liest jeder authentifizierte Benutzer die Messdaten mit - und, schlimmer,
# die verschluesselte Schluesseldatei, die dann nur noch das Konto braucht.
if (-not $NurPruefen) {
    # (OI)(CI) sind VERERBUNGS-Flags und gelten nur fuer Ordner. Mit /T auch
    # auf die Dateien angewendet ergeben sie Berechtigungen, die ins Leere
    # zeigen: die Datei bleibt ohne jeden Zugriff, auch fuer SYSTEM. Am
    # 30.10.2026 genau so passiert - python.exe hatte danach eine leere
    # Zugriffsliste und liess sich nicht mehr starten ("Zugriff verweigert").
    #
    # Richtig: die Rechte NUR auf den Ordner, mit Vererbung, und die
    # vorhandenen Kinder per /reset darauf zurueckfuehren.
    # Reihenfolge ist entscheidend. /reset /T setzt AUCH den Ordner selbst
    # auf Vererbung zurueck - danach erbt er von C:\, wo "Authentifizierte
    # Benutzer: Aendern" steht. Die verschluesselte Schluesseldatei waere
    # damit fuer jeden angemeldeten Benutzer lesbar, und weil die
    # Verschluesselung an die MASCHINE gebunden ist (nicht an ein Konto),
    # auch entschluesselbar. Die Dateirechte sind hier der einzige Schutz.
    #
    # Also: erst die Kinder von ihren eigenen Eintraegen befreien, DANN den
    # Ordner schliessen - die Kinder erben das Ergebnis.
    & icacls.exe $Verzeichnis /reset /T /C /Q 2>$null

    $gruppe = "Administratoren"
    & icacls.exe $Verzeichnis /inheritance:r /grant:r `
        "${Konto}:(OI)(CI)F" "SYSTEM:(OI)(CI)F" "${gruppe}:(OI)(CI)F" /C /Q 2>$null
    if ($LASTEXITCODE -ne 0) {
        # Englisches Windows kennt keine "Administratoren".
        $gruppe = "Administrators"
        & icacls.exe $Verzeichnis /inheritance:r /grant:r `
            "${Konto}:(OI)(CI)F" "SYSTEM:(OI)(CI)F" "${gruppe}:(OI)(CI)F" /C /Q 2>$null
    }
    if ($LASTEXITCODE -eq 0) {
        Gut "Rechte gesetzt (nur $Konto, SYSTEM, $gruppe; Kinder erben)"

        # Gegenprobe, weil der Fehler oben lautlos war: eine Datei muss
        # wieder lesbar sein.
        $probe = Get-ChildItem $Verzeichnis -File -Recurse -ErrorAction SilentlyContinue |
                 Select-Object -First 1
        if ($probe) {
            try {
                $null = [IO.File]::OpenRead($probe.FullName).Close()
                Gut "Gegenprobe: Dateien sind lesbar"
            } catch {
                Hinweis "Gegenprobe FEHLGESCHLAGEN: $($probe.Name) ist nicht lesbar."
                Hinweis "Reparatur: icacls '$Verzeichnis' /reset /T"
            }
        }
    }
    else { Hinweis "Rechte konnten nicht gesetzt werden - bitte von Hand pruefen." }
}

# Das Skript selbst muss dort liegen. Kopiert wird nur, was fehlt oder
# aelter ist - eine laufende Messdatei wird NIE ueberschrieben.
$quelle = $PSScriptRoot
foreach ($datei in @("udm_wan_monitor.py")) {
    $von = Join-Path $quelle $datei
    $nach = Join-Path $Verzeichnis $datei
    if (-not (Test-Path $von)) { Hinweis "fehlt in der Quelle: $datei"; continue }
    if ((Test-Path $nach) -and
        ((Get-Item $nach).LastWriteTime -ge (Get-Item $von).LastWriteTime)) {
        Gut "aktuell: $datei"
    } else {
        Tun "kopiert: $datei" { Copy-Item $von $nach -Force } | Out-Null
    }
}
foreach ($datei in @("wan_traffic.csv", "monitor_state.json")) {
    $von = Join-Path $quelle $datei
    $nach = Join-Path $Verzeichnis $datei
    if (Test-Path $nach) { Gut "bleibt unberuehrt: $datei" }
    elseif (Test-Path $von) { Tun "uebernommen: $datei" { Copy-Item $von $nach } | Out-Null }
    else { Hinweis "$datei fehlt - der erste Lauf faengt bei Null an" }
}


# ------------------------------------------------------------ 2) Schluessel
Schritt "API-Schluessel"

# DPAPI mit Maschinen- statt Kontobindung.
#
# Die kontogebundene Form (ConvertFrom-SecureString) waere enger, setzt aber
# voraus, dass der Schluessel in einer Sitzung des Zielkontos angelegt wird.
# Auf ws007 verbieten Richtlinien diesem Konto den Prozessstart ueber die
# Fernverwaltung - die Einrichtung laeuft dort als SYSTEM, und ein von SYSTEM
# kontogebunden verschluesselter Schluessel waere fuer die Anzeige wertlos.
#
# Was die Maschinenbindung kostet: ein lokaler Administrator dieses Rechners
# kann entschluesseln. Er kann die Datei aber ohnehin lesen und sich als das
# Zielkonto ausgeben - der Unterschied ist gering. Der Schutz liegt bei den
# Dateirechten (siehe oben) und beim Zugang zum Geraet.
if (Test-Path $Schluesseldatei) {
    try {
        $rohdaten = [Convert]::FromBase64String((Get-Content $Schluesseldatei -Raw).Trim())
        $null = [Security.Cryptography.ProtectedData]::Unprotect(
                    $rohdaten, $null, "LocalMachine")
        Gut "Schluesseldatei vorhanden und auf diesem Rechner lesbar"
    } catch {
        Hinweis "Schluesseldatei vorhanden, aber nicht lesbar: $($_.Exception.Message)"
        Hinweis "Von einem anderen Rechner kopiert, oder noch im alten Format?"
        Hinweis "Loeschen und neu anlegen: Remove-Item '$Schluesseldatei'"
    }
} elseif ($NurPruefen) {
    Hinweis "Schluesseldatei fehlt - wuerde abgefragt"
} elseif ($OhneSchluessel) {
    Hinweis "Schluesseldatei fehlt - wird uebersprungen (-OhneSchluessel)."
    Hinweis "Ohne sie startet der Monitor nicht."
} else {
    Write-Host "  Der Schluessel wird verschluesselt abgelegt (DPAPI, gebunden an"
    Write-Host "  diesen Rechner). Die Eingabe bleibt verdeckt."
    $sicher = Read-Host -AsSecureString "  UniFi API-Schluessel"
    if ($sicher.Length -eq 0) { throw "Kein Schluessel eingegeben, abgebrochen." }
    $zeiger = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sicher)
    try {
        $klar = [Runtime.InteropServices.Marshal]::PtrToStringAuto($zeiger)
        $bytes = [Text.Encoding]::UTF8.GetBytes($klar)
        $geschuetzt = [Security.Cryptography.ProtectedData]::Protect(
                          $bytes, $null, "LocalMachine")
        Set-Content $Schluesseldatei ([Convert]::ToBase64String($geschuetzt)) -Encoding ASCII
        # Klartext und Bytes sofort aus dem Speicher nehmen.
        [Array]::Clear($bytes, 0, $bytes.Length)
    } finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($zeiger)
    }
    Gut "Schluesseldatei angelegt"
}
}  # Ende: nicht bei -NurStarter


# --------------------------------------------------------------- 3) Starter
Schritt "Starter-Skripte"

# Der Schluessel wird NUR in die Umgebung DIESES Prozesses gesetzt. Nicht
# maschinenweit: dort stuende er im Klartext in der Registrierung, fuer
# jeden lokalen Administrator lesbar.
$inhaltMonitor = @"
# Erzeugt von einrichten-leitstand.ps1 - Aenderungen gehen beim naechsten
# Lauf verloren. Startet den Monitor als Dauerlauf samt lokalem Webserver.
`$ErrorActionPreference = "Stop"
Set-Location "$Verzeichnis"

# Protokoll ZUERST bestimmen. Vorher stand diese Zeile unten, und alles, was
# davor schiefging, verschwand spurlos: die Aufgabe meldete nur "Ergebnis 1",
# das Protokoll blieb 0 Bytes gross. Am 30.10.2026 kostete das eine halbe
# Stunde Suche nach einem Fehler, der ein einziger Satz gewesen waere.
# Eine Datei je Tag, damit sie nicht unbegrenzt waechst.
`$log = Join-Path "$Protokoll" ("monitor-" + (Get-Date -Format "yyyy-MM-dd") + ".log")

# Windows PowerShell 5.1 schreibt mit *>> standardmaessig UTF-16, und das
# Protokoll ist dann mit tail, grep oder Get-Content kaum zu lesen (jedes
# Zeichen mit einem Nullbyte dazwischen). Hier beides festlegen: was
# Out-File schreibt - *>> benutzt es im Hintergrund - und was von Python
# hereinkommt.
`$PSDefaultParameterValues['Out-File:Encoding'] = 'utf8'
`$PSDefaultParameterValues['Add-Content:Encoding'] = 'utf8'
[Console]::OutputEncoding = [Text.UTF8Encoding]::new(`$false)

function Notiz(`$text) {
    Add-Content `$log ((Get-Date -Format "HH:mm:ss") + " " + `$text)
}

try {
    # DPAPI mit Maschinenbindung - Begruendung in einrichten-leitstand.ps1.
    # In Windows PowerShell 5.1 - und die startet die Aufgabenplanung - muss
    # System.Security erst geladen werden; in PowerShell 7 ist der Aufruf
    # harmlos.
    Add-Type -AssemblyName System.Security
    `$rohdaten = [Convert]::FromBase64String((Get-Content "$Schluesseldatei" -Raw).Trim())
    `$bytes = [Security.Cryptography.ProtectedData]::Unprotect(
                 `$rohdaten, `$null, "LocalMachine")
    try {
        `$env:UI_API_KEY = [Text.Encoding]::UTF8.GetString(`$bytes)
    } finally {
        [Array]::Clear(`$bytes, 0, `$bytes.Length)
    }
    Notiz "Start, Schluessel gelesen."
} catch {
    Notiz ("ABBRUCH beim Schluessel: " + `$_.Exception.Message)
    throw
}

try {
    # -u (ungepuffert): sonst haelt Python seine Ausgabe zurueck,
    # solange der Prozess laeuft - bei einem Dauerlaeufer also fuer
    # immer. Das Protokoll blieb dadurch leer, obwohl jede Minute
    # gemessen wurde.
    & "$python" -u udm_wan_monitor.py --loop --serve --port $Port *>> `$log
    Notiz ("Monitor beendet, Rueckgabewert " + `$LASTEXITCODE + ".")
} catch {
    Notiz ("ABBRUCH beim Start von Python: " + `$_.Exception.Message)
    throw
}
"@
Tun "geschrieben: starte-monitor.ps1" {
    Set-Content $StarterMonitor $inhaltMonitor -Encoding UTF8
} | Out-Null

# Chrome erst starten, wenn die Seite antwortet. Ein Kioskfenster auf einer
# Fehlerseite laedt NICHT von selbst nach - es blieben ein grauer Bildschirm
# und niemand, der es bemerkt.
$inhaltKiosk = @"
# Erzeugt von einrichten-leitstand.ps1 - Aenderungen gehen beim naechsten
# Lauf verloren. Wartet auf den Monitor und startet dann den Kiosk.
`$ErrorActionPreference = "Continue"

`$bereit = `$false
foreach (`$versuch in 1..$WartenAufMonitor) {
    try {
        `$null = Invoke-WebRequest "$Adresse" -UseBasicParsing -TimeoutSec 2
        `$bereit = `$true
        break
    } catch { Start-Sleep -Seconds 1 }
}
if (-not `$bereit) {
    # Trotzdem starten: besser eine Fehlerseite am Bildschirm als ein
    # schwarzer Monitor, an dem niemand sieht, dass etwas fehlt.
    Write-Warning "Monitor antwortet nach $WartenAufMonitor s nicht - Kiosk startet dennoch."
}

# Die Absturz- und Wiederherstellungsdialoge wuerden am Bildschirm stehen
# bleiben, bis jemand hinklickt - auf einem Geraet ohne Tastatur also fuer
# immer.
`$argumente = @(
    # --kiosk ist ein Schalter, die Adresse gehoert als EIGENES Argument
    # dahinter. Als "--kiosk=<Adresse>" geschrieben startet Chrome im
    # Vollbild mit einer leeren Registerkarte - an einer Wandanzeige faellt
    # das niemandem auf.
    "--kiosk"
    "--start-fullscreen"
    "--no-first-run"
    "--no-default-browser-check"
    "--disable-session-crashed-bubble"
    "--disable-features=TranslateUI"
    "--hide-crash-restore-bubble"
    "--password-store=basic"
    "--user-data-dir=$Verzeichnis\chrome-profil"
    "$Adresse"
)
Start-Process "$chrome" -ArgumentList `$argumente
"@
Tun "geschrieben: starte-kiosk.ps1" {
    Set-Content $StarterKiosk $inhaltKiosk -Encoding UTF8
} | Out-Null

if ($NurStarter) {
    Schritt "Fertig"
    Gut "Beide Starter in $Verzeichnis erzeugt, sonst nichts geaendert."
    return
}


# --------------------------------------------------------- 4) Aufgaben
Schritt "Aufgabenplanung"

function Aufgabe($name, $skript, $verzoegerung, [switch]$AlsSystem) {
    $aktion = New-ScheduledTaskAction -Execute "powershell.exe" `
        -Argument "-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$skript`"" `
        -WorkingDirectory $Verzeichnis
    # Keine Zeitbegrenzung: der Monitor SOLL durchlaufen. Neustart bei
    # Fehlern, weil ein Dauerlauf, der still endet, niemandem auffaellt.
    $einstellungen = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
        -ExecutionTimeLimit ([TimeSpan]::Zero) `
        -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 5) `
        -MultipleInstances IgnoreNew
    if ($AlsSystem) {
        # Die MESSUNG darf nicht davon abhaengen, dass jemand angemeldet
        # ist. Moeglich, weil die Schluesseldatei an die Maschine gebunden
        # ist (nicht an ein Konto) - SYSTEM kann sie lesen. Nach einem
        # Neustart laufen die Messwerte also weiter, selbst wenn die
        # Anmeldung aussteht, etwa weil ein Kennwortwechsel das Autologin
        # ausgehebelt hat. Am Autologin haengt dann nur noch das Bild.
        $trigger = New-ScheduledTaskTrigger -AtStartup
        $hauptteil = New-ScheduledTaskPrincipal -UserId "SYSTEM" `
            -LogonType ServiceAccount -RunLevel Highest
    } else {
        # Der Kiosk braucht zwingend eine Sitzung mit Bildschirm.
        $trigger = New-ScheduledTaskTrigger -AtLogOn -User $Konto
        $hauptteil = New-ScheduledTaskPrincipal -UserId $Konto `
            -LogonType Interactive -RunLevel Limited
    }
    if ($verzoegerung) { $trigger.Delay = $verzoegerung }
    Register-ScheduledTask -TaskName $name -Action $aktion -Trigger $trigger `
        -Settings $einstellungen -Principal $hauptteil -Force | Out-Null
}

Tun "Aufgabe 'WAN-Monitor' (Dauerlauf als SYSTEM beim Systemstart)" {
    Aufgabe "WAN-Monitor" $StarterMonitor "PT30S" -AlsSystem
} | Out-Null
Tun "Aufgabe 'WAN-Monitor Kiosk' (Chrome bei Anmeldung)" {
    Aufgabe "WAN-Monitor Kiosk" $StarterKiosk "PT10S"
} | Out-Null


# ------------------------------------------------- 5) Bildschirm bleibt an
Schritt "Energie und Bildschirmschoner"

# Eine Wandanzeige, die sich nach zehn Minuten abschaltet, ist keine.
if (-not $NurPruefen) {
    & powercfg.exe /change monitor-timeout-ac 0
    & powercfg.exe /change standby-timeout-ac 0
    & powercfg.exe /change disk-timeout-ac 0
    & powercfg.exe /change hibernate-timeout-ac 0
    Gut "Bildschirm, Standby, Platte und Ruhezustand auf 'nie' (Netzbetrieb)"

    # HKCU zeigt auf das Profil des AUSFUEHRENDEN Kontos. Bei der
    # Ferneinrichtung ist das SYSTEM - dort eingetragen haette es auf die
    # Anzeige keinerlei Wirkung. Also in die Struktur des Zielkontos, die
    # geladen ist, solange es angemeldet ist (und das ist es: die Anzeige
    # laeuft ja).
    $desktop = "HKCU:\Control Panel\Desktop"
    if ($FremdesKonto) {
        $desktop = $null
        try {
            $sid = (New-Object Security.Principal.NTAccount($Konto)).Translate(
                       [Security.Principal.SecurityIdentifier]).Value
            if (Test-Path "Registry::HKEY_USERS\$sid\Control Panel\Desktop") {
                $desktop = "Registry::HKEY_USERS\$sid\Control Panel\Desktop"
                Gut "Zielprofil gefunden ($sid)"
            } else {
                Hinweis "Profil von $Konto ist nicht geladen - das Konto ist"
                Hinweis "gerade nicht angemeldet. Bildschirmschoner bitte in"
                Hinweis "seiner Sitzung abschalten, sonst verdeckt er die Anzeige."
            }
        } catch {
            Hinweis "Konto $Konto nicht aufloesbar: $($_.Exception.Message)"
        }
    }
    if ($desktop) {
        Set-ItemProperty $desktop -Name ScreenSaveActive -Value "0"
        Set-ItemProperty $desktop -Name ScreenSaverIsSecure -Value "0"
        Set-ItemProperty $desktop -Name ScreenSaveTimeOut -Value "0"
        Gut "Bildschirmschoner aus"
    }
} else {
    Hinweis "wuerde Energieoptionen und Bildschirmschoner setzen"
}

# Eine Sperre nach Untaetigkeit macht den Kiosk unbrauchbar - und sie kommt
# oft aus einer Richtlinie, die dieses Skript nicht aufheben kann.
$sperre = Get-ItemProperty "HKCU:\Software\Policies\Microsoft\Windows\Control Panel\Desktop" `
    -Name ScreenSaveTimeOut -ErrorAction SilentlyContinue
if ($sperre) {
    Hinweis "Eine Richtlinie setzt eine Bildschirmsperre ($($sperre.ScreenSaveTimeOut)s)."
    Hinweis "Die ueberschreibt die Einstellung oben. Fuer dieses Geraet braucht"
    Hinweis "es eine Ausnahme in der GPO, sonst sperrt der Bildschirm doch."
}


# ------------------------------------------------------------ 6) Autologin
Schritt "Automatische Anmeldung"

$winlogon = "HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Winlogon"
$jetzt = Get-ItemProperty $winlogon -ErrorAction SilentlyContinue
$an = $jetzt -and $jetzt.PSObject.Properties.Name -contains "AutoAdminLogon" `
      -and $jetzt.AutoAdminLogon -eq "1"
if ($an) {
    Gut "Automatische Anmeldung ist aktiv (Benutzer: $($jetzt.DefaultUserName))"
    if ($jetzt.PSObject.Properties.Name -contains "DefaultPassword") {
        Hinweis "ACHTUNG: Das Kennwort steht im KLARTEXT in der Registrierung"
        Hinweis "(Winlogon\DefaultPassword), lesbar fuer jeden lokalen Administrator."
        Hinweis "Mit Sysinternals Autologon neu setzen - das legt es LSA-"
        Hinweis "verschluesselt ab - und den Klartextwert danach entfernen."
    }
} elseif ($MitAutologin) {
    Write-Host @"
  Das Kennwort fasst dieses Skript nicht an - absichtlich. Der gaengige Weg
  ueber Winlogon\DefaultPassword legt es im KLARTEXT in der Registrierung ab.

  Statt dessen Sysinternals Autologon (speichert LSA-verschluesselt):

      https://learn.microsoft.com/sysinternals/downloads/autologon
      Autologon.exe -accepteula $env:USERNAME $env:USERDOMAIN <Kennwort>

  Oder ohne Kommandozeile: Autologon.exe starten, Konto eintragen, Enable.

  Dazu bedenken:
   - Jeder mit Zugang zum Geraet hat damit eine offene Sitzung dieses Kontos.
     Fuer ein Einzweckkonto in Ordnung, fuer ein persoenliches nicht.
   - Das Geraet gehoert an einen Ort, an den nicht jeder herankommt, und die
     Festplatte sollte verschluesselt sein (BitLocker) - sonst ist das
     gespeicherte Kennwort offline auslesbar.
   - Nach einem Kennwortwechsel muss das Autologin neu gesetzt werden, sonst
     bleibt der Bildschirm nach dem naechsten Neustart auf der Anmeldung.
"@ -ForegroundColor Yellow
} else {
    Hinweis "Nicht aktiv. Fuer die Anleitung: -MitAutologin"
}


# ----------------------------------------------------------------- Abschluss
Schritt "Fertig"
if ($NurPruefen) {
    Hinweis "Nur-Pruefen-Lauf beendet, es wurde nichts geaendert."
    return
}
Write-Host @"
  Jetzt einmal von Hand pruefen - in dieser Reihenfolge:

   1. Start-ScheduledTask "WAN-Monitor"
      Dann: Get-Content "$Protokoll\monitor-$(Get-Date -Format 'yyyy-MM-dd').log" -Wait
      Erwartet: eine Zeile je Minute mit neuen Messpunkten.

   2. $Adresse im Browser - die Uebersicht muss erscheinen.

   3. Start-ScheduledTask "WAN-Monitor Kiosk"
      Chrome im Vollbild, die Seite laedt sich jede Minute selbst neu.

   4. Abmelden und neu anmelden: beides muss ohne Zutun hochkommen.

  Erst wenn das vier Mal gutgegangen ist, den GitHub-Workflow abschalten
  und den Auftrag bei cron-job.org loeschen.
"@ -ForegroundColor White
