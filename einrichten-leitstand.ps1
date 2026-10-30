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

$Konto = "$env:USERDOMAIN\$env:USERNAME"
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
if ($env:USERNAME -like "admin-*") {
    Hinweis "Dieses Konto sieht nach einem Administrationskonto aus."
    Hinweis "Der Monitor braucht keinerlei Domaenenrechte. Mit Autologin"
    Hinweis "bekommt jeder, der an das Geraet kommt, eine offene Sitzung"
    Hinweis "mit diesen Rechten. Lieber ein Einzweckkonto nehmen."
}

$python = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $python) {
    throw "Python nicht gefunden. 3.9 oder neuer installieren und PATH pruefen."
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
    & icacls.exe $Verzeichnis /inheritance:r /grant:r `
        "${Konto}:(OI)(CI)F" "SYSTEM:(OI)(CI)F" "Administratoren:(OI)(CI)F" /T /C /Q 2>$null
    if ($LASTEXITCODE -ne 0) {
        # Englisches Windows kennt keine "Administratoren".
        & icacls.exe $Verzeichnis /inheritance:r /grant:r `
            "${Konto}:(OI)(CI)F" "SYSTEM:(OI)(CI)F" "Administrators:(OI)(CI)F" /T /C /Q 2>$null
    }
    if ($LASTEXITCODE -eq 0) { Gut "Rechte gesetzt (nur $Konto, SYSTEM, Administratoren)" }
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

if (Test-Path $Schluesseldatei) {
    # Lesen beweist, dass DPAPI unter DIESEM Konto entschluesseln kann. Der
    # Klartext wird nicht angezeigt und nicht aufbewahrt.
    try {
        $null = Get-Content $Schluesseldatei -Raw | ConvertTo-SecureString
        Gut "Schluesseldatei vorhanden und von diesem Konto lesbar"
    } catch {
        Hinweis "Schluesseldatei vorhanden, aber von diesem Konto NICHT lesbar."
        Hinweis "DPAPI bindet sie an Konto und Maschine. Unter einem anderen"
        Hinweis "Konto angelegt oder von einem anderen Rechner kopiert?"
        Hinweis "Loeschen und neu anlegen: Remove-Item '$Schluesseldatei'"
    }
} elseif ($NurPruefen) {
    Hinweis "Schluesseldatei fehlt - wuerde abgefragt"
} else {
    Write-Host "  Der Schluessel wird verschluesselt abgelegt (DPAPI, gebunden an"
    Write-Host "  dieses Konto und diesen Rechner). Die Eingabe bleibt verdeckt."
    $sicher = Read-Host -AsSecureString "  UniFi API-Schluessel"
    if ($sicher.Length -eq 0) { throw "Kein Schluessel eingegeben, abgebrochen." }
    $sicher | ConvertFrom-SecureString | Set-Content $Schluesseldatei -Encoding ASCII
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

`$sicher = Get-Content "$Schluesseldatei" -Raw | ConvertTo-SecureString
`$zeiger = [Runtime.InteropServices.Marshal]::SecureStringToBSTR(`$sicher)
try {
    `$env:UI_API_KEY = [Runtime.InteropServices.Marshal]::PtrToStringAuto(`$zeiger)
} finally {
    # Den Klartext aus dem unverwalteten Speicher nehmen, sobald er in der
    # Prozessumgebung steht.
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR(`$zeiger)
}

# Eine Protokolldatei je Tag, damit sie nicht unbegrenzt waechst.
`$log = Join-Path "$Protokoll" ("monitor-" + (Get-Date -Format "yyyy-MM-dd") + ".log")
& "$python" udm_wan_monitor.py --loop --serve --port $Port *>> `$log
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

function Aufgabe($name, $skript, $verzoegerung) {
    $aktion = New-ScheduledTaskAction -Execute "powershell.exe" `
        -Argument "-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$skript`"" `
        -WorkingDirectory $Verzeichnis
    $trigger = New-ScheduledTaskTrigger -AtLogOn -User $Konto
    if ($verzoegerung) { $trigger.Delay = $verzoegerung }
    # Keine Zeitbegrenzung: der Monitor SOLL durchlaufen. Neustart bei
    # Fehlern, weil ein Dauerlauf, der still endet, niemandem auffaellt.
    $einstellungen = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
        -ExecutionTimeLimit ([TimeSpan]::Zero) `
        -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 5) `
        -MultipleInstances IgnoreNew
    # Interaktiv, nicht als Dienst: DPAPI haengt an der Anmeldesitzung, und
    # der Kiosk braucht ohnehin einen Bildschirm.
    $hauptteil = New-ScheduledTaskPrincipal -UserId $Konto -LogonType Interactive `
        -RunLevel Limited
    Register-ScheduledTask -TaskName $name -Action $aktion -Trigger $trigger `
        -Settings $einstellungen -Principal $hauptteil -Force | Out-Null
}

Tun "Aufgabe 'WAN-Monitor' (Dauerlauf bei Anmeldung)" {
    Aufgabe "WAN-Monitor" $StarterMonitor $null
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

    $desktop = "HKCU:\Control Panel\Desktop"
    Set-ItemProperty $desktop -Name ScreenSaveActive -Value "0"
    Set-ItemProperty $desktop -Name ScreenSaverIsSecure -Value "0"
    Set-ItemProperty $desktop -Name ScreenSaveTimeOut -Value "0"
    Gut "Bildschirmschoner aus"
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
