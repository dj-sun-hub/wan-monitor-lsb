<#
.SYNOPSIS
    Richtet den Leitstand-PC aus der Ferne ein (WinRM).

.BESCHREIBUNG
    Von einem Arbeitsplatz aus. Fragt die Anmeldedaten selbst ab - sie werden
    getippt, nirgends abgelegt und nicht angezeigt.

    Drei Dinge laufen in UNTERSCHIEDLICHEN Rechten, deshalb gegebenenfalls
    zwei Anmeldungen:

      Maschinenweit (Aufgaben anlegen, Energieoptionen)  -> Administrator
      Schluesseldatei anlegen (DPAPI an das Konto gebunden) -> Sitzungskonto

    Ist das Sitzungskonto auf dem Geraet selbst lokaler Administrator, genuegt
    eine Anmeldung. Das Skript prueft das und fragt nur, was es braucht.

    Der API-Schluessel wird in der Sitzung des Sitzungskontos abgefragt und
    dort sofort verschluesselt. Er geht nicht durch eine Datei und steht in
    keinem Protokoll.

.BEISPIEL
    .\ferneinrichtung-leitstand.ps1 -Rechner 10.0.60.123 -Sitzungskonto j.eismann

.BEISPIEL
    .\ferneinrichtung-leitstand.ps1 -Rechner leitstand01 -NurPruefen
#>
[CmdletBinding()]
param(
    # IP oder Name des Leitstand-PC.
    [Parameter(Mandatory = $true)]
    [string]$Rechner,

    # Das Konto, unter dem die Anzeige laeuft (ohne Domaene).
    [string]$Sitzungskonto = "j.eismann",

    # Domaene fuer die Anmeldung.
    [string]$Domaene = $env:USERDOMAIN,

    # Zielverzeichnis auf dem Leitstand-PC.
    [string]$Verzeichnis = "C:\WAN-Monitor",

    # Port des lokalen Webservers dort.
    [int]$Port = 8787,

    # Nur erreichen und berichten, nichts aendern.
    [switch]$NurPruefen
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$Quelle = $PSScriptRoot
$Dateien = @("udm_wan_monitor.py", "einrichten-leitstand.ps1")

function Schritt($t) { Write-Host "`n=== $t" -ForegroundColor Cyan }
function Gut($t) { Write-Host "  [ok] $t" -ForegroundColor Green }
function Hinweis($t) { Write-Host "  [--] $t" -ForegroundColor Yellow }
function Schlecht($t) { Write-Host "  [!!] $t" -ForegroundColor Red }

Write-Host "Leitstand-PC aus der Ferne einrichten" -ForegroundColor White
Write-Host "  Rechner       : $Rechner"
Write-Host "  Sitzungskonto : $Domaene\$Sitzungskonto"
Write-Host "  Verzeichnis   : $Verzeichnis"
if ($NurPruefen) { Hinweis "Nur-Pruefen-Lauf, es wird nichts geaendert." }

foreach ($d in $Dateien) {
    if (-not (Test-Path (Join-Path $Quelle $d))) {
        throw "$d fehlt neben diesem Skript. Aus dem Projektverzeichnis starten."
    }
}


# ------------------------------------------------------------ 1) Erreichbar
Schritt "Erreichbarkeit"

if (-not (Test-Connection $Rechner -Count 1 -Quiet -ErrorAction SilentlyContinue)) {
    Hinweis "Antwortet nicht auf Ping. Das kann an der Firewall liegen - weiter."
} else {
    Gut "antwortet auf Ping"
}

$winrm = Test-NetConnection -ComputerName $Rechner -Port 5985 -InformationLevel Quiet `
    -WarningAction SilentlyContinue -ErrorAction SilentlyContinue
if (-not $winrm) {
    # Ohne WinRM geht von hier aus nichts. Einschalten muss jemand am Geraet
    # oder eine Richtlinie - aus der Ferne ist das genau das Huhn-Ei-Problem.
    Schlecht "WinRM (Port 5985) nicht erreichbar."
    Write-Host @"
  Einmalig am Geraet selbst, in einer Administrator-Eingabeaufforderung:

      Enable-PSRemoting -Force

  Oder per Richtlinie fuer die OU des Geraets. Danach dieses Skript erneut.
"@ -ForegroundColor Yellow
    return
}
Gut "WinRM erreichbar (5985)"


# ----------------------------------------------------------- 2) Anmeldungen
Schritt "Anmeldung"

Write-Host "  Anmeldedaten fuer das SITZUNGSKONTO ($Domaene\$Sitzungskonto)."
Write-Host "  Damit wird die Schluesseldatei erzeugt - die Verschluesselung ist"
Write-Host "  an dieses Konto gebunden, ein anderes kann sie nicht anlegen."
$anmeldungNutzer = Get-Credential -UserName "$Domaene\$Sitzungskonto" `
    -Message "Kennwort von $Domaene\$Sitzungskonto (Leitstand-PC)"

try {
    $sitzungNutzer = New-PSSession -ComputerName $Rechner -Credential $anmeldungNutzer
} catch {
    Schlecht "Anmeldung als $Domaene\$Sitzungskonto fehlgeschlagen: $($_.Exception.Message)"
    return
}
Gut "verbunden als $Domaene\$Sitzungskonto"

# Reicht dieses Konto fuer die maschinenweiten Teile? Dann keine zweite
# Abfrage.
$istAdmin = Invoke-Command -Session $sitzungNutzer -ScriptBlock {
    $ich = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
    $ich.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}
if ($istAdmin) {
    Gut "Sitzungskonto ist dort Administrator - eine Anmeldung genuegt"
    $sitzungAdmin = $sitzungNutzer
} else {
    Hinweis "Sitzungskonto ist dort kein Administrator."
    Write-Host "  Fuer Aufgabenplanung und Energieoptionen braucht es eine zweite"
    Write-Host "  Anmeldung mit einem Administratorkonto."
    $anmeldungAdmin = Get-Credential -Message "Administrator auf $Rechner"
    try {
        $sitzungAdmin = New-PSSession -ComputerName $Rechner -Credential $anmeldungAdmin
    } catch {
        Schlecht "Administrator-Anmeldung fehlgeschlagen: $($_.Exception.Message)"
        Remove-PSSession $sitzungNutzer
        return
    }
    Gut "verbunden als Administrator"
}

try {

# --------------------------------------------------------- 3) Voraussetzungen
Schritt "Voraussetzungen auf dem Geraet"

$stand = Invoke-Command -Session $sitzungNutzer -ScriptBlock {
    $py = (Get-Command python -ErrorAction SilentlyContinue).Source
    $pyVersion = $null
    if ($py) { $pyVersion = (& $py -c "import sys;print('%d.%d' % sys.version_info[:2])").Trim() }
    $chrome = @(
        "$env:ProgramFiles\Google\Chrome\Application\chrome.exe"
        "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe"
        "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe"
    ) | Where-Object { Test-Path $_ } | Select-Object -First 1
    [pscustomobject]@{
        Rechnername = $env:COMPUTERNAME
        Konto       = "$env:USERDOMAIN\$env:USERNAME"
        Windows     = (Get-CimInstance Win32_OperatingSystem).Caption
        Python      = $py
        PythonVer   = $pyVersion
        Chrome      = $chrome
        Profil      = $env:USERPROFILE
    }
}

Write-Host "  Rechner : $($stand.Rechnername)"
Write-Host "  Windows : $($stand.Windows)"
Write-Host "  Konto   : $($stand.Konto)"
if ($stand.Python) { Gut "Python $($stand.PythonVer): $($stand.Python)" }
else { Schlecht "Python fehlt - 3.9 oder neuer installieren, dann erneut." }
if ($stand.Chrome) { Gut "Chrome: $($stand.Chrome)" }
else { Schlecht "Chrome fehlt - installieren, dann erneut." }

if (-not $stand.Python -or -not $stand.Chrome) { return }
if ($NurPruefen) {
    Hinweis "Nur-Pruefen-Lauf: erreichbar, Voraussetzungen geprueft, Ende."
    return
}


# ------------------------------------------------------------- 4) Uebertragen
Schritt "Dateien uebertragen"

Invoke-Command -Session $sitzungNutzer -ArgumentList $Verzeichnis -ScriptBlock {
    param($ziel)
    if (-not (Test-Path $ziel)) { New-Item -ItemType Directory -Path $ziel -Force | Out-Null }
}

foreach ($d in $Dateien) {
    Copy-Item (Join-Path $Quelle $d) -Destination $Verzeichnis `
        -ToSession $sitzungNutzer -Force
    Gut "uebertragen: $d"
}

# Messdaten nur, wenn dort noch keine liegen - sonst wuerde eine laufende
# Messung ueberschrieben.
foreach ($d in @("wan_traffic.csv", "monitor_state.json")) {
    $von = Join-Path $Quelle $d
    if (-not (Test-Path $von)) { Hinweis "$d fehlt hier - wird dort neu angelegt"; continue }
    $dort = Invoke-Command -Session $sitzungNutzer -ArgumentList (Join-Path $Verzeichnis $d) `
        -ScriptBlock { param($p) Test-Path $p }
    if ($dort) { Gut "bleibt unberuehrt: $d" }
    else {
        Copy-Item $von -Destination $Verzeichnis -ToSession $sitzungNutzer -Force
        Gut "uebernommen: $d"
    }
}


# ---------------------------------------------------- 5) Schluessel anlegen
Schritt "API-Schluessel"

$schluesselDa = Invoke-Command -Session $sitzungNutzer `
    -ArgumentList (Join-Path $Verzeichnis "schluessel.xml") -ScriptBlock {
    param($p)
    if (-not (Test-Path $p)) { return "fehlt" }
    try { $null = Get-Content $p -Raw | ConvertTo-SecureString; return "lesbar" }
    catch { return "unlesbar" }
}

switch ($schluesselDa) {
    "lesbar"   { Gut "Schluesseldatei vorhanden und von diesem Konto lesbar" }
    "unlesbar" {
        Hinweis "Vorhanden, aber von diesem Konto nicht lesbar - wird ersetzt."
        $schluesselDa = "fehlt"
    }
    default    { }
}

if ($schluesselDa -eq "fehlt") {
    Write-Host "  Der Schluessel wird jetzt abgefragt und auf dem Leitstand-PC"
    Write-Host "  verschluesselt abgelegt. Die Eingabe bleibt verdeckt; der"
    Write-Host "  Schluessel geht nicht durch eine Datei und in kein Protokoll."
    $schluessel = Read-Host -AsSecureString "  UniFi API-Schluessel"
    if ($schluessel.Length -eq 0) { throw "Kein Schluessel eingegeben, abgebrochen." }
    Invoke-Command -Session $sitzungNutzer `
        -ArgumentList (Join-Path $Verzeichnis "schluessel.xml"), $schluessel -ScriptBlock {
        param($pfad, $geheim)
        # Ueber WinRM kommt der SecureString verschluesselt an und wird hier
        # mit DPAPI an dieses Konto und diese Maschine gebunden.
        $geheim | ConvertFrom-SecureString | Set-Content $pfad -Encoding ASCII
    }
    Gut "Schluesseldatei auf dem Geraet angelegt"
}


# -------------------------------------------------- 6) Einrichtung ausfuehren
Schritt "Einrichtung auf dem Geraet"

# Das oertliche Skript macht die Arbeit - eine Quelle der Wahrheit, statt
# die Schritte hier ein zweites Mal zu pflegen. Es laeuft in der Sitzung mit
# den passenden Rechten und ueberspringt den Schluessel, der schon da ist.
# -Konto ausdruecklich mitgeben: laeuft dieser Teil unter einem
# Administratorkonto, wuerde das oertliche Skript sonst DESSEN Konto nehmen -
# die Aufgaben liefen auf den Administrator, die Dateirechte auch, und die
# Anzeige nie los.
$ausgabe = Invoke-Command -Session $sitzungAdmin `
    -ArgumentList $Verzeichnis, $Port, "$Domaene\$Sitzungskonto" -ScriptBlock {
    param($ziel, $port, $konto)
    Set-Location $ziel
    & (Join-Path $ziel "einrichten-leitstand.ps1") -Verzeichnis $ziel -Port $port `
        -Konto $konto 2>&1 | ForEach-Object { $_.ToString() }
}
$ausgabe | ForEach-Object { Write-Host "  | $_" }


# ---------------------------------------------------------------- 7) Probe
Schritt "Probelauf"

$probe = Invoke-Command -Session $sitzungAdmin -ArgumentList $Port -ScriptBlock {
    param($port)
    Start-ScheduledTask -TaskName "WAN-Monitor"
    $antwort = $null
    foreach ($i in 1..60) {
        Start-Sleep -Seconds 2
        try {
            $r = Invoke-WebRequest "http://127.0.0.1:$port/" -UseBasicParsing -TimeoutSec 2
            $antwort = "HTTP $($r.StatusCode), $($r.Content.Length) Zeichen"
            break
        } catch { }
    }
    $log = Get-ChildItem (Join-Path $PWD "logs") -Filter "monitor-*.log" -ErrorAction SilentlyContinue |
           Sort-Object LastWriteTime | Select-Object -Last 1
    # Kein "if" als Ausdruck in der Hashtabelle: die WinRM-Gegenstelle ist
    # standardmaessig Windows PowerShell 5.1, und dort ist das ein
    # Syntaxfehler - der Block wuerde gar nicht erst laufen.
    $zeilen = @("kein Protokoll gefunden")
    if ($log) { $zeilen = Get-Content $log.FullName -Tail 15 }
    [pscustomobject]@{
        Antwort = $antwort
        Log     = $zeilen
    }
}

if ($probe.Antwort) { Gut "Seite antwortet: $($probe.Antwort)" }
else { Schlecht "Seite antwortet nach zwei Minuten nicht." }
Write-Host "  Letzte Protokollzeilen:" -ForegroundColor White
$probe.Log | ForEach-Object { Write-Host "  | $_" }


# ---------------------------------------------------------------- Abschluss
Schritt "Noch von Hand"
Write-Host @"
  1. Kiosk starten und am Bildschirm nachsehen:
         Start-ScheduledTask -TaskName "WAN-Monitor Kiosk"

  2. Automatische Anmeldung einrichten. Das Kennwort fasst dieses Skript
     nicht an - der gaengige Weg legt es im Klartext in die Registrierung.
     Stattdessen Sysinternals Autologon (LSA-verschluesselt), einmalig
     am Geraet oder per Kopie dorthin:
         https://learn.microsoft.com/sysinternals/downloads/autologon

  3. Abmelden und neu anmelden: Monitor und Kiosk muessen ohne Zutun
     wiederkommen.

  Erst danach den GitHub-Workflow abschalten und den Auftrag bei
  cron-job.org loeschen.
"@ -ForegroundColor White

} finally {
    if ($sitzungAdmin -and $sitzungAdmin -ne $sitzungNutzer) { Remove-PSSession $sitzungAdmin }
    if ($sitzungNutzer) { Remove-PSSession $sitzungNutzer }
    Write-Host "`nVerbindungen geschlossen." -ForegroundColor DarkGray
}
