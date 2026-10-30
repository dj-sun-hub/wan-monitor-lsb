<#
.SYNOPSIS
    Richtet die automatische Anmeldung auf dem Leitstand-PC ein.

.BESCHREIBUNG
    Damit der Wandbildschirm nach einem Neustart ohne Zutun wieder dasteht,
    muss sich das Konto selbst anmelden.

    Das Kennwort wird HIER verdeckt eingegeben - auf einer echten Konsole, wo
    die Maskierung funktioniert - und auf dem Zielrechner von Sysinternals
    Autologon LSA-verschluesselt abgelegt. Nicht als Winlogon\DefaultPassword:
    das waere Klartext in der Registrierung, lesbar fuer jeden lokalen
    Administrator und bei Fernzugriff auf die Registrierung auch von weiter weg.

    Der Weg ist derselbe wie beim API-Schluessel (schluessel-setzen.ps1) und aus
    demselben Grund: PsExec reicht die Standardeingabe mit -s nicht durch, und
    am Geraet getippt wird sie nicht verdeckt. Also eine Zwischendatei, die das
    Gegenstueck als ERSTES ueberschreibt und loescht.

    Was bleibt, offen gesagt: das Kennwort liegt wenige Sekunden
    unverschluesselt in einer Datei und steht fuer einen Moment auf der
    Befehlszeile von Autologon, also in der Prozessliste des Zielrechners.
    Beides in einem Verzeichnis bzw. auf einem Geraet, auf das nur das
    Zielkonto, SYSTEM und Administratoren Zugriff haben. Wer das nicht will,
    muss Autologon von Hand am Geraet starten.

    Die Messung haengt NICHT hieran: sie laeuft als Dienst beim Systemstart
    (siehe EINRICHTUNG-LEITSTAND-PC.md). Dieses Skript betrifft nur das Bild.

.BEISPIEL
    .\autologin-setzen.ps1 -Rechner ws007 -Benutzer j.eismann -PsExec C:\ProgramData\WAN-Monitor\PsExec64.exe
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Rechner,

    # Das Konto, das sich automatisch anmelden soll (ohne Domaene).
    [Parameter(Mandatory = $true)]
    [string]$Benutzer,

    [string]$Domaene = $env:USERDOMAIN,

    [string]$Verzeichnis = "C:\WAN-Monitor",

    # PsExec64.exe.
    [string]$PsExec,

    # Autologon64.exe, falls noch nicht auf dem Zielrechner.
    [string]$Autologon,

    # Anmeldung fuer PsExec, falls keine SMB-Sitzung besteht.
    [string]$Anmeldung,

    # Nur den jetzigen Stand zeigen, nichts aendern.
    [switch]$NurPruefen
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if (-not $PsExec) {
    $PsExec = (Get-Command PsExec64.exe, PsExec.exe -ErrorAction SilentlyContinue |
               Select-Object -First 1).Source
}
if (-not $PsExec -or -not (Test-Path $PsExec)) {
    throw "PsExec nicht gefunden. Mit -PsExec den Pfad angeben."
}

$freigabe = "\\$Rechner\" + ($Verzeichnis -replace ':','$')
$zwischenUnc = Join-Path $freigabe "autologin.tmp"
$zwischenDatei = Join-Path $Verzeichnis "autologin.tmp"
$gegenDatei = Join-Path $Verzeichnis "autologin-ablegen.ps1"
$autologonZiel = Join-Path $Verzeichnis "Autologon64.exe"

function Gut($t) { Write-Host "  [ok] $t" -ForegroundColor Green }
function Hinweis($t) { Write-Host "  [--] $t" -ForegroundColor Yellow }

Write-Host "Automatische Anmeldung auf $Rechner" -ForegroundColor White
Write-Host "  Konto : $Domaene\$Benutzer"

if (-not (Test-Path $freigabe)) {
    throw "Freigabe $freigabe nicht erreichbar. Erst verbinden: net use '$freigabe' /user:<DOMAENE>\<Admin> *"
}

# Das Gegenstueck. Es liest das Kennwort aus der Zwischendatei, loescht sie
# als Erstes und ruft Autologon auf. Ausgegeben wird nur, was ueberpruefbar
# ist - nie das Kennwort.
#
# Die Werte kommen aus einer Datei NEBEN dem Gegenstueck, nicht als
# Parameter. Ueber "cmd /c" durch PsExec hindurch ueberlebten die
# Anfuehrungszeichen nicht: der Aufruf scheiterte an der Parameterbindung
# ("kein Positionsparameter gefunden, der das Argument '-Domaene'
# akzeptiert"), und Autologon lief nie - waehrend das Kennwort schon in der
# Zwischendatei lag. In der Wertedatei steht kein Geheimnis, nur Pfade und
# der Kontoname.
$gegenstueck = @'
$ErrorActionPreference = "Stop"
$werte = Get-Content (Join-Path $PSScriptRoot "autologin-werte.json") -Raw | ConvertFrom-Json
$Zwischendatei = $werte.Zwischendatei
$Benutzer      = $werte.Benutzer
$Domaene       = $werte.Domaene
$Autologon     = $werte.Autologon

if (-not (Test-Path $Zwischendatei)) { Write-Output "FEHLER: Zwischendatei fehlt"; exit 1 }
$klar = (Get-Content $Zwischendatei -Raw)
# Zeilenende abschneiden, aber KEIN Trim: ein Kennwort darf mit Leerzeichen
# anfangen oder enden.
$klar = $klar -replace "`r?`n$", ""

# Vor allem anderen weg: ueberschreiben, dann loeschen.
try {
    Set-Content $Zwischendatei ("0" * 512) -Encoding ASCII
} catch { }
Remove-Item $Zwischendatei -Force -ErrorAction SilentlyContinue
if (-not $klar) { Write-Output "FEHLER: Zwischendatei war leer"; exit 1 }

try {
    # Autologon legt das Kennwort als LSA-Geheimnis ab, nicht in die
    # Registrierung. Die Ausgabe wird verworfen: sie wiederholt die
    # Befehlszeile und damit das Kennwort.
    $null = & $Autologon -accepteula $Benutzer $Domaene $klar 2>&1
    $code = $LASTEXITCODE
} finally {
    $klar = $null
    [GC]::Collect()
}

$w = "HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Winlogon"
$j = Get-ItemProperty $w
Write-Output ("Autologon-Rueckgabe       : " + $code)
Write-Output ("AutoAdminLogon            : " + $(if ($j.PSObject.Properties.Name -contains "AutoAdminLogon") { $j.AutoAdminLogon } else { "fehlt" }))
Write-Output ("DefaultUserName           : " + $(if ($j.PSObject.Properties.Name -contains "DefaultUserName") { $j.DefaultUserName } else { "fehlt" }))
Write-Output ("DefaultDomainName         : " + $(if ($j.PSObject.Properties.Name -contains "DefaultDomainName") { $j.DefaultDomainName } else { "fehlt" }))
# Das Entscheidende: KEIN Klartextkennwort in der Registrierung.
if ($j.PSObject.Properties.Name -contains "DefaultPassword") {
    Write-Output "DefaultPassword           : VORHANDEN - Klartext! wird entfernt"
    Remove-ItemProperty $w -Name DefaultPassword -Force
    Write-Output "DefaultPassword           : entfernt"
} else {
    Write-Output "DefaultPassword           : nicht vorhanden (richtig, LSA statt Registrierung)"
}
Write-Output ("Zwischendatei weg         : " + (-not (Test-Path $Zwischendatei)))
'@

# ------------------------------------------------------------- Nur pruefen
if ($NurPruefen) {
    $p = @'
$w = "HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Winlogon"
$j = Get-ItemProperty $w
foreach ($n in "AutoAdminLogon","DefaultUserName","DefaultDomainName") {
  Write-Output ("  $n = " + $(if ($j.PSObject.Properties.Name -contains $n) { $j.$n } else { "fehlt" }))
}
Write-Output ("  DefaultPassword (Klartext) = " + $(if ($j.PSObject.Properties.Name -contains "DefaultPassword") { "VORHANDEN!" } else { "nicht vorhanden" }))
'@
    Set-Content (Join-Path $freigabe "autologin-stand.ps1") $p -Encoding UTF8
    Write-Host "  Stand abfragen:" -ForegroundColor White
    $arg = @("\\$Rechner", "-s", "-accepteula", "-nobanner")
    if ($Anmeldung) { $arg += @("-u", $Anmeldung) }
    $arg += @("cmd", "/c", "powershell -NoProfile -ExecutionPolicy Bypass -File $Verzeichnis\autologin-stand.ps1 > $Verzeichnis\logs\autologin-stand.txt 2>&1")
    & $PsExec @arg | Out-Null
    Get-Content (Join-Path $freigabe "logs\autologin-stand.txt")
    return
}

# --------------------------------------------------------- Autologon dorthin
if (Test-Path (Join-Path $freigabe "Autologon64.exe")) {
    Gut "Autologon liegt schon auf dem Rechner"
} elseif ($Autologon) {
    if (-not (Test-Path $Autologon)) { throw "Autologon nicht gefunden: $Autologon" }
    $sig = Get-AuthenticodeSignature $Autologon
    if ($sig.Status -ne "Valid") { throw "Autologon ist nicht gueltig signiert ($($sig.Status))." }
    Copy-Item $Autologon (Join-Path $freigabe "Autologon64.exe") -Force
    Gut "Autologon uebertragen (Signatur geprueft: $($sig.Status))"
} else {
    throw "Autologon fehlt auf dem Rechner. Mit -Autologon den Pfad zu Autologon64.exe angeben."
}

$gegenPfad = Join-Path $PSScriptRoot "autologin-ablegen.tmp.ps1"
Set-Content $gegenPfad $gegenstueck -Encoding UTF8
Copy-Item $gegenPfad (Join-Path $freigabe "autologin-ablegen.ps1") -Force
# Pfade und Kontoname - kein Geheimnis. Dass sie als Datei statt als
# Parameter gehen, ist die Lehre aus dem Fehlversuch (siehe oben).
@{
    Zwischendatei = $zwischenDatei
    Benutzer      = $Benutzer
    Domaene       = $Domaene
    Autologon     = $autologonZiel
} | ConvertTo-Json | Set-Content (Join-Path $freigabe "autologin-werte.json") -Encoding UTF8
Gut "Gegenstueck und Werte uebertragen"

# ------------------------------------------------------------- Kennwort
Write-Host ""
Write-Host "  Kennwort von $Domaene\$Benutzer eingeben. Die Eingabe ist hier" -ForegroundColor Yellow
Write-Host "  verdeckt. Es liegt dann wenige Sekunden unverschluesselt in" -ForegroundColor Yellow
Write-Host "  $zwischenDatei und steht fuer einen Moment auf der" -ForegroundColor Yellow
Write-Host "  Befehlszeile von Autologon. Danach nur noch LSA-verschluesselt." -ForegroundColor Yellow
$sicher = Read-Host -AsSecureString "  Kennwort"
if ($sicher.Length -eq 0) { throw "Kein Kennwort eingegeben, abgebrochen." }

$arg = @("\\$Rechner", "-s", "-accepteula", "-nobanner")
if ($Anmeldung) { $arg += @("-u", $Anmeldung) }
$arg += @("cmd", "/c",
          "powershell -NoProfile -ExecutionPolicy Bypass -File $gegenDatei " +
          "> $Verzeichnis\logs\autologin.txt 2>&1")

$zeiger = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sicher)
try {
    $klar = [Runtime.InteropServices.Marshal]::PtrToStringAuto($zeiger)
    [IO.File]::WriteAllText($zwischenUnc, $klar, [Text.UTF8Encoding]::new($false))
    Write-Host ""
    & $PsExec @arg | Out-Null
} finally {
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($zeiger)
    $klar = $null
    [GC]::Collect()
    Remove-Item $gegenPfad -Force -ErrorAction SilentlyContinue
    # Falls das Gegenstueck nicht lief: auf keinen Fall liegen lassen.
    if (Test-Path $zwischenUnc) {
        Set-Content $zwischenUnc ("0" * 512) -Encoding ASCII
        Remove-Item $zwischenUnc -Force -ErrorAction SilentlyContinue
        Write-Warning "Zwischendatei musste von hier entfernt werden - das Gegenstueck lief nicht."
    }
}

Write-Host "  Ergebnis vom Rechner:" -ForegroundColor White
Get-Content (Join-Path $freigabe "logs\autologin.txt") | ForEach-Object { "  | $_" }

# Das Gegenstueck hat seinen Zweck getan.
Remove-Item (Join-Path $freigabe "autologin-ablegen.ps1"),
            (Join-Path $freigabe "autologin-werte.json") -Force -ErrorAction SilentlyContinue

Write-Host ""
Write-Host @"
  Zum Abschluss der Neustart - er prueft die ganze Kette auf einmal:
  Anmeldung von selbst, Messung als Dienst, Kiosk im Vollbild.

  Und eine Dauerregel: nach jedem Kennwortwechsel dieses Kontos muss das
  Autologin neu gesetzt werden, sonst bleibt der Bildschirm beim naechsten
  Neustart auf der Anmeldung stehen. Die MESSUNG laeuft weiter - sie haengt
  nicht an der Anmeldung.
"@ -ForegroundColor White
