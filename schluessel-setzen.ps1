<#
.SYNOPSIS
    Setzt den UniFi-API-Schluessel auf dem Leitstand-PC, ohne ihn irgendwo
    sichtbar oder haltbar zu machen.

.BESCHREIBUNG
    Der Schluessel muss AUF dem Zielrechner verschluesselt werden - DPAPI mit
    Maschinenbindung laesst sich nicht aus der Ferne anwenden. Getippt werden
    darf er dort aber nicht: ueber PsExec ist die Konsole umgeleitet, und
    Read-Host -AsSecureString verdeckt die Eingabe dann NICHT. Nachgeprueft
    am 30.10.2026 - der Probewert stand im Klartext im Protokoll.

    Deshalb dieser Weg:

        hier getippt (verdeckt, echte Konsole)
          -> durch PsExecs Eingabekanal
          -> dort verschluesselt und abgelegt

    Der Schluessel erscheint auf keinem Bildschirm, steht in keiner
    Befehlszeile (also auch in keiner Prozessliste) und liegt zu keinem
    Zeitpunkt unverschluesselt auf einer Platte. Zur Kontrolle gibt die
    Gegenseite nur Laenge und einen kurzen Fingerabdruck zurueck.

.BEISPIEL
    .\schluessel-setzen.ps1 -Rechner ws007
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Rechner,

    [string]$Verzeichnis = "C:\WAN-Monitor",

    # PsExec64.exe. Ohne Angabe wird neben diesem Skript und im PATH gesucht.
    [string]$PsExec,

    # Anmeldung fuer PsExec, falls keine SMB-Sitzung zum Rechner besteht.
    [string]$Benutzer,

    # Den Schluessel aus der Umgebungsvariablen UI_API_KEY nehmen, statt ihn
    # abzufragen. Dann muss ihn niemand tippen oder kennen.
    [switch]$AusUmgebung
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

# Das Gegenstueck. Es liest EINE Zeile von der Standardeingabe - nicht von
# der Tastatur - und gibt nur Laenge und Fingerabdruck zurueck.
$gegenstueck = @'
param([Parameter(Mandatory = $true)][string]$Zieldatei,
      [Parameter(Mandatory = $true)][string]$Zwischendatei)
$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.Security

# Die Standardeingabe waere der sauberere Weg gewesen - PsExec reicht sie mit
# -s auf diesem System aber nicht durch (geprueft am 30.10.2026: findstr
# bekam weder ueber die PowerShell-Pipeline noch ueber eine echte OS-Pipe
# etwas zu sehen). Also eine Zwischendatei, die als ERSTES wieder weggeht.
if (-not (Test-Path $Zwischendatei)) {
    Write-Output "FEHLER: Zwischendatei fehlt"; exit 1
}
$klar = (Get-Content $Zwischendatei -Raw).Trim()
# Vor allem anderen loeschen - ueberschreiben, dann entfernen, damit der
# Klartext nicht in einem geloeschten Sektor stehen bleibt.
try {
    $muell = "0" * [Math]::Max(64, (Get-Item $Zwischendatei).Length)
    Set-Content $Zwischendatei $muell -Encoding ASCII
} catch { }
Remove-Item $Zwischendatei -Force -ErrorAction SilentlyContinue
if (-not $klar) { Write-Output "FEHLER: Zwischendatei war leer"; exit 1 }

$bytes = [Text.Encoding]::UTF8.GetBytes($klar)
try {
    $geschuetzt = [Security.Cryptography.ProtectedData]::Protect(
                      $bytes, $null, "LocalMachine")
    Set-Content $Zieldatei ([Convert]::ToBase64String($geschuetzt)) -Encoding ASCII

    # Gegenprobe: zurueckgelesen muss derselbe Wert herauskommen. Ausgegeben
    # wird NUR, ob es stimmt - nie der Wert.
    $roh = [Convert]::FromBase64String((Get-Content $Zieldatei -Raw).Trim())
    $zurueck = [Text.Encoding]::UTF8.GetString(
                   [Security.Cryptography.ProtectedData]::Unprotect(
                       $roh, $null, "LocalMachine"))
    $gleich = $zurueck -eq $klar

    # Fingerabdruck zum Vergleichen, ohne den Schluessel zu zeigen: die
    # ersten acht Zeichen des SHA-256. Daraus laesst sich der Schluessel
    # nicht zurueckrechnen.
    $hash = [Security.Cryptography.SHA256]::Create().ComputeHash($bytes)
    $finger = ([BitConverter]::ToString($hash) -replace "-","").Substring(0, 8)

    Write-Output ("Abgelegt: " + $Zieldatei)
    Write-Output ("Laenge: " + $klar.Length + " Zeichen")
    Write-Output ("Fingerabdruck (SHA-256, erste 8): " + $finger)
    Write-Output ("Gegenprobe: " + $(if ($gleich) { "stimmt" } else { "FEHLGESCHLAGEN" }))
    Write-Output ("Zwischendatei weg: " + (-not (Test-Path $Zwischendatei)))
} finally {
    [Array]::Clear($bytes, 0, $bytes.Length)
    $klar = $null
    [GC]::Collect()
}
'@

$gegenPfad = Join-Path $PSScriptRoot "schluessel-ablegen.tmp.ps1"
Set-Content $gegenPfad $gegenstueck -Encoding UTF8
$zielSkript = Join-Path $Verzeichnis "schluessel-ablegen.ps1"
$zielDatei = Join-Path $Verzeichnis "schluessel.xml"
$zwischenDatei = Join-Path $Verzeichnis "schluessel.tmp"
$freigabe = "\\$Rechner\" + ($Verzeichnis -replace ':','$')
$zwischenUnc = Join-Path $freigabe "schluessel.tmp"

Write-Host "Schluessel auf $Rechner setzen" -ForegroundColor White
Write-Host "  Zieldatei : $zielDatei"

# Das Gegenstueck hinueberkopieren. Es enthaelt keinen Schluessel - nur die
# Anweisung, einen zu verschluesseln.
Copy-Item $gegenPfad "\\$Rechner\$($Verzeichnis -replace ':','$')\schluessel-ablegen.ps1" -Force
Write-Host "  [ok] Gegenstueck uebertragen" -ForegroundColor Green

Write-Host ""
if ($AusUmgebung) {
    $ausVariable = [Environment]::GetEnvironmentVariable("UI_API_KEY")
    if (-not $ausVariable) {
        $ausVariable = [Environment]::GetEnvironmentVariable("UI_API_KEY", "User")
    }
    if (-not $ausVariable) {
        throw "UI_API_KEY ist in dieser Sitzung nicht gesetzt. Ohne -AusUmgebung " +
              "starten, dann wird der Schluessel abgefragt."
    }
    # Nur die Laenge nennen, nie den Wert.
    Write-Host ("  [ok] Schluessel aus UI_API_KEY uebernommen (" +
                $ausVariable.Trim().Length + " Zeichen)") -ForegroundColor Green
    $sicher = ConvertTo-SecureString $ausVariable.Trim() -AsPlainText -Force
    $ausVariable = $null
} else {
    Write-Host "  Jetzt den UniFi-API-Schluessel eingeben. Die Eingabe ist hier" -ForegroundColor Yellow
    Write-Host "  verdeckt (echte Konsole) und geht durch den Eingabekanal von" -ForegroundColor Yellow
    Write-Host "  PsExec zum Rechner - ohne Bildschirm, ohne Befehlszeile, ohne" -ForegroundColor Yellow
    Write-Host "  Zwischendatei." -ForegroundColor Yellow
    $sicher = Read-Host -AsSecureString "  UniFi API-Schluessel"
}
if ($sicher.Length -eq 0) { throw "Kein Schluessel vorhanden, abgebrochen." }

$argumente = @("\\$Rechner", "-s", "-accepteula", "-nobanner")
if ($Benutzer) { $argumente += @("-u", $Benutzer) }
$argumente += @("powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass",
                "-File", $zielSkript, "-Zieldatei", $zielDatei,
                "-Zwischendatei", $zwischenDatei)

$zeiger = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sicher)
try {
    $klar = [Runtime.InteropServices.Marshal]::PtrToStringAuto($zeiger)
    Write-Host ""
    Write-Host "  Der Schluessel liegt jetzt fuer wenige Sekunden unverschluesselt" -ForegroundColor Yellow
    Write-Host "  in $zwischenDatei - in einem Verzeichnis, auf das nur das" -ForegroundColor Yellow
    Write-Host "  Zielkonto, SYSTEM und Administratoren Zugriff haben. Das" -ForegroundColor Yellow
    Write-Host "  Gegenstueck loescht sie als Erstes, bevor es verschluesselt." -ForegroundColor Yellow
    # Ohne Zeilenumbruch und ohne Stuecklistenzeichen, damit die Gegenseite
    # genau die Zeichen liest, die hier stehen.
    [IO.File]::WriteAllText($zwischenUnc, $klar, [Text.UTF8Encoding]::new($false))
    & $PsExec @argumente
} finally {
    # Falls das Gegenstueck nicht lief: hier auf keinen Fall liegen lassen.
    if (Test-Path $zwischenUnc) {
        Set-Content $zwischenUnc ("0" * 256) -Encoding ASCII
        Remove-Item $zwischenUnc -Force -ErrorAction SilentlyContinue
        Write-Warning "Zwischendatei musste von hier entfernt werden - das Gegenstueck lief nicht."
    }
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($zeiger)
    $klar = $null
    [GC]::Collect()
    Remove-Item $gegenPfad -Force -ErrorAction SilentlyContinue
}

Write-Host ""
Write-Host "  Das Gegenstueck auf dem Rechner kann weg:" -ForegroundColor DarkGray
Write-Host "      Remove-Item '\\$Rechner\$($Verzeichnis -replace ':','$')\schluessel-ablegen.ps1'" -ForegroundColor DarkGray
