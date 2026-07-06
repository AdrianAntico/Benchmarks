[CmdletBinding()]
param(
    [switch]$InstallJava,
    [switch]$ForceRecreate
)

$ErrorActionPreference = "Stop"
$SparkDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $SparkDirectory
$VirtualEnvironment = Join-Path $RepoRoot ".venv-spark"
$Python = Join-Path $VirtualEnvironment "Scripts\python.exe"

function Test-Command {
    param([Parameter(Mandatory)][string]$Name)
    return $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

function Find-Java17Home {
    $javaHomes = @(
        Get-ChildItem "C:\Program Files\Microsoft" -Directory -Filter "jdk-17*" -ErrorAction SilentlyContinue
        Get-ChildItem "C:\Program Files\Eclipse Adoptium" -Directory -Filter "jdk-17*" -ErrorAction SilentlyContinue
        Get-ChildItem "C:\Program Files\Java" -Directory -Filter "jdk-17*" -ErrorAction SilentlyContinue
    ) | Sort-Object LastWriteTime -Descending

    if ($javaHomes) {
        return $javaHomes[0].FullName
    }
    return $null
}

function Get-JavaMajorVersion {
    if (-not (Test-Command "java")) {
        return 0
    }

    $versionText = (& java -version 2>&1 | Out-String)
    if ($versionText -notmatch 'version\s+"(?<version>\d+(?:\.\d+)*)') {
        throw "Could not determine the installed Java version from: $versionText"
    }

    $parts = $Matches.version.Split(".")
    if ($parts[0] -eq "1" -and $parts.Count -gt 1) {
        return [int]$parts[1]
    }
    return [int]$parts[0]
}

function Set-Java17Home {
    $javaHome = Find-Java17Home

    if (-not $javaHome) {
        throw "Java 17 was installed but JAVA_HOME could not be located. Restart PowerShell and rerun this script."
    }

    $env:JAVA_HOME = $javaHome
    $env:Path = "$(Join-Path $javaHome 'bin');$env:Path"
    [Environment]::SetEnvironmentVariable("JAVA_HOME", $javaHome, "User")
    Write-Host "JAVA_HOME=$javaHome"
}

if (-not (Test-Command "python")) {
    throw "Python is not available on PATH. Install Python 3.9 or newer, then rerun this script."
}

$pythonVersion = python -c "import sys; print('.'.join(map(str, sys.version_info[:3])))"
Write-Host "Python $pythonVersion"

$javaMajorVersion = Get-JavaMajorVersion
if ($javaMajorVersion -lt 17) {
    if (-not $InstallJava) {
        if ($javaMajorVersion -eq 0) {
            throw "Java was not found. Rerun with: .\Spark\Setup_Spark.ps1 -InstallJava"
        }
        throw "Java $javaMajorVersion is active, but PySpark 4 requires Java 17 or newer. Rerun with: .\Spark\Setup_Spark.ps1 -InstallJava"
    }

    if (-not (Find-Java17Home)) {
        if (-not (Test-Command "winget")) {
            throw "winget is unavailable. Install a Java 17 JDK, set JAVA_HOME, and rerun this script."
        }

        Write-Host "Installing Microsoft OpenJDK 17..."
        winget install --id Microsoft.OpenJDK.17 --exact --accept-package-agreements --accept-source-agreements
        if ($LASTEXITCODE -ne 0) {
            throw "The Java installation failed with exit code $LASTEXITCODE."
        }
    }
    Set-Java17Home
}

java -version
if ($LASTEXITCODE -ne 0) {
    throw "Java is installed but could not start. Check JAVA_HOME and PATH."
}
$javaMajorVersion = Get-JavaMajorVersion
if ($javaMajorVersion -lt 17) {
    throw "Java $javaMajorVersion is still active. Close PowerShell, open a new terminal, and rerun this setup."
}
Write-Host "Java $javaMajorVersion is compatible with PySpark 4."

if ($ForceRecreate -and (Test-Path $VirtualEnvironment)) {
    $resolvedRepo = (Resolve-Path $RepoRoot).Path
    $resolvedVenv = (Resolve-Path $VirtualEnvironment).Path
    if (-not $resolvedVenv.StartsWith($resolvedRepo + [IO.Path]::DirectorySeparatorChar)) {
        throw "Refusing to remove a virtual environment outside the repository."
    }
    Remove-Item -LiteralPath $resolvedVenv -Recurse -Force
}

if (-not (Test-Path $Python)) {
    Write-Host "Creating $VirtualEnvironment..."
    python -m venv $VirtualEnvironment
}

Write-Host "Installing PySpark..."
& $Python -m pip install --upgrade pip
& $Python -m pip install --requirement (Join-Path $SparkDirectory "requirements.txt")

Write-Host "Running a local Spark smoke test..."
& $Python (Join-Path $SparkDirectory "Verify_Spark.py")
if ($LASTEXITCODE -ne 0) {
    throw "PySpark installed, but the smoke test failed. Review the error above."
}

Write-Host ""
Write-Host "Spark is ready. Run a benchmark with:"
Write-Host ".\.venv-spark\Scripts\python.exe .\Spark\RollingJoin_Spark.py"
