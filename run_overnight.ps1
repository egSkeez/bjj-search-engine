##############################################################
# BJJ Search Engine — Overnight Run Script
# Starts backend, frontend, runs ingest, then semantic chunks
##############################################################

$ROOT    = "C:\Users\Skeez\Documents\chunking"
$BACKEND = "$ROOT\backend"
$FRONTEND= "$ROOT\frontend"
$LOGS    = "$ROOT\logs"

New-Item -ItemType Directory -Force -Path $LOGS | Out-Null

Write-Host ""
Write-Host "========================================================"
Write-Host "  BJJ Search Engine — Overnight Run"
Write-Host "  $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Host "========================================================"
Write-Host ""

# ── 1. Backend (uvicorn) ──────────────────────────────────────
Write-Host "[1/4] Starting backend (uvicorn)..."
$backendLog = "$LOGS\backend.log"
$backendJob = Start-Process pwsh -ArgumentList @(
    "-NoProfile", "-Command",
    "Set-Location '$BACKEND'; python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload 2>&1 | Tee-Object -FilePath '$backendLog'"
) -PassThru -WindowStyle Minimized

# Wait until backend is actually accepting connections
$timeout = 30; $waited = 0
do {
    Start-Sleep -Seconds 2; $waited += 2
    try { $r = Invoke-WebRequest -Uri "http://localhost:8000/health" -TimeoutSec 2 -UseBasicParsing -ErrorAction Stop; break }
    catch { Write-Host "  Waiting for backend... ${waited}s" }
} while ($waited -lt $timeout)
Write-Host "  Backend ready at http://localhost:8000"

# ── 2. Frontend (Next.js) ─────────────────────────────────────
Write-Host ""
Write-Host "[2/4] Starting frontend (Next.js)..."
$frontendLog = "$LOGS\frontend.log"
$frontendJob = Start-Process pwsh -ArgumentList @(
    "-NoProfile", "-Command",
    "Set-Location '$FRONTEND'; npm run dev 2>&1 | Tee-Object -FilePath '$frontendLog'"
) -PassThru -WindowStyle Minimized
Write-Host "  Frontend starting at http://localhost:3000 (check $frontendLog)"

# ── 3. Ingest all volumes ────────────────────────────────────
Write-Host ""
Write-Host "[3/4] Starting ingest_all.py — this runs all night..."
Write-Host "  Log: $LOGS\ingest.log"
Write-Host "  (window will stay open and show live progress)"
Write-Host ""

$ingestLog = "$LOGS\ingest.log"
Set-Location $BACKEND
python ingest_all.py 2>&1 | Tee-Object -FilePath $ingestLog

Write-Host ""
Write-Host "========================================================"
Write-Host "  ingest_all.py FINISHED at $(Get-Date -Format 'HH:mm:ss')"
Write-Host "========================================================"

# ── 4. Semantic chunks (music-boundary technique detection) ──
Write-Host ""
Write-Host "[4/4] Building semantic chunks for all ingested volumes..."
Write-Host "  Log: $LOGS\semantic.log"
Write-Host ""

$semanticLog = "$LOGS\semantic.log"
python create_semantic_chunks.py 2>&1 | Tee-Object -FilePath $semanticLog

Write-Host ""
Write-Host "========================================================"
Write-Host "  ALL DONE at $(Get-Date -Format 'HH:mm:ss')"
Write-Host "  Ingest log  : $ingestLog"
Write-Host "  Semantic log: $semanticLog"
Write-Host "  Backend log : $backendLog"
Write-Host "========================================================"
Write-Host ""
Read-Host "Press Enter to exit"
