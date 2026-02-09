# Upload PMTiles to Cloudflare R2
# Usage: .\upload-pmtiles.ps1 [-BucketName "grid3-tiles"]

param(
    [string]$BucketName = "grid3-tiles",
    [string]$PMTilesDir = "..\..\data\3-pmtiles"
)

Write-Host "🚀 Uploading PMTiles to R2 bucket: $BucketName" -ForegroundColor Cyan
Write-Host "📁 Source directory: $PMTilesDir" -ForegroundColor Cyan
Write-Host ""

# Check if wrangler is installed
try {
    $null = Get-Command wrangler -ErrorAction Stop
} catch {
    Write-Host "❌ Error: wrangler is not installed" -ForegroundColor Red
    Write-Host "Install it with: npm install -g wrangler" -ForegroundColor Yellow
    exit 1
}

# Check if PMTiles directory exists
if (-not (Test-Path $PMTilesDir)) {
    Write-Host "❌ Error: PMTiles directory not found: $PMTilesDir" -ForegroundColor Red
    exit 1
}

# Get all PMTiles files
$pmtilesFiles = Get-ChildItem -Path $PMTilesDir -Filter "*.pmtiles"

if ($pmtilesFiles.Count -eq 0) {
    Write-Host "❌ Error: No .pmtiles files found in $PMTilesDir" -ForegroundColor Red
    exit 1
}

Write-Host "Found $($pmtilesFiles.Count) PMTiles files to upload" -ForegroundColor Green
Write-Host ""

# Upload each file
$uploadCount = 0
foreach ($file in $pmtilesFiles) {
    $filename = $file.Name
    $filesize = "{0:N2} MB" -f ($file.Length / 1MB)
    
    Write-Host "📤 Uploading: $filename ($filesize)" -ForegroundColor White
    
    try {
        $result = & wrangler r2 object put "$BucketName/$filename" --file $file.FullName 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   ✅ Success" -ForegroundColor Green
            $uploadCount++
        } else {
            Write-Host "   ❌ Failed: $result" -ForegroundColor Red
        }
    } catch {
        Write-Host "   ❌ Failed: $_" -ForegroundColor Red
    }
    Write-Host ""
}

Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Cyan
Write-Host "✨ Upload complete: $uploadCount/$($pmtilesFiles.Count) files uploaded" -ForegroundColor Green
Write-Host ""
Write-Host "To verify uploads:" -ForegroundColor Yellow
Write-Host "  wrangler r2 object list $BucketName" -ForegroundColor White
Write-Host ""
Write-Host "To test a tile:" -ForegroundColor Yellow
Write-Host "  curl https://your-worker.workers.dev/base.json" -ForegroundColor White
