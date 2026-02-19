# --- НАСТРОЙКИ ---
$TokenFile     = "C:\Users\project\token.txt"
$DocFile       = "C:\Users\project\tst\7733154124_rep.json"
$SigFile       = "C:\Users\project\tst\7733154124_rep.json.sig"
$DocType       = "AGGREGATION_DOCUMENT" 
$Url           = "https://markirovka.crpt.ru/api/v3/true-api/lk/documents/create?pg=chemistry"


try {
    Write-Host "[1/3] Loading components..." -ForegroundColor Cyan

    # 1. Read Token
    if (-not (Test-Path $TokenFile)) { throw "Token file not found" }
    $TokenRaw = [System.IO.File]::ReadAllText((Resolve-Path $TokenFile))
    $Token = $TokenRaw.Trim() -replace '[^\x20-\x7E]', '' 

    # 2. Read Document as BYTES
    if (-not (Test-Path $DocFile)) { throw "Doc file not found" }
    $DocBytes = [System.IO.File]::ReadAllBytes($DocFile)
    $DocBase64 = [Convert]::ToBase64String($DocBytes)

    # 3. Read Signature as TEXT
    if (-not (Test-Path $SigFile)) { throw "Signature file not found" }
    $SigRaw = [System.IO.File]::ReadAllText((Resolve-Path $SigFile))
    $SigBase64 = $SigRaw.Trim() -replace '\s+', '' 

    Write-Host "[2/3] Building JSON..." -ForegroundColor Cyan

    $BodyObj = @{
        document_format  = "MANUAL"
        product_document = $DocBase64
        type             = $DocType
        signature        = $SigBase64
    }
    
    $JsonBody = $BodyObj | ConvertTo-Json -Compress

    Write-Host "[3/3] Sending to CRPT..." -ForegroundColor Cyan

    $Headers = @{
        "Authorization" = "Bearer $Token"
        "Accept"        = "application/json"
    }

    $Response = Invoke-RestMethod -Method Post -Uri $Url -Headers $Headers -Body $JsonBody -ContentType "application/json; charset=utf-8"

    Write-Host "SUCCESS! Document accepted." -ForegroundColor Green
    $Response | Format-List
}
catch {
    Write-Host "ERROR OCCURRED:" -ForegroundColor Red
    if ($_.Exception.Response) {
        $Reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
        $RawError = $Reader.ReadToEnd()
        Write-Host "Server Response: $RawError" -ForegroundColor Yellow
    } else {
        Write-Host $_.Exception.Message -ForegroundColor Yellow
    }
}

Write-Host "`nPress any key to exit..."
$null = [System.Console]::ReadKey()