# --- 1. Settings ---
$INN = "7733154124"
$conId = "90a75021-a56e-415c-b5d1-daabb66002b9"
$userProfile = $env:USERPROFILE
$workDir = "$userProfile\tst"

# File Paths
$signDataJsonPath = "$userProfile\sign_data.json"
$dataToSignPath = "$workDir\$($INN)_dataToSign.txt"
$signaturePath = "$workDir\$($INN)_dataToSign.txt.sig"
$getTokenJsonPath = "$userProfile\get_token.json"

# Create directory if not exists
if (!(Test-Path $workDir)) { 
    New-Item -ItemType Directory -Path $workDir | Out-Null 
}

# Remove old signature if exists
if (Test-Path $signaturePath) { 
    Remove-Item $signaturePath -Force 
}

# --- 2. Get Auth Data from CRPT ---
Write-Host "Requesting data from Chestny Znak..." -ForegroundColor Cyan
try {
    $authKeyResponse = Invoke-RestMethod -Uri "https://markirovka.crpt.ru/api/v3/true-api/auth/key" -Method Get
    
    # Save uuid to temp file
    $authKeyResponse | ConvertTo-Json | Out-File $signDataJsonPath -Encoding utf8

    # Create file for the signing daemon
    $authKeyResponse.data | Out-File $dataToSignPath -Encoding utf8 -NoNewline
    Write-Host "File created: $dataToSignPath. Waiting for daemon..." -ForegroundColor Yellow
}
catch {
    Write-Error "Failed to get auth key: $_"
    exit
}

# --- 3. Wait for Signature (.sig) ---
$timeout = 0
while (!(Test-Path $signaturePath)) {
    Start-Sleep -Seconds 2
    $timeout++
    if ($timeout -gt 30) {
        Write-Error "Timeout: Daemon did not sign the file within 60s."
        exit
    }
}
Write-Host "Signature file detected!" -ForegroundColor Green
Start-Sleep -Milliseconds 500

# --- 4. Create get_token.json ---
$signatureBody = Get-Content $signaturePath -Raw

if ($null -ne $signatureBody -and $signatureBody -ne "") {
    $signatureBody = $signatureBody.Trim()

    $finalJsonObject = @{
        "uuid" = $authKeyResponse.uuid
        "data" = $signatureBody
        "inn"  = $INN
    }

    $finalJsonObject | ConvertTo-Json | Out-File $getTokenJsonPath -Encoding utf8
    Write-Host "File get_token.json created successfully." -ForegroundColor Green
}
else {
    Write-Error "Signature file is empty."
    exit
}

# --- 5. Send to Chestny Znak ---
Write-Host "Sending token to SUZ..." -ForegroundColor Cyan
try {
    $params = @{
        Uri         = "https://markirovka.crpt.ru/api/v3/true-api/auth/simpleSignIn/$conId"
        Method      = 'Post'
        Body        = (Get-Content $getTokenJsonPath -Raw)
        ContentType = 'application/json'
    }

    $tokenResponse = Invoke-RestMethod @params
    $tokenResponse | ConvertTo-Json | Out-File "$userProfile\token_$conId.json" -Encoding utf8
    Write-Host "Done! Token saved to token_$conId.json" -ForegroundColor Green
}
catch {
    Write-Error "Error sending to CRPT: $_"
}