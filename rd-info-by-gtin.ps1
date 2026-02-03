# Script parameters
$csvFilePath = "gtins.csv"  # Path to CSV file with GTINs
$inn = "YOUR_INN"  # INN parameter
$tokenFilePath = "token.txt"
$outputDirectory = "results"   # Directory for results

# Create output directory if not exists
if (!(Test-Path $outputDirectory)) {
    New-Item -ItemType Directory -Path $outputDirectory -Force
    Write-Host "Created directory: $outputDirectory"
}

# Read token from file
$token = (Get-Content -Path $tokenFilePath -Raw).Trim()
$headers = @{
    'Authorization' = "Bearer $token"
    'Content-Type' = 'application/json'
}

# Base API URL
$baseUri = "https://xn--80aqu.xn----7sbabas4ajkhfocclk9d3cvfsa.xn--p1ai:443/v4/rd-info-by-gtin"

# Function for single request
function Invoke-RDInfoRequest {
    param(
        [string]$gtin,
        [string]$inn,
        [hashtable]$headers,
        [string]$baseUri
    )

    $body = @{
        gtin = $gtin
        inn = $inn
    } | ConvertTo-Json

    try {
        Write-Host "Request for GTIN: $gtin" -ForegroundColor Yellow

        $response = Invoke-RestMethod -Uri $baseUri -Method "POST" -Headers $headers -Body $body

        return @{
            Success = $true
            Data = $response
            Error = $null
        }
    }
    catch {
        Write-Host "Error for GTIN: $gtin - $($_.Exception.Message)" -ForegroundColor Red

        return @{
            Success = $false
            Data = $null
            Error = $_.Exception.Message
        }
    }
}

# Function to save result with GTIN and INN in filename
function Save-RDInfoResult {
    param(
        [string]$gtin,
        [string]$inn,
        [object]$result,
        [string]$outputDirectory
    )

    # Create filename with GTIN and INN
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $fileName = "RD_Info_${gtin}_${inn}_${timestamp}.json"
    $fullPath = Join-Path $outputDirectory $fileName

    try {
        $outputData = @{
            Metadata = @{
                GTIN = $gtin
                INN = $inn
                Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
                Success = $result.Success
            }
            Result = $result.Data
            Error = $result.Error
        }

        $outputData | ConvertTo-Json -Depth 10 | Out-File -FilePath $fullPath -Encoding UTF8

        Write-Host "Result saved: $fullPath" -ForegroundColor Green
        return $fullPath
    }
    catch {
        Write-Host "Save error for GTIN: $gtin - $($_.Exception.Message)" -ForegroundColor Red
        return $null
    }
}

# Main execution process

# Read GTINs from CSV file
try {
    $gtinData = Import-Csv -Path $csvFilePath
    Write-Host "Loaded $($gtinData.Count) GTINs from CSV" -ForegroundColor Green
}
catch {
    Write-Host "CSV read error: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Check if GTIN column exists in CSV
if ($gtinData[0].PSObject.Properties.Name -notcontains "GTIN") {
    Write-Host "CSV file missing 'GTIN' column" -ForegroundColor Red
    Write-Host "Available columns: $($gtinData[0].PSObject.Properties.Name -join ', ')" -ForegroundColor Yellow
    exit 1
}

$processedResults = @()
$counter = 0

foreach ($item in $gtinData) {
    $counter++
    $gtin = $item.GTIN.Trim()

    Write-Host "Processing $counter of $($gtinData.Count): GTIN $gtin" -ForegroundColor Magenta

    # Execute request
    $result = Invoke-RDInfoRequest -gtin $gtin -inn $inn -headers $headers -baseUri $baseUri

    # Save result
    $filePath = Save-RDInfoResult -gtin $gtin -inn $inn -result $result -outputDirectory $outputDirectory

    # Store processing info
    $processedResults += [PSCustomObject]@{
        GTIN = $gtin
        INN = $inn
        Success = $result.Success
        Error = if ($result.Success) { "N/A" } else { $result.Error }
        FilePath = $filePath
        Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    }

    # Pause between requests (1 second)
    if ($counter -lt $gtinData.Count) {
        Write-Host "Pause 1 second..." -ForegroundColor Gray
        Start-Sleep -Seconds 1
    }
}

# Create summary report
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$summaryFile = Join-Path $outputDirectory "RD_Info_Summary_${inn}_${timestamp}.csv"

$processedResults | Export-Csv -Path $summaryFile -Encoding UTF8 -NoTypeInformation
Write-Host "Summary report saved: $summaryFile" -ForegroundColor Cyan

# Final statistics
$successCount = ($processedResults | Where-Object { $_.Success -eq $true }).Count
$errorCount = ($processedResults | Where-Object { $_.Success -eq $false }).Count

Write-Host "`n" + "="*50 -ForegroundColor Green
Write-Host "PROCESSING COMPLETED" -ForegroundColor Green
Write-Host "Total GTINs processed: $($gtinData.Count)" -ForegroundColor White
Write-Host "Successful: $successCount" -ForegroundColor Green
Write-Host "Errors: $errorCount" -ForegroundColor Red
Write-Host "Summary report: $summaryFile" -ForegroundColor Cyan
Write-Host "="*50 -ForegroundColor Green

# Function to view results
function Show-RDResults {
    param(
        [string]$searchPath = $outputDirectory
    )

    $files = Get-ChildItem -Path $searchPath -Filter "RD_Info_*_${inn}_*.json" | Sort-Object LastWriteTime -Descending

    foreach ($file in $files) {
        Write-Host "`nFile: $($file.Name)" -ForegroundColor Yellow
        $content = Get-Content $file.FullName -Encoding UTF8 | ConvertFrom-Json

        if ($content.Metadata.Success) {
            Write-Host "Status: Success" -ForegroundColor Green
            if ($content.Result.result.documents) {
                Write-Host "Documents found: $($content.Result.result.documents.Count)" -ForegroundColor White
                foreach ($doc in $content.Result.result.documents) {
                    Write-Host "  - $($doc.number) (Type: $($doc.attr_id))" -ForegroundColor Gray
                }
            } else {
                Write-Host "No documents found" -ForegroundColor Yellow
            }
        } else {
            Write-Host "Status: Error" -ForegroundColor Red
            Write-Host "Error: $($content.Error)" -ForegroundColor Red
        }
    }
}

Write-Host "`nTo view results run: Show-RDResults" -ForegroundColor Cyan
