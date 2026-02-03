$headers = @{
    'Accept' = 'application/json'
    'clientToken' = 'YOUR_CLIENT_TOKEN'
}

Invoke-RestMethod -Uri "https://suzgrid.crpt.ru:443/api/v3/ping?omsId=YOUR_OMS_ID" -Headers $headers -Method GET
