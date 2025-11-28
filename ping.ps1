$headers = @{     'Accept' = 'application/json'
'clientToken' = 'd4cc95f1-9a14-42b6-9d41-e40edc41d193'  }
Invoke-RestMethod -Uri "https://suzgrid.crpt.ru:443/api/v3/ping?omsId=3b1ed9ae-a5d9-4458-9f02-596781bd1e41" -Headers $headers -Method GET
