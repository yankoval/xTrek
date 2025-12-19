$headers = @{     'Accept' = 'application/json'
'clientToken' = '2eb0d4dc-56ef-4731-9709-20149014326e'  }
	Invoke-RestMethod -Uri "https://suzgrid.crpt.ru:443/api/v3/ping?omsId=3b1ed9ae-a5d9-4458-9f02-596781bd1e41" -Headers $headers -Method GET
