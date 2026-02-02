$headers = @{     'Accept' = 'application/json'
'clientToken' = 'ca8d727f-e596-422b-96a3-45a0c90183fa'  }
	Invoke-RestMethod -Uri "https://suzgrid.crpt.ru:443/api/v3/ping?omsId=3b1ed9ae-a5d9-4458-9f02-596781bd1e41" -Headers $headers -Method GET
