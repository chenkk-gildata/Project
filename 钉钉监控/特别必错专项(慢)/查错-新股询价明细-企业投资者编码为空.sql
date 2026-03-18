SELECT ID,InvestorName,InvestorType,InvestorCode
FROM [10.101.0.212].JYDB.dbo.LC_AshareIPOBid
WHERE InvestorCode is NULL AND LEN(InvestorName)>6
  AND InvestorName NOT LIKE '%海拉提·阿不力米提%'
  AND InvestorName LIKE '%公司'
  AND InsertTime BETWEEN GETDATE()-3 AND GETDATE()+1
