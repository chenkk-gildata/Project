
--标的产品表
SELECT ProjectName 熔断表单中文名称,InsertCnt 日增量,UpdateCnt 数据刷新,DeleteCnt 数据删除,TotalCnt 总累计熔断值,Flag
FROM [10.101.0.212].JYDB.dbo.TableCircuitBreaker WHERE TableName='MT_DailyMargin'

SELECT InfoSource,COUNT(*) FROM [10.101.0.212].JYDB.dbo.MT_DailyMargin WHERE UpdateTime>=CONVERT(DATE,GETDATE()) GROUP BY InfoSource

--可充抵产品表
SELECT ProjectName 熔断表单中文名称,InsertCnt 日增量,UpdateCnt 数据刷新,DeleteCnt 数据删除,TotalCnt 总累计熔断值,Flag
FROM [10.101.0.212].JYDB.dbo.TableCircuitBreaker WHERE TableName='MT_DailyConvensionRate'

SELECT COUNT(*) FROM [10.101.0.212].JYDB.dbo.MT_DailyConvensionRate WHERE UpdateTime>=CONVERT(DATE,GETDATE())




