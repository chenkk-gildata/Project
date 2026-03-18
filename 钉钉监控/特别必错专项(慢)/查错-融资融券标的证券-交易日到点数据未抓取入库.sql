
-- 查错-融资融券标的证券-交易日到点数据未抓取入库
DECLARE @CurrentDate VARCHAR(10) = FORMAT(GETDATE(),'yyyy-MM-dd');
DECLARE @CurrentTime VARCHAR(5) = FORMAT(GETDATE(),'HH:mm');
DECLARE @IsTradingDay BIT =
    (SELECT TOP 1 1 FROM [10.101.0.212].JYPRIME.dbo.usrJYR WHERE ZQSC=83 AND SFJYR=1 AND RQ=@CurrentDate);

SELECT '上交所数据08:05未入库' 异常描述,@CurrentDate 信息发布日期,'上交所' 信息来源,入库数量
FROM(
    SELECT 入库数量=COUNT(ID)
    FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ
    WHERE XXFBRQ=@CurrentDate AND XXLY='上交所' AND @IsTradingDay=1
    )A
WHERE @CurrentTime>='08:05' AND 入库数量=0
UNION ALL
SELECT '北交所数据08:10未入库' 异常描述,@CurrentDate 信息发布日期,'北交所' 信息来源,入库数量
FROM(
    SELECT 入库数量=COUNT(ID)
    FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ
    WHERE XXFBRQ=@CurrentDate AND XXLY='北交所' AND @IsTradingDay=1
    )A
WHERE @CurrentTime>='08:10' AND 入库数量=0
UNION ALL
SELECT '深交所数据09:05未入库' 异常描述,@CurrentDate 信息发布日期,'深交所' 信息来源,入库数量
FROM(
    SELECT 入库数量=COUNT(ID)
    FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ
    WHERE XXFBRQ=@CurrentDate AND XXLY='深交所' AND @IsTradingDay=1
    )A
WHERE @CurrentTime>='09:05' AND 入库数量=0
UNION ALL
SELECT '国信证券数据13:00未入库' 异常描述,@CurrentDate 信息发布日期,'国信证券' 信息来源,入库数量
FROM(
    SELECT 入库数量=COUNT(ID)
    FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ
    WHERE XXFBRQ=@CurrentDate AND XXLY='国信证券' AND @IsTradingDay=1
    )A
WHERE @CurrentTime>='13:00' AND 入库数量=0
UNION ALL
SELECT '申银万国数据10:30未入库' 异常描述,@CurrentDate 信息发布日期,'申银万国' 信息来源,入库数量
FROM(
    SELECT 入库数量=COUNT(ID)
    FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ
    WHERE XXFBRQ=@CurrentDate AND XXLY='申银万国' AND @IsTradingDay=1
    )A
WHERE @CurrentTime>='10:30' AND 入库数量=0
UNION ALL
SELECT '华泰证券数据13:30未入库' 异常描述,@CurrentDate 信息发布日期,'华泰证券' 信息来源,入库数量
FROM(
    SELECT 入库数量=COUNT(ID)
    FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ
    WHERE XXFBRQ=@CurrentDate AND XXLY='华泰证券' AND @IsTradingDay=1
    )A
WHERE @CurrentTime>='13:30' AND 入库数量=0
UNION ALL
SELECT '中信证券数据13:10未入库' 异常描述,@CurrentDate 信息发布日期,'中信证券' 信息来源,入库数量
FROM(
    SELECT 入库数量=COUNT(ID)
    FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ
    WHERE XXFBRQ=@CurrentDate AND XXLY='中信证券' AND @IsTradingDay=1
    )A
WHERE @CurrentTime>='13:10' AND 入库数量=0
UNION ALL
SELECT '银河证券数据13:30未入库' 异常描述,@CurrentDate 信息发布日期,'银河证券' 信息来源,入库数量
FROM(
    SELECT 入库数量=COUNT(ID)
    FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ
    WHERE XXFBRQ=@CurrentDate AND XXLY='银河证券' AND @IsTradingDay=1
    )A
WHERE @CurrentTime>='13:30' AND 入库数量=0
UNION ALL
SELECT '国泰海通数据13:30未入库' 异常描述,@CurrentDate 信息发布日期,'国泰海通' 信息来源,入库数量
FROM(
    SELECT 入库数量=COUNT(ID)
    FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ
    WHERE XXFBRQ=@CurrentDate AND XXLY='国泰海通' AND @IsTradingDay=1
    )A
WHERE @CurrentTime>='13:30' AND 入库数量=0
UNION ALL
SELECT '东方证券数据12:30未入库' 异常描述,@CurrentDate 信息发布日期,'东方证券' 信息来源,入库数量
FROM(
    SELECT 入库数量=COUNT(ID)
    FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ
    WHERE XXFBRQ=@CurrentDate AND XXLY='东方证券' AND @IsTradingDay=1
    )A
WHERE @CurrentTime>='12:30' AND 入库数量=0