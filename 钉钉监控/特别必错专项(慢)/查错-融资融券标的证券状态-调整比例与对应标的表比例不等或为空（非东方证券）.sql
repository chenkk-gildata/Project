SELECT 信息来源标的,证券代码标的,CONVERT(DATE,信息发布日期标的) 信息发布日期标的,变更日期状态,变更后保证金比例状态,变动类型状态,信息来源标的,证券代码标的,信息发布日期标的,生效日期标的,比例标的
FROM
(SELECT XXLY 信息来源状态,INBBM,XXFBRQ 信息发布日期状态,FORMAT(RXRQ,'yyyy-MM-dd') 变更日期状态,BGHBZJBL 变更后保证金比例状态,JZRQ 截止日期状态,MS 变动类型状态,BDLX
FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQZT A
JOIN (SELECT DM,MS FROM [10.101.0.212].JYPRIME.dbo.usrXTCLB WHERE LB=1577)C
ON A.BDLX=C.DM AND A.BDLX IN (7,8)
WHERE XXLY<>'东方证券')AA
LEFT JOIN
(SELECT XXLY 信息来源标的,ZQDM 证券代码标的,XXFBRQ 信息发布日期标的,SXRQ 生效日期标的,BZJBL 比例标的,INBBM,
CASE WHEN BDLB=10 THEN '7' WHEN BDLB=20 THEN '8' END 标的类别
FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ
WHERE XXLY<>'东方证券')BB
ON AA.信息来源状态=BB.信息来源标的 AND AA.INBBM=BB.INBBM AND AA.BDLX=BB.标的类别 AND AA.信息发布日期状态=BB.信息发布日期标的
WHERE (AA.变更后保证金比例状态<>BB.比例标的 OR (AA.变更后保证金比例状态 IS NULL AND BB.比例标的 IS NOT NULL)) AND  信息发布日期状态=CONVERT(VARCHAR(10),GETDATE(),120)
ORDER BY AA.信息发布日期状态 DESC