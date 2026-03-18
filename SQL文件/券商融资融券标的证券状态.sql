/*融资融券调入调出*/
DECLARE
    --T日
    @IntraDay DATE = '2022-01-05',
    --T-1日
    @T_1Day DATE;
SELECT @T_1Day = CONVERT(DATE,MAX(RQ))
FROM [10.101.0.212].JYPRIME.dbo.usrJYR
WHERE ZQSC=83 AND SFJYR=1 AND RQ<@IntraDay;

WITH BBB AS
(SELECT A.证券代码 证券代码上,A.证券简称 证券简称上,A.信息来源 信息来源上,A.机构代码 机构代码上,A.信息发布日期 信息发布日期上,
A.证券市场 证券市场上,A.内部编码 内部编码上,A.生效日期 生效日期上,A.变动类别 变动类别上,A.客户信用级别 客户信用级别上,A.保证金比例 保证金比例上,
        B.证券代码 证券代码今,B.证券简称 证券简称今,B.信息来源 信息来源今,B.机构代码 机构代码今,B.信息发布日期 信息发布日期今,
		B.证券市场 证券市场今,B.内部编码 内部编码今,B.生效日期 生效日期今,B.变动类别 变动类别今,B.客户信用级别 客户信用级别今,B.保证金比例 保证金比例今
FROM
(SELECT ZQDM 证券代码,ZQJC 证券简称,XXLY 信息来源,JGDM 机构代码,FORMAT(XXFBRQ,'yyyy-MM-dd') 信息发布日期,
ZQSC 证券市场,INBBM 内部编码,FORMAT(SXRQ,'yyyy-MM-dd') 生效日期,BDLB 变动类别,KHXYJB 客户信用级别,BZJBL 保证金比例
FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ
WHERE XXFBRQ=@T_1Day
AND JGDM IN (41740,42064))A
FULL JOIN
(SELECT ZQDM 证券代码,ZQJC 证券简称,XXLY 信息来源,JGDM 机构代码,FORMAT(XXFBRQ,'yyyy-MM-dd') 信息发布日期,
ZQSC 证券市场,INBBM 内部编码,FORMAT(SXRQ,'yyyy-MM-dd') 生效日期,BDLB 变动类别,KHXYJB 客户信用级别,BZJBL 保证金比例
FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ
WHERE XXFBRQ=@IntraDay
AND JGDM IN (41740,42064))B
ON A.机构代码=B.机构代码 AND A.变动类别=B.变动类别 AND A.证券代码=B.证券代码
WHERE A.证券代码 IS NULL OR B.证券代码 IS NULL),
CCC AS
(SELECT 证券代码今,证券简称今,信息来源今,机构代码今,信息发布日期今,证券市场今,内部编码今,生效日期今,
CASE WHEN 变动类别今=10 THEN '调入融资标的' WHEN 变动类别今=20 THEN '调入融券标的' END 变动类型
FROM BBB
WHERE 证券代码上 IS NULL AND (客户信用级别今 IS NULL OR (客户信用级别今<>999 AND 客户信用级别今<>970 AND 客户信用级别今<>840 AND 客户信用级别今<>760))
UNION ALL
SELECT 证券代码上,证券简称上,信息来源上,机构代码上,信息发布日期=@IntraDay,证券市场上,内部编码上,生效日期=@IntraDay,
CASE WHEN 变动类别上=10 THEN '调出融资标的' WHEN 变动类别上=20 THEN '调出融券标的' END 变动类型
FROM BBB
WHERE 证券代码今 IS NULL AND (客户信用级别上 IS NULL OR (客户信用级别上<>999 AND 客户信用级别上<>970 AND 客户信用级别上<>840 AND 客户信用级别上<>760)))
SELECT 信息来源今,机构代码今,证券代码今,证券简称今,信息发布日期今,生效日期今,变动类型,内部编码今
FROM CCC  order by 2,3


/*融资融券调整保证金*/
DECLARE
    --T日
    @IntraDay DATE = '2022-01-10',
    --T-1日
    @T_1Day DATE;
SELECT @T_1Day = CONVERT(DATE,MAX(RQ))
FROM [10.101.0.212].JYPRIME.dbo.usrJYR
WHERE ZQSC=83 AND SFJYR=1 AND RQ<@IntraDay;

SELECT XXFBRQ AS 信息发布日期, @IntraDay AS 变更日期,XXLY AS 信息来源,JGDM AS 机构代码,ZQDM AS 证券代码,ZQJC AS 证券简称,
       KHXYJB AS 客户信用等级,BZJBL AS 变更后保证金比例,'调整融资保证金比例' AS 类别,INBBM AS 内部编码
FROM (SELECT A.* FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ A
	WHERE BDLB=10 
	AND JGDM IN (41740,42064)
	AND CONVERT(DATE,SXRQ)=@IntraDay) t1
WHERE EXISTS 
	(SELECT * FROM 
		(SELECT * FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ 
		WHERE BDLB=10 
		AND JGDM IN (41740,42064)
		AND CONVERT(DATE,SXRQ)=@T_1Day) t2
	WHERE t1.BDLB=t2.BDLB 
	AND t1.JGDM=t2.JGDM 
	AND t1.INBBM=t2.INBBM 
	AND ISNULL(t1.KHXYJB,0)=ISNULL(t2.KHXYJB,0) 
	AND t1.BZJBL<>t2.BZJBL) 
UNION 
SELECT XXFBRQ AS 信息发布日期, @IntraDay AS 变更日期,XXLY AS 信息来源,JGDM AS 机构代码,ZQDM AS 证券代码,ZQJC AS 证券简称,
       KHXYJB AS 客户信用等级,BZJBL AS 变更后保证金比例,'调整融券保证金比例' AS 类别,INBBM AS 内部编码 
FROM (SELECT A.* FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ A 
	WHERE BDLB=20 
	AND JGDM IN (41740,42064)
	AND CONVERT(DATE,SXRQ)=@IntraDay) t1
WHERE EXISTS 
	(SELECT * FROM 
		(SELECT * FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ 
		WHERE BDLB=20 
		AND JGDM IN (41740,42064)
		AND CONVERT(DATE,SXRQ)=@T_1Day) t2
	WHERE t1.BDLB=t2.BDLB 
	AND t1.JGDM=t2.JGDM 
	AND t1.INBBM=t2.INBBM 
	AND ISNULL(t1.KHXYJB,0)=ISNULL(t2.KHXYJB,0)
	AND t1.BZJBL<>t2.BZJBL)





