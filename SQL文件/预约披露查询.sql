
--T日预披露代码
SELECT B.GPDM,B.ZQJC
FROM [10.101.0.212].JYPRIME.dbo.usrYYPLRXX A,
	 [10.101.0.212].JYPRIME.dbo.usrZQZB B 
WHERE 
	A.INBBM=B.INBBM
	AND B.SSZT=1
	--AND A.JZRQ='2025-03-31'
	AND A.JZRQ='2025-09-30'
	AND A.SFYX=1
	AND (
		B.ZQLB IN (1,41)
		OR B.GPDM IN (--纯B股代码
			'200468','200512','200706','200771','200992','900929','900939','900948'))
	AND (
		(A.TSRQQSR=CONVERT(DATE,DATEADD(DD,1,GETDATE())) AND B.ZQSC IN (83,90)
			--剔除已提前披露的上、深代码
			AND (A.SJPLRQ = CONVERT(DATE,DATEADD(DD,1,GETDATE())) OR A.SJPLRQ IS NULL))
		OR (A.TSRQQSR=CONVERT(DATE,GETDATE()) AND B.ZQSC IN (18)
			--剔除已提前披露的北代码
			AND (A.SJPLRQ = CONVERT(DATE,GETDATE()) OR A.SJPLRQ IS NULL))
		--提前披露的代码
		OR ( A.SJPLRQ=CONVERT(DATE,DATEADD(DD,1,GETDATE())) AND B.ZQSC IN (83,90))
		OR (A.SJPLRQ=CONVERT(DATE,GETDATE()) AND B.ZQSC IN (18)))
ORDER BY 1


--定报披露数量分布时间表
SELECT 预计披露日期 ,截止日期, COUNT(*) 预计披露家数
FROM (
    SELECT CONVERT(DATE,DATEADD(DD,-1,A.TSRQQSR)) 预计披露日期,
            CONVERT(DATE,A.JZRQ) 截止日期
    FROM [10.101.0.212].JYPRIME.dbo.usrYYPLRXX A
        JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B ON A.INBBM = B.INBBM AND B.ZQSC != '18'
    WHERE A.JZRQ>='2025-12-31' AND A.SFYX=1
    UNION ALL
    SELECT CONVERT(DATE,A.TSRQQSR) 预计披露日期,
           CONVERT(DATE,A.JZRQ) 截止日期
    FROM [10.101.0.212].JYPRIME.dbo.usrYYPLRXX A
        JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B ON A.INBBM = B.INBBM AND B.ZQSC = '18'
    WHERE A.JZRQ>='2025-12-31' AND A.SFYX=1
) M
--WHERE M.JZRQ='2025-03-31'
WHERE 预计披露日期>=CONVERT(DATE,GETDATE())
GROUP BY 预计披露日期,截止日期
ORDER BY 预计披露日期,截止日期


--截止披露日未更新实际披露日
SELECT B.GPDM,B.ZQJC,CONVERT(DATE,A.JZRQ) JZRQ,CONVERT(DATE,A.TSRQQSR) TSRQQSR
FROM [10.101.0.212].JYPRIME.dbo.usrYYPLRXX A , [10.101.0.212].JYPRIME.dbo.usrZQZB B WHERE 
A.IGSDM=B.IGSDM
AND B.ZQLB in (1,2)
AND A.JZRQ>='2025-06-30'
AND A.SFYX=1
AND (
	(B.GPDM >= '300000' AND B.GPDM <'900000')
	OR B.GPDM < '200000'
	OR B.GPDM IN (
	--纯B股代码
	'200468','200512','200706','200771','200992','900929','900939','900948'
	)
)
AND (
		(
		A.TSRQQSR<=
			--'2024-08-30'
			CONVERT(DATE,DATEADD(DD,1,GETDATE()))
		AND B.ZQSC IN (83,90)
		)
		OR
		(
		A.TSRQQSR<=
			--'2024-08-29'
			CONVERT(DATE,GETDATE())
		AND B.ZQSC IN (18)
		)
	)
AND A.SJPLRQ IS NULL
ORDER BY 3 DESC


------实际披露日
WITH L1 AS ( 
SELECT 
    B.GPDM 公司代码,B.INBBM ,
    B.ZQJC 证券简称,
    CONVERT(DATE,A.JZRQ ) 报告截止期,
    CONVERT(DATE,A.SJPLRQ  ) 实际披露日,
    A.ID
FROM [10.101.0.212].JYPRIME.dbo.usrYYPLRXX A
JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B ON A.INBBM= B.INBBM AND ZQSC IN (18,83,90) AND ZQLB IN (1,2,41)
WHERE A.SFYX=1 AND A.SJPLRQ IS NULL
),
L2 AS (
SELECT A.GPDM 公司代码,A.ZQJC 证券简称,CONVERT(date,B.JZRQ)  报告截止期,B.XXLYBM 来源编码,CONVERT(date,B.XXFBRQ ) 主要指标信息发布日
FROM [10.101.0.212].JYPRIME.dbo.usrGSCWZYZB B
    JOIN  [10.101.0.212].JYPRIME.dbo.usrZQZB A ON  B.INBBM=A.INBBM  AND ZQSC IN (18,83,90) AND ZQLB IN (1,2,41)
WHERE B.XXLYBM IN(110102,110103,110101,110104) AND B.TZBZ=2
)
SELECT DISTINCT L1.公司代码,L1.证券简称,L1.报告截止期,L2.主要指标信息发布日,L1.实际披露日,L1.ID,L1.INBBM
FROM L1 LEFT JOIN L2 ON L1.公司代码=L2.公司代码 AND L1.报告截止期=L2.报告截止期
WHERE L2.主要指标信息发布日 IS NOT NULL
ORDER BY 3 DESC




--截止高峰期最后一天尚未披露公告（退市）
WITH SSZT AS(
	SELECT INBBM,变更类型,BGRQ,row#
	FROM (
		SELECT INBBM,B.MS 变更类型,CONVERT(DATE,BGRQ) BGRQ,ROW_NUMBER() OVER(PARTITION BY INBBM ORDER BY BGRQ DESC) AS row#
		FROM [10.101.0.212].JYPRIME.dbo.usrSSZTGGZK A
			JOIN [10.101.0.212].JYPRIME.dbo.usrXTCLB B
				ON A.BGLX=B.DM AND B.LB=1184
	) A WHERE row#=1
)
SELECT B.GPDM,B.ZQJC,B.ZWMC,CONVERT(DATE,A.TSRQQSR) TSRQQSR,C.变更类型,BGRQ 变更日期
FROM [10.101.0.212].JYPRIME.dbo.usrYYPLRXX A 
JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B
	ON A.INBBM=B.INBBM
	--AND B.SSZT=1
	AND B.ZQLB IN (1,2,41)
	AND B.ZQSC IN (18,83,90)
LEFT JOIN SSZT C
	ON A.INBBM=C.INBBM
WHERE A.JZRQ >= '2025-06-30'
AND SFYX = 1
AND SJPLRQ IS NULL
ORDER BY 4,1 DESC








--银行、证券类公司定报披露数量分布时间表
SELECT 预计披露日期 , COUNT(*) 预计披露家数 FROM 
(SELECT CONVERT(DATE,DATEADD(DD,-1,A.TSRQQSR)) 预计披露日期,A.ID,A.JZRQ,A.SFYX,B.ZWMC
FROM [10.101.0.212].JYPRIME.dbo.usrYYPLRXX A 
JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B ON A.INBBM = B.INBBM
AND B.ZQSC != '18'
UNION ALL
SELECT CONVERT(DATE,A.TSRQQSR) 预计披露日期,A.ID,A.JZRQ,A.SFYX,B.ZWMC 
FROM [10.101.0.212].JYPRIME.dbo.usrYYPLRXX A 
JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B ON A.INBBM = B.INBBM
WHERE B.ZQSC = '18') M
WHERE M.JZRQ='2025-06-30'
AND M.SFYX=1
AND (M.ZWMC LIKE '%银行%'
OR M.ZWMC LIKE '%券%'
OR M.ZWMC LIKE '%信托%'
OR M.ZWMC LIKE '%航空%'
OR M.ZWMC LIKE '%快递%'
OR M.ZWMC LIKE '%保险%')
GROUP BY 预计披露日期
ORDER BY 预计披露日期




--查询主板所有B股代码
SELECT ZWMC FROM [10.101.0.212].JYPRIME.dbo.usrZQZB WHERE ZQLB=2 AND SSBZ IN (1,6,7,8) AND SSZT = 1


--查询纯B股代码
SELECT * 
FROM [10.101.0.212].JYPRIME.dbo.usrZQZB 
WHERE ZWMC IN (
	SELECT ZWMC FROM [10.101.0.212].JYPRIME.dbo.usrZQZB 
	WHERE ZQLB IN (1,2) AND ZWMC IN (
        '康佳集团股份有限公司','深圳市特力(集团)股份有限公司','方大集团股份有限公司','海南京粮控股股份有限公司','常柴股份有限公司','上海振华重工(集团)股份有限公司','上海神奇制药投资管理股份有限公司','上海三毛企业(集团)股份有限公司','上海物资贸易股份有限公司','上海海欣集团股份有限公司','深圳中华自行车(集团)股份有限公司','飞亚达精密科技股份有限公司','深圳市皇庭国际企业股份有限公司','无锡威孚高科技集团股份有限公司','上海金桥出口加工区开发股份有限公司','杭州汽轮动力集团股份有限公司','上海百联集团股份有限公司','上海锦江国际旅游股份有限公司','中国南玻集团股份有限公司','深圳经济特区房地产(集团)股份有限公司','冰山冷热科技股份有限公司','上海大名城企业股份有限公司','云赛智联股份有限公司','山西省国新能源股份有限公司','上海锦江在线网络服务股份有限公司','湖南天雁机械股份有限公司','山东省中鲁远洋渔业股份有限公司','上海新动力汽车科技股份有限公司','上海汇丽建材股份有限公司','深圳中恒华发股份有限公司','深圳南山热电股份有限公司','佛山电器照明股份有限公司','重庆长安汽车股份有限公司','海南航空控股股份有限公司','贵州中毅达股份有限公司','上海海立(集团)股份有限公司','上海凤凰企业(集团)股份有限公司','海航科技股份有限公司','本钢板材股份有限公司','厦门灿坤实业股份有限公司','丹化化工科技股份有限公司','招商局港口集团股份有限公司','广东省高速公路发展股份有限公司','江铃汽车股份有限公司','京东方科技集团股份有限公司','上海开开实业股份有限公司','大众交通(集团)股份有限公司','上海华谊集团股份有限公司','上海锦江国际酒店股份有限公司','烟台张裕葡萄酿酒股份有限公司','瓦房店轴承股份有限公司','中路股份有限公司','上海宝信软件股份有限公司','深圳市纺织(集团)股份有限公司','山东晨鸣纸业集团股份有限公司','安道麦股份有限公司','鲁泰纺织股份有限公司','内蒙古鄂尔多斯资源股份有限公司','老凤祥股份有限公司','上海耀皮玻璃集团股份有限公司','上海临港控股股份有限公司','深圳市物业发展(集团)股份有限公司','国药集团一致药业股份有限公司','深圳赛格股份有限公司','长虹美菱股份有限公司','安徽古井贡酒股份有限公司','黄山旅游发展股份有限公司','上海外高桥集团股份有限公司','上海机电股份有限公司','深圳市深粮控股股份有限公司','富奥汽车零部件股份有限公司','广东电力发展股份有限公司','锦州港股份有限公司','上海市北高新股份有限公司','上海氯碱化工股份有限公司','上海陆家嘴金融贸易区开发股份有限公司','华电能源股份有限公司','南京普天通信股份有限公司','东方通信股份有限公司','上工申贝(集团)股份有限公司','内蒙古伊泰煤炭股份有限公司'
	    )
	GROUP BY ZWMC HAVING COUNT(*)=1
)
AND ZQLB=2
ORDER BY 4





--主要指标有，实际披露日未更新
SELECT DISTINCT C.GPDM,CONVERT(DATE,A.JZRQ) JZRQ,CONVERT(DATE,B.XXFBRQ) XXFBRQ 
FROM [10.101.0.212].JYPRIME.dbo.usrYYPLRXX A 
LEFT JOIN [10.101.0.212].JYPRIME.dbo.usrGSCWZYZB B ON A.INBBM=B.INBBM AND A.JZRQ=B.JZRQ
JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB C ON A.INBBM=C.INBBM AND C.ZQSC IN (90,83,18) AND C.ZQLB in (1,2,41)
WHERE A.JZRQ='2024-12-31' AND A.SFYX=1 AND A.SJPLRQ IS NULL
AND (((( A.TSRQQSR=CONVERT(DATE,DATEADD(DD,1,GETDATE())) AND C.ZQSC IN (83,90))
OR (A.TSRQQSR=CONVERT(DATE,GETDATE()) AND C.ZQSC in (18)))
AND B.ID IS NOT NULL AND B.TZBZ=2)
OR 
((( A.TSRQQSR>CONVERT(DATE,DATEADD(DD,1,GETDATE())) AND C.ZQSC IN (83,90))
OR (A.TSRQQSR>CONVERT(DATE,GETDATE()) AND C.ZQSC in (18)))
AND (B.XXFBRQ>CONVERT(DATE,DATEADD(DD,1,GETDATE())) AND C.ZQSC IN (83,90))
OR (B.XXFBRQ>CONVERT(DATE,GETDATE()) AND C.ZQSC in (18))
))
ORDER BY 1







