
--数据库查询
[10.101.0.212].JYPRIME.dbo.		--99库关联JYP
[10.101.0.201].JYPRIME.dbo.	--164JYFIN关联JYP查询
[10.101.1.144].FSCSJ.dbo.	--内部使用
[10.106.22.60].JYFIN.dbo.	--99库关联60 JYFIN

--数据库账号
10.106.22.51
10.101.1.144
10.102.25.11,8080   WebResourceNew_Read	    New_45ted
10.101.0.164	DataCenter_Read		        ter#CenD89
SHJYPSQLDEV01	data_sjzx			        123qwe
10.106.1.101	data_sjcb			        tAEQBij3  --任务流程数据库


IF
	object_id( 'tempdb..#TPFPLS' ) IS NOT NULL
DROP TABLE #TPFPLS;



CREATE TABLE #Table_Name ( TABLE_NAME VARCHAR ( 100 ) ) 
INSERT INTO #Table_Name ( TABLE_NAME)
VALUES
('usrDTXXB')




--产品表关联证券主表
SELECT B.SecuCode,CONVERT(DATE,A.InfoPublDate) InfoPublDate,A.* FROM  A
JOIN SecuMain B ON A.CompanyCode=B.CompanyCode AND B.SecuMarket IN (18,83,90) AND B.SecuCategory IN (1,2,41)



-- 开启查询性能统计
SET STATISTICS TIME ON;





--DECIMAL转换varchar去0
SELECT A.INBBM,CONVERT(DATE,A.XXFBRQ) XXFBRQ,A.YJDXYSMC,REPLACE(REPLACE(A.YJYJJGSM,',',''),'，','') YJYJJGSM,
	CASE 
		WHEN CONVERT(VARCHAR,A.QSZJE) LIKE '%.00000000' THEN LEFT(A.QSZJE,LEN(CONVERT(VARCHAR,A.QSZJE))-PATINDEX('%[^0]%', REVERSE(A.QSZJE)))
		ELSE LEFT(A.QSZJE,LEN(CONVERT(VARCHAR,A.QSZJE))-PATINDEX('%[^0]%', REVERSE(A.QSZJE))+1)
	END AS QSZJE,
	CASE 
		WHEN CONVERT(VARCHAR,A.JZZJE) LIKE '%.00000000' THEN LEFT(A.JZZJE,LEN(CONVERT(VARCHAR,A.JZZJE))-PATINDEX('%[^0]%', REVERSE(A.JZZJE)))
		ELSE LEFT(A.JZZJE,LEN(CONVERT(VARCHAR,A.JZZJE))-PATINDEX('%[^0]%', REVERSE(A.JZZJE))+1)
	END AS JZZJE
FROM usrCWJCZBZYCYJYGCJB A





--交易日比对
SELECT A.RQ,A.ZQSC,A.SFJYR,A.ZM,A.YM,A.JM,A.NM FROM [10.101.0.212].JYPRIME.dbo.usrJYR A
JOIN [10.101.0.212].JYPRIME.dbo.usrJYR B ON A.RQ=B.RQ AND B.ZQSC=83 AND( A.SFJYR!=B.SFJYR OR A.ZM!=B.ZM OR A.YM!=B.YM OR A.JM!=B.JM OR A.NM!=B.NM)
JOIN [10.101.0.212].JYPRIME.dbo.usrJYR C ON A.RQ=C.RQ AND C.ZQSC=90 AND( A.SFJYR!=C.SFJYR OR A.ZM!=C.ZM OR A.YM!=C.YM OR A.JM!=C.JM OR A.NM!=C.NM)
WHERE A.RQ BETWEEN '2024-01-01' AND '2024-12-31' AND A.ZQSC IN (18)



--熔断查询
SELECT ProjectName 熔断表单中文名称,InsertCnt 日增量,UpdateCnt 数据刷新,DeleteCnt 数据删除,TotalCnt 总累计熔断值,Flag
FROM [10.101.0.212].JYDB.dbo.TableCircuitBreaker WHERE TableName='LC_SMAttendInfo'
/**
字段解释如下：
项目名ProjectName-熔断表单中文名称
表名TableName-熔断表单英文名称
日增量(条)InsertCnt-单批次熔断值
数据刷新(条)UpdateCnt-单批次熔断值
数据删除(条)DeleteCnt-单批次和累计熔断值
总量(刷新+新增)TotalCnt-总累计熔断值
flag：1是熔断开启的状态 0是熔断关闭的状态
remark：可以填写相应的备注  @
**/


/*
底层表及产品表调度频率
10.106.1.101    data_gh    123qwe
*/
SELECT 	ZYMC '作业名称',	SFQY '是否有效',	ZYBZ '步骤序号',	BZMC '步骤名称',	ZXJHMX '执行计划明细'
FROM JYPRIME..usrSJKBDD WHERE BZMC LIKE '%CFI_MajorContract%' --模糊查询表名





--取表名
SELECT A.TABLENAME 
FROM [10.101.0.212].JYPRIME.dbo.cmdTABLES A
	JOIN [10.101.0.212].JYPRIME.dbo.dscmdTABLES B
		ON A.TABLENAME=B.AB
	JOIN [10.101.0.212].JYPRIME.dbo.usrSJZXKBSJY C
		ON B.AA=C.TABLENAME
WHERE C.WHXZ=149 AND C.SSYW=40

--取人名
SELECT B.OPERATORNAME 修改人员
FROM ********** A
	JOIN cmdOPERATORS B 
		ON A.XGRY=B.OPERATORID