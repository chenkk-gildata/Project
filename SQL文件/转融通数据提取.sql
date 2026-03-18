--7:30网页更新前一交易日数据，八点前入库

--转融券交易汇总   99库
select  ZQDM 证券代码,ZQJC 证券名称,QCYL 期初余量,RCSL 融出数量,QMYL 期末余量,QMYE 期末余额 from [10.101.0.212].JYPRIME.dbo.usrZRTZRQJYHZ
WHERE JYRQ='2025-06-20' and  JQYTLB=1
ORDER BY 1

--转融券交易明细   99库
select A.XH 序号,A.ZQDM 证券代码,A.ZQJC 证券简称,A.PZ 品种,A.RCFL 融出费率,A.RCSL 融出数量 from [10.101.0.212].JYPRIME.dbo.usrZRTZRQJYMX A
WHERE A.JYRQ='2025-06-20'and  JQYTLB=1
order by A.ZQDM

--做市借券交易明细   99库
select A.XH 序号,A.ZQDM 证券代码,A.ZQJC 证券简称,A.PZ 品种,A.RCFL 融出费率,A.RCSL 融出数量 from [10.101.0.212].JYPRIME.dbo.usrZRTZRQJYMX A
WHERE A.JYRQ='2025-06-20'and  JQYTLB=2
order by A.ZQDM

--做市借券交易汇总   99库
select  ZQDM 证券代码,ZQJC 证券名称,QCYL 期初余量,RCSL 融出数量,QMYL 期末余量,QMYE 期末余额 from [10.101.0.212].JYPRIME.dbo.usrZRTZRQJYHZ
WHERE JYRQ='2025-07-11' and  JQYTLB=2
ORDER BY 1

--转融通标的证券   99库
select CAST(A.SXRQ AS DATE) 生效日期,A.SCMC 市场名称,A.ZQDM 证券代码,A.ZQJC 证券名称,A.ZQLBMS 证券类别描述,B.MS 状态 
from [10.101.0.212].JYPRIME.dbo.usrZRTBDZQ A JOIN [10.101.0.212].JYPRIME.dbo.usrXTCLB B ON A.ZT=B.DM
where A.ZT=1 AND B.LB=1790
order by A.ZQDM

--九点四十网页更新当前交易日数据，十点前入库

--参与人名单
SELECT B.MS 证券分类,A.ZQGS,A.QYBH 企业编号,cast(A.SXRQ as date) 生效时间  FROM  [10.101.0.212].JYPRIME.dbo.usrZRTCYRMD A ,[10.101.0.212].JYPRIME.dbo.usrXTCLB B
where A.ZT=1 AND B.LB=1848
 and A.ZGFL=B.DM
ORDER BY A.ZGFL

--可充抵保证金
select ID,SCMC 市场名称,ZQDM 证券代码,ZQJC 证券名称,ZSL 折算率 from [10.101.0.212].JYPRIME.dbo.usrZRTBDZQMX
where SXRQ='2025-06-20'
order by ZQSC DESC,ZQDM ASC

/*日增数量*/    全量数据(51库)
select 'usrZRTBDZQ' 表英文名,'转融通-标的证券' 表中文名,'' 备注,count(ID) from usrZRTBDZQ
where ZT=1
group by ZT
UNION ALL
select 'usrZRTQXFL' 表名,'转融通-期限费率' 表中文名,'' 备注,count(ID) from usrZRTQXFL
where cast(SXRQ as date)=(select max(cast(RQ as date)) from usrJYR where ZQSC=83 and SFJYR=1 and cast(RQ as date)<=cast(getdate() as date))
group by SXRQ
UNION ALL
select 'usrZRTZRZJYHZID' 表名,'转融通-转融资交易汇总' 表中文名,'' 备注,count(ID) from usrZRTZRZJYHZ
where cast(JYRQ as date)=(select max(cast(RQ as date)) from usrJYR where ZQSC=83 and SFJYR=1 and cast(RQ as date)<cast(getdate() as date))
group by JYRQ
UNION ALL
select 'usrZRTZRQJYHZ' 表名,'转融通-转融券交易汇总' 表中文名,'' 备注,count(ID) from usrZRTZRQJYHZ
where cast(JYRQ as date)=(select max(cast(RQ as date)) from usrJYR where ZQSC=83 and SFJYR=1 and cast(RQ as date)<cast(getdate() as date))
group by JYRQ
UNION ALL
select 'usrZRTZRQJYMX' 表名,'转融通-转融券交易明细' 表中文名,'' 备注,count(ID) from usrZRTZRQJYMX
where cast(JYRQ as date)=(select max(cast(RQ as date)) from usrJYR where ZQSC=83 and SFJYR=1 and cast(RQ as date)<cast(getdate() as date))
group by JYRQ
UNION ALL
select 'usrZRTBDZQMX' 表名,'转融通-可充抵保证金证券明细' 表中文名,'' 备注,count(ID) from usrZRTBDZQMX
where cast(SXRQ as date)=(select max(cast(RQ as date)) from usrJYR where ZQSC=83 and SFJYR=1 and cast(RQ as date)<=cast(getdate() as date))
group by SXRQ
UNION ALL
select 'usrZRTCYRMD' 表名,'转融通-参与人名单' 表中文名,ZGFL 备注,count(ID) from usrZRTCYRMD
where ZT=1
group by ZGFL


未检验数据
--转融通-标的证券未公开
select a.SXRQ 生效日期,a.SCMC 市场名称,a.ZQSC 证券市场 ,a.INBBM 内部编码,a.ZQDM 证券代码,a.GKBZ 公开标志
from [10.101.0.212].JYPRIME.dbo.usrZRTBDZQ a 
where  a.GKBZ <>3
order by 1 desc

--转融通-转融券交易汇总未公开
select a.JYRQ 交易日期,a.INBBM 内部编码,a.XH 序号,a.ZQDM 证券代码,a.ZQJC 证券名称,a.GKBZ 公开标志
from [10.101.0.212].JYPRIME.dbo.usrZRTZRQJYHZ a 
where  a.GKBZ <>3
order by 1 desc

--转融券交易明细未公开
select a.JYRQ 交易日期,a.INBBM 内部编码,a.ZQDM 证券代码,a.ZQJC 证券名称,a.PZ 品种,a.GKBZ 公开标志
from [10.101.0.212].JYPRIME.dbo.usrZRTZRQJYMX a 
where  a.GKBZ <>3
order by 1 desc

--转融资交易汇总未公开
select a.JYRQ 交易日期,a.SCMC 市场名称,a.SCDM 市场代码,a.GKBZ 公开标志
from [10.101.0.212].JYPRIME.dbo.usrZRTZRZJYHZ a
where  a.GKBZ <>3
order by 1 desc

--转融通-期限费率未公开
select a.SXRQ 生效日期,a.QXFL 期限费率,a.QXMS 期限描述,a.QX 期限,a.GKBZ 公开标志
from [10.101.0.212].JYPRIME.dbo.usrZRTQXFL a
where  a.GKBZ <>3
order by 1 desc

--转融通-参与人名单未公开
select a.ZGFL 资格分类,a.ZQGS 证券公司,a.QYBH 企业编码,a.SXRQ 生效日期,a.TCRQ 剔除日期,a.GKBZ 公开标志
from [10.101.0.212].JYPRIME.dbo.usrZRTCYRMD a 
where  a.GKBZ <>3 
order by 1 desc

--转融通-可充抵保证金证券明细未公开
select a.SXRQ 生效日期,a.SCMC 市场名称,a.ZQSC 证券市场 ,a.INBBM 内部编码,a.ZQDM 证券代码,a.GKBZ 公开标志
from [10.101.0.212].JYPRIME.dbo.usrZRTBDZQMX a 
where  a.GKBZ <>3
order by 1 desc
