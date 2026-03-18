--查错-上交所融资融券标的证券状态变更需处理
---usrJYR表提取上一交易日日期
if object_id('tempdb..#usrJYR') is not null
drop table #usrJYR
  select ZXJYR
  into #usrJYR
  from
	(select
	TOP 1 convert(DATE,RQ) ZXJYR
	from [10.101.0.212].JYPRIME.dbo.usrJYR WITH(NOLOCK)
	where ZQSC=83 AND SFJYR=1 AND RQ<convert(DATE,GETDATE())
	order by RQ desc ) M
--#usrRZRQBDZQ存储最新交易日数据与上一交易日数据
--信息来源，变得类别以及证券代码为关联条件，一方缺失数据
if object_id('tempdb..#usrRZRQBDZQ') is not null
drop table #usrRZRQBDZQ
select ZQDM,XXLY,JGDM,XXFBRQ,ZQSC,INBBM,SXRQ,BDLB,KHXYJB,BZJBL,
ZQDM_NEW,XXLY_NEW,JGDM_NEW,XXFBRQ_NEW,ZQSC_NEW,INBBM_NEW,
SXRQ_NEW,BDLB_NEW,KHXYJB_NEW,BZJBL_NEW
into #usrRZRQBDZQ
from (
select A.ZQDM,A.XXLY,A.JGDM,A.XXFBRQ,A.ZQSC,A.INBBM,A.SXRQ,A.BDLB,A.KHXYJB,A.BZJBL,
B.ZQDM ZQDM_NEW,B.XXLY XXLY_NEW,B.JGDM JGDM_NEW,B.XXFBRQ XXFBRQ_NEW,B.ZQSC ZQSC_NEW,
B.INBBM INBBM_NEW,B.SXRQ SXRQ_NEW,B.BDLB BDLB_NEW,B.KHXYJB KHXYJB_NEW,B.BZJBL BZJBL_NEW
from
(select ZQDM,XXLY,JGDM,XXFBRQ,ZQSC,INBBM,SXRQ,BDLB,KHXYJB,BZJBL
from [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ A WITH(NOLOCK)
join #usrJYR B
on A.XXFBRQ = B.ZXJYR) A
full join
(select ZQDM,XXLY,JGDM,XXFBRQ,ZQSC,INBBM,SXRQ,BDLB,KHXYJB,BZJBL
from [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ A WITH(NOLOCK)
where
A.XXFBRQ = convert(DATE,GETDATE() )
) B
ON A.XXLY = B.XXLY AND A.BDLB = B.BDLB and A.ZQDM = B.ZQDM
where A.ZQDM is null or B.ZQDM is null) C
--查询主体，缺失数据最后筛选
select
ZQDM_NEW ZQDM,XXLY_NEW XXLY,JGDM_NEW JGDM,XXFBRQ_NEW XXFBRQ,ZQSC_NEW ZQSC,INBBM_NEW INBBM,
SXRQ_NEW SXRQ,BDLB_NEW BDLB,KHXYJB_NEW KHXYJB,BZJBL_NEW BZJBL,
case  when   BDLB_NEW = 10 then  '调入融资标的' when   BDLB_NEW = 20 then  '调入融券标的' end  变动类型
from  #usrRZRQBDZQ with (NOLOCK)
where  ZQDM is  null
and JGDM = 41644
and  (KHXYJB is  null
or  (KHXYJB<>999 and  KHXYJB<>970 and  KHXYJB<>840 and  KHXYJB<>760)
)
union  all
select ZQDM,XXLY,JGDM,XXFBRQ,ZQSC,INBBM,SXRQ,BDLB,KHXYJB,BZJBL,
case  when  BDLB = 10 then  '调出融资标的' when  BDLB = 20 then  '调出融券标的' end  变动类型
from  #usrRZRQBDZQ with (NOLOCK)
where  ZQDM_NEW is  null
and JGDM = 41644
and
(KHXYJB_NEW is  null
or  (KHXYJB_NEW<>999 and  KHXYJB_NEW<>970 and  KHXYJB_NEW<>840 and  KHXYJB_NEW<>760)
)