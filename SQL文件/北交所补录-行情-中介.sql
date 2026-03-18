--中介机构
SELECT b.GPDM,cast(a.GGRQ as date) ,a.XXLY,a.LXBM,a.ZJGS FROM dbo.usrSBZJJGGLB a,dbo.usrZQZB b
WHERE a.IGSDM=b.IGSDM
AND b.ZQSC=81
AND b.ZQLB IN (1,2)
and a.LXBM in (3,4)
and b.GPDM in ('873806')
order by 2


--北交所补录 行情
SELECT B.GPDM 股票代码,A.INBBM 内部编码,CAST(RQ AS DATE) 日期,QSP 前收盘,KPJ 开盘价,ZGJ 最高价,ZDJ 最低价,SPJ 收盘价,CJL 成交量,CJJE 成交金额,JYBS 交易笔数,ZQSC  证券市场 FROM usrSBMRHQB A  INNER JOIN usrZQZB B 
ON A.INBBM=B.INBBM AND ZQSC='81' AND ZQLB<3
AND SSZT='1'
AND B.GPDM IN ('836961')
ORDER BY 3 DESC