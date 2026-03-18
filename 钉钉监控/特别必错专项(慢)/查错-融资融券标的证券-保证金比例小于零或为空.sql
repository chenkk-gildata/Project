--查错-融资融券标的证券-保证金比例小于零或为空
SELECT FORMAT(XXFBRQ, 'yyyy-MM-dd') 信息发布日期,
       XXLY 信息来源,
       ZQDM 证券代码,
       BDLB 变动类别,
       CONCAT(FLOOR(BZJBL * 100), '%') 保证金比例
  FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQ
 WHERE XXFBRQ = FORMAT(GETDATE(), 'yyyy-MM-dd')
   AND (BZJBL < 0 OR BZJBL IS NULL)
