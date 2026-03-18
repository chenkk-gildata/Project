--业绩预告信息来源编码  201109
SELECT *
FROM [10.106.22.60].JYFIN.dbo.usrCWJCZBZYCYJYGCJB
WHERE XXLYBM IS NULL


SELECT DISTINCT B.GPDM,CONVERT(DATE,A.XXFBRQ) XXFBRQ
FROM [10.106.22.60].JYFIN.dbo.usrCWJCZBZYCYJYGCJB A
    JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B ON A.IGSDM = B.IGSDM AND B.ZQLB IN (1,2,41) AND B.ZQSC IN (18,83,90)
WHERE XXLYBM IS NULL
ORDER BY 2 DESC


--业绩预告对应非文本只有1条公告的记录  94398 -已完成
SELECT A.ID, A.IGSDM, CONVERT(DATE,A.XXFBRQ) XXFBRQ, B.ID, B.XXBT
       ,CASE WHEN B.XXBT LIKE '%业绩预%' AND B.XXBT NOT LIKE '%修%' AND B.XXBT NOT LIKE '%更正%' AND B.XXBT NOT LIKE '%补充%' THEN 'FCC000001E16' END AS XXLYBM
FROM [10.106.22.60].JYFIN.dbo.usrCWJCZBZYCYJYGCJB A
JOIN (
    SELECT IGSDM, XXFBRQ, ID, XXBT
    FROM (
        SELECT IGSDM, XXFBRQ, ID, XXBT,
               COUNT(*) OVER (PARTITION BY IGSDM, XXFBRQ) AS cnt
        FROM [10.101.0.212].JYPRIME.dbo.usrGSGGYWFWB
    ) t
    WHERE cnt = 1
) B ON A.IGSDM = B.IGSDM AND A.XXFBRQ = B.XXFBRQ
WHERE A.XXLYBM IS NULL


--符合标题名称的公告数量=1 78780 -已完成
SELECT A.ID,A.IGSDM, CONVERT(DATE,A.XXFBRQ) XXFBRQ, CONVERT(DATE,A.JZRQ) JZRQ, B.ID, B.XXBT
FROM (SELECT ID,IGSDM,XXFBRQ,JZRQ
      FROM [10.106.22.60].JYFIN.dbo.usrCWJCZBZYCYJYGCJB
      WHERE XXLYBM IS NULL
      ) A
JOIN (
        SELECT IGSDM, XXFBRQ, ID, XXBT
        FROM (
        SELECT A.IGSDM, A.XXFBRQ, A.ID, A.XXBT,
               COUNT(*) OVER (PARTITION BY A.IGSDM, A.XXFBRQ) AS cnt
        FROM [10.101.0.212].JYPRIME.dbo.usrGSGGYWFWB A
            JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B ON A.INBBM=B.INBBM AND B.ZQSC IN (18,81,83,90) AND B.ZQLB IN (1,2,41)
        WHERE XXBT NOT LIKE '%(网页已撤销)'
        AND XXBT NOT LIKE '%英文%'
        AND XXBT NOT LIKE '%问询函%'
        AND XXBT NOT LIKE '%回复函%'
        AND (--临时公告
              (XXLB=70
                AND (XXBT LIKE '%业绩预%'
                OR XXBT LIKE '%预增%'
                OR XXBT LIKE '%预盈%'
                OR XXBT LIKE '%预亏%'
                OR XXBT LIKE '%预减%'
                OR XXBT LIKE '%扭亏%'
                OR XXBT LIKE '%业绩亏损%'
                OR XXBT LIKE '%业绩%增长%'
                OR XXBT LIKE '%业绩%下降%'
                OR XXBT LIKE '%质量回报双提升%'
                OR XXBT LIKE '%异常波动%'
                OR XXBT LIKE '%股价异动%'
                OR XXBT LIKE '%业绩快报%'
                OR XXBT LIKE '%营业收入简报%'
                OR XXBT LIKE '%保费收入%'
                OR XXBT LIKE '%经营情况%'
                OR XXBT LIKE '%经营业绩%'
                OR XXBT LIKE '%经营数据%'
                OR XXBT LIKE '%上市首日风险提示%')
             )
            OR
             --定报
             (XXLB=20 AND NRLB IN (17,6,23,5))
            OR
             --招股说明书
             (XXLB=10 AND NRLB IN (1,2,25)))
    ) t
    WHERE cnt <> 1
) B ON A.IGSDM = B.IGSDM AND A.XXFBRQ = B.XXFBRQ



--符合标题名称的公告数量=1 78780
SELECT A.ID,A.IGSDM, CONVERT(DATE,A.XXFBRQ) XXFBRQ, CONVERT(DATE,A.JZRQ) JZRQ, B.ID, B.XXBT
FROM (SELECT ID,IGSDM,XXFBRQ,JZRQ
      FROM [10.106.22.60].JYFIN.dbo.usrCWJCZBZYCYJYGCJB
      WHERE XXLYBM IS NULL
      ) A
JOIN (
        SELECT IGSDM, XXFBRQ, ID, XXBT
        FROM (
        SELECT A.IGSDM, A.XXFBRQ, A.ID, A.XXBT,
               COUNT(*) OVER (PARTITION BY A.IGSDM, A.XXFBRQ) AS cnt
        FROM [10.101.0.212].JYPRIME.dbo.usrGSGGYWFWB A
            JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B ON A.INBBM=B.INBBM AND B.ZQSC IN (18,83,90) AND B.ZQLB IN (1,2,41)
        WHERE XXBT NOT LIKE '%(网页已撤销)'
        AND XXBT NOT LIKE '%英文%'
        AND XXBT NOT LIKE '%问询函%'
        AND XXBT NOT LIKE '%复函%'
        AND (--临时公告
              (XXLB=70
                AND (XXBT LIKE '%业绩预%'
                OR XXBT LIKE '%预增%'
                OR XXBT LIKE '%预盈%'
                OR XXBT LIKE '%预亏%'
                OR XXBT LIKE '%预减%'
                OR XXBT LIKE '%扭亏%'
                OR XXBT LIKE '%业绩亏损%'
                OR XXBT LIKE '%业绩%增长%'
                OR XXBT LIKE '%业绩%下降%')
             )
            OR
             --招股说明书
             (XXLB=10 AND NRLB IN (1,2,25)))
    ) t
    WHERE cnt = 1
) B ON A.IGSDM = B.IGSDM AND A.XXFBRQ = B.XXFBRQ


SELECT A.ID,A.IGSDM, CONVERT(DATE,A.XXFBRQ) XXFBRQ,CONVERT(DATE,A.JZRQ) JZRQ, B.ID, B.XXBT
FROM (SELECT ID,IGSDM,XXFBRQ,JZRQ
      FROM [10.106.22.60].JYFIN.dbo.usrCWJCZBZYCYJYGCJB
      WHERE XXLYBM IS NULL AND YJYJLX!='FCC000001E1F'
      ) A
JOIN (
        SELECT IGSDM, XXFBRQ, ID, XXBT
        FROM (
        SELECT A.IGSDM, A.XXFBRQ, A.ID, A.XXBT,
               COUNT(*) OVER (PARTITION BY A.IGSDM, A.XXFBRQ) AS cnt
        FROM [10.101.0.212].JYPRIME.dbo.usrGSGGYWFWB A
            JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B ON A.INBBM=B.INBBM AND B.ZQSC IN (18,81,83,90) AND B.ZQLB IN (1,2,41)
        WHERE XXBT NOT LIKE '%(网页已撤销)'
        AND XXBT NOT LIKE '%英文%'
        AND XXBT NOT LIKE '%问询函%'
        AND XXBT NOT LIKE '%回复函%'
        AND (--临时公告
              (XXLB=70
                AND (XXBT LIKE '%业绩预%'
                OR XXBT LIKE '%预增%'
                OR XXBT LIKE '%预盈%'
                OR XXBT LIKE '%预亏%'
                OR XXBT LIKE '%预减%'
                OR XXBT LIKE '%扭亏%'
                OR XXBT LIKE '%业绩亏损%'
                OR XXBT LIKE '%业绩%增长%'
                OR XXBT LIKE '%业绩%下降%'
                OR XXBT LIKE '%质量回报双提升%'
                OR XXBT LIKE '%异常波动%'
                OR XXBT LIKE '%股价异动%'
                OR XXBT LIKE '%业绩快报%'
                OR XXBT LIKE '%营业收入简报%'
                OR XXBT LIKE '%保费收入%'
                OR XXBT LIKE '%经营情况%'
                OR XXBT LIKE '%经营业绩%'
                OR XXBT LIKE '%经营数据%'
                OR XXBT LIKE '%上市首日风险提示%')
             )
            OR
             --定报
             (XXLB=20 AND NRLB IN (17,6,23))
            OR
             --招股说明书
             (XXLB=10 AND NRLB IN (1,2,25)))
    ) t
    WHERE cnt = 1
) B ON A.IGSDM = B.IGSDM AND A.XXFBRQ = B.XXFBRQ

--经营计划
SELECT A.ID,CONVERT(DATE,A.XXFBRQ) XXFBRQ,CONVERT(DATE,A.JZRQ) JZRQ
FROM [10.106.22.60].JYFIN.dbo.usrCWJCZBZYCYJYGCJB A
WHERE YJYJLX!='FCC000001E1F' AND XXLYBM IN ('FCC00000005E','FCC00000005U') AND YEAR(XXFBRQ)=YEAR(JZRQ) AND MONTH(A.XXFBRQ) IN (3,4) AND FORMAT(JZRQ,'MM-dd')='12-31'



--非文本无公告  56条  人工
SELECT A.ID
FROM [10.106.22.60].JYFIN.dbo.usrCWJCZBZYCYJYGCJB A
    FULL JOIN [10.101.0.212].JYPRIME.dbo.usrGSGGYWFWB B ON A.IGSDM=B.IGSDM AND A.XXFBRQ=B.XXFBRQ
WHERE A.XXLYBM IS NULL AND B.ID IS NULL



--查找可能的标题名称
SELECT A.IGSDM,A.XXFBRQ,B.ID,B.XXBT
FROM (SELECT DISTINCT IGSDM,CONVERT(DATE,XXFBRQ) XXFBRQ
      FROM [10.106.22.60].JYFIN.dbo.usrCWJCZBZYCYJYGCJB
      WHERE XXLYBM='FCC000000065'
      ) A
    JOIN [10.101.0.212].JYPRIME.dbo.usrGSGGYWFWB B ON A.IGSDM=B.IGSDM AND A.XXFBRQ=B.XXFBRQ
WHERE B.XXBT NOT LIKE '%异常波动%'