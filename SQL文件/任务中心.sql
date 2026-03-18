select a.combination_name 任务节点
    , JSON_EXTRACT(a.table_template, '$.antBaseEditList[*].templateNameChn') AS 涉及中文表名
    , b.add_time 资源中心入库时间
    , b.code 股票代码
    , b.release_date 信息发布日期
    , b.title 信息标题
    , b.product_last_time 生产时间
    , b.job_unique_id 任务ID
from task_pool_config a
        join task_job_20241013 b on a.id = b.pool_config_id
    and locate('生产任务-股票任务', a.combination_name) = 1
    and a.is_effect = 1 -- and a.is_check_inside = 1
    and b.create_time >= '2024-10-13' and b.create_time < '2025-02-01'



SELECT
    a.combination_name AS 任务节点,
    jt.template_name_chn AS 涉及中文表名,
    b.add_time AS 资源中心入库时间,
    YEAR(b.add_time) AS 年份,
    AVG(TIMESTAMPDIFF(MINUTE, b.add_time, b.product_last_time)) AS timeline
FROM
    task_pool_config a
    INNER JOIN task_job_20241013 b ON a.id = b.pool_config_id
    INNER JOIN JSON_TABLE(
        a.table_template,
        '$.antBaseEditList[*]' COLUMNS (
            template_name_chn VARCHAR(255) PATH '$.templateNameChn'
        )
    ) AS jt
WHERE
    a.combination_name LIKE '生产任务-股票任务%'
    AND a.is_effect = 1
    AND b.create_time >= '2025-01-01'
    AND b.create_time < '2026-01-01'
GROUP BY
    jt.template_name_chn,
    YEAR(b.add_time)
