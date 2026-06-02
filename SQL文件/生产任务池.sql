select b.combination_name,a.*
from task_job_20241013 a
    join task_pool_config b on a.pool_config_id = b.id
where b.combination_name like '生产任务-股票任务%'
  and b.is_effect = 1
  and a.create_time >= '2026-01-01'