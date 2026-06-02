# MD5查询功能新增计划

## 目标

在模式1（新增数据公告处理）和模式2（已有数据公告处理）中新增按MD5查询公告的功能。

***

## 完整查询逻辑说明

### 模式1：新增数据公告处理

#### 原始SQL (daily.sql)

```sql
SELECT HASHCODE, B.GPDM, B.ZQJC, CONVERT(DATE, A.XXFBRQ) AS XXFBRQ, A.XXBT, A.FBSJ
FROM [10.101.0.212].JYPRIME.dbo.usrGSGGYWFWB A
JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B ON A.INBBM = B.INBBM 
     AND B.ZQSC IN (83, 90, 18) AND B.ZQLB IN (1, 2, 41)
WHERE A.XXLB = 10 
AND A.NRLB IN (1, 25, 101)
AND A.XXLY IN (69, 70, 410007600) 
AND A.MTCC IN ('上海证券交易所','深圳证券交易所','北京证券交易所')
AND A.XXFBRQ > CONVERT(DATE,GETDATE()-30)  -- 日期条件
AND A.XXBT LIKE '%申报稿%'
AND NOT EXISTS (
     SELECT 1 FROM [10.101.0.212].JYPRIME.dbo.usrGSZYLDRJS X WHERE X.IGSDM=A.IGSDM
)  -- 排除已有记录
```

#### 优化后两种查询方式

| 查询方式      | SQL处理                           | 最终SQL条件                                        |
| --------- | ------------------------------- | ---------------------------------------------- |
| **日期查询**  | 替换日期条件                          | `AND A.XXFBRQ BETWEEN '...'` + 保留 `NOT EXISTS` |
| **MD5查询** | 移除日期条件 + 移除NOT EXISTS + 添加MD5条件 | `AND HASHCODE IN (...)`                        |

**日期查询最终SQL：**

```sql
WHERE A.XXLB = 10 
AND A.NRLB IN (1, 25, 101)
AND A.XXLY IN (69, 70, 410007600) 
AND A.MTCC IN ('上海证券交易所','深圳证券交易所','北京证券交易所')
AND A.XXFBRQ BETWEEN '2026-05-01' AND '2026-05-27'  -- 用户指定日期
AND A.XXBT LIKE '%申报稿%'
AND NOT EXISTS (SELECT 1 FROM ... WHERE X.IGSDM=A.IGSDM)  -- 保留
```

**MD5查询最终SQL：**

```sql
WHERE A.XXLB = 10 
AND A.NRLB IN (1, 25, 101)
AND A.XXLY IN (69, 70, 410007600) 
AND A.MTCC IN ('上海证券交易所','深圳证券交易所','北京证券交易所')
-- 日期条件被移除
AND A.XXBT LIKE '%申报稿%'
-- NOT EXISTS 条件被移除
AND HASHCODE IN ('abc123', 'def456', 'ghi789')  -- 用户指定MD5
```

***

### 模式2：已有数据公告处理

#### 原始SQL (compare.sql)

```sql
SELECT HASHCODE, B.GPDM, B.ZQJC, CONVERT(DATE, A.XXFBRQ) AS XXFBRQ, A.XXBT, A.FBSJ
FROM [10.101.0.212].JYPRIME.dbo.usrGSGGYWFWB A
JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B ON A.INBBM = B.INBBM 
     AND B.ZQSC IN (83, 90, 18) AND B.ZQLB IN (1, 2, 41)
WHERE A.XXLB = 10 
AND A.NRLB IN (1, 25, 101)
AND A.XXLY IN (69, 70, 410007600) 
AND A.MTCC IN ('上海证券交易所','深圳证券交易所','北京证券交易所')
AND A.XXFBRQ > CONVERT(DATE,GETDATE()-30)  -- 日期条件
AND A.XXBT NOT LIKE '%上市发行公告'
AND EXISTS (
     SELECT 1 FROM [10.101.0.212].JYPRIME.dbo.usrGSZYLDRJS X WHERE X.IGSDM=A.IGSDM
)  -- 只查已有记录
```

#### 优化后两种查询方式

| 查询方式      | SQL处理                       | 最终SQL条件                                    |
| --------- | --------------------------- | ------------------------------------------ |
| **日期查询**  | 替换日期条件                      | `AND A.XXFBRQ BETWEEN '...'` + 保留 `EXISTS` |
| **MD5查询** | 移除日期条件 + 移除EXISTS + 添加MD5条件 | `AND HASHCODE IN (...)`                    |

**日期查询最终SQL：**

```sql
WHERE A.XXLB = 10 
AND A.NRLB IN (1, 25, 101)
AND A.XXLY IN (69, 70, 410007600) 
AND A.MTCC IN ('上海证券交易所','深圳证券交易所','北京证券交易所')
AND A.XXFBRQ BETWEEN '2026-05-01' AND '2026-05-27'  -- 用户指定日期
AND A.XXBT NOT LIKE '%上市发行公告'
AND EXISTS (SELECT 1 FROM ... WHERE X.IGSDM=A.IGSDM)  -- 保留
```

**MD5查询最终SQL：**

```sql
WHERE A.XXLB = 10 
AND A.NRLB IN (1, 25, 101)
AND A.XXLY IN (69, 70, 410007600) 
AND A.MTCC IN ('上海证券交易所','深圳证券交易所','北京证券交易所')
-- 日期条件被移除
AND A.XXBT NOT LIKE '%上市发行公告'
-- EXISTS 条件被移除
AND HASHCODE IN ('abc123', 'def456', 'ghi789')  -- 用户指定MD5
```

***

## 总结：SQL条件处理对照表

### 模式1 (daily.sql)

| 条件                            | 日期查询       | MD5查询                    |
| ----------------------------- | ---------- | ------------------------ |
| 基础条件 (XXLB, NRLB, XXLY, MTCC) | ✅ 保留       | ✅ 保留                     |
| 日期条件 `XXFBRQ > GETDATE()-30`  | 🔄 替换为用户日期 | ❌ 移除                     |
| 业务条件 `XXBT LIKE '%申报稿%'`      | ✅ 保留       | ✅ 保留                     |
| `NOT EXISTS (领导人记录)`          | ✅ 保留       | ❌ 移除                     |
| MD5条件                         | ❌ 无        | ✅ 添加 `HASHCODE IN (...)` |

### 模式2 (compare.sql)

| 条件                             | 日期查询       | MD5查询                    |
| ------------------------------ | ---------- | ------------------------ |
| 基础条件 (XXLB, NRLB, XXLY, MTCC)  | ✅ 保留       | ✅ 保留                     |
| 日期条件 `XXFBRQ > GETDATE()-30`   | 🔄 替换为用户日期 | ❌ 移除                     |
| 业务条件 `XXBT NOT LIKE '%上市发行公告'` | ✅ 保留       | ✅ 保留                     |
| `EXISTS (领导人记录)`               | ✅ 保留       | ❌ 移除                     |
| MD5条件                          | ❌ 无        | ✅ 添加 `HASHCODE IN (...)` |

***

## 实现步骤

### 1. 修改 `downloader.py`

#### 1.1 新增 `get_md5_list()` 方法

```python
def get_md5_list(self) -> List[str]:
    """获取用户输入的MD5列表"""
    print("\n请输入MD5列表（支持逗号、空格、换行分隔）:")
    print("输入 0 返回上一级")
    
    while True:
        md5_input = input("\nMD5列表: ").strip()
        
        if md5_input == "0":
            return None
        
        # 支持逗号、空格、换行分隔
        import re
        md5_list = re.split(r'[,\s\n]+', md5_input)
        md5_list = [md5.strip() for md5 in md5_list if md5.strip()]
        
        if not md5_list:
            print("未输入有效的MD5，请重新输入")
            continue
        
        # 去重
        md5_list = list(dict.fromkeys(md5_list))
        
        print(f"\n解析到 {len(md5_list)} 个MD5")
        confirm = input("确认查询? (Y/n): ").strip().lower()
        if confirm in ['', 'y', 'yes']:
            return md5_list
```

#### 1.2 新增 `_inject_md5_condition()` 方法

```python
def _inject_md5_condition(self, sql_content: str, md5_list: List[str]) -> str:
    """MD5查询：移除日期限制和EXISTS条件，添加MD5条件"""
    import re
    
    # 1. 移除日期条件
    date_pattern = r"AND\s+A\.XXFBRQ\s*>\s*CONVERT\s*\(\s*DATE\s*,\s*GETDATE\s*\(\s*\)\s*-\s*30\s*\)"
    sql_content = re.sub(date_pattern, "", sql_content, flags=re.IGNORECASE)
    
    # 2. 移除 EXISTS / NOT EXISTS 条件（多行匹配）
    exists_pattern = r"AND\s+(NOT\s+)?EXISTS\s*\(\s*SELECT\s+1\s+FROM\s+\[10\.101\.0\.212\]\.JYPRIME\.dbo\.usrGSZYLDRJS\s+X\s+WHERE\s+X\.IGSDM\s*=\s*A\.IGSDM\s*\)"
    sql_content = re.sub(exists_pattern, "", sql_content, flags=re.IGNORECASE)
    
    # 3. 添加MD5条件
    md5_values = ", ".join(f"'{md5}'" for md5 in md5_list)
    md5_condition = f"AND HASHCODE IN ({md5_values})"
    
    sql_content = sql_content.rstrip()
    if sql_content.endswith(';'):
        sql_content = sql_content[:-1]
    sql_content = sql_content + "\n" + md5_condition
    
    return sql_content
```

#### 1.3 新增 `query_announcements_by_md5()` 方法

```python
def query_announcements_by_md5(self, md5_list: List[str], sql_file: str = 'daily.sql') -> List[Dict[str, Any]]:
    """按MD5查询公告"""
    sql_content = self.load_sql_from_file(sql_file)
    sql_content = self._inject_md5_condition(sql_content, md5_list)
    
    try:
        results = db_manager.execute_query(sql_content)
        logger.info(f"MD5查询到 {len(results)} 条公告记录 (SQL: {sql_file})")
        return results
    except Exception as e:
        logger.error(f"MD5查询公告失败: {e}")
        raise
```

#### 1.4 新增 `create_download_folder_by_md5()` 方法

```python
def create_download_folder_by_md5(self, session_id: str = None) -> str:
    """为MD5查询创建下载文件夹"""
    files_dir = get_files_dir()
    
    if session_id is None:
        timestamp = datetime.now().strftime('%H%M%S')
    else:
        timestamp = session_id
    
    folder_name = f"md5_{datetime.now().strftime('%Y%m%d')}_{timestamp}"
    download_folder = os.path.join(files_dir, folder_name)
    
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
    
    logger.info(f"创建MD5下载文件夹: {download_folder}")
    return download_folder
```

### 2. 修改 `prospectus_leader.py`

#### 2.1 新增 `get_query_mode_choice()` 函数

```python
def get_query_mode_choice():
    """获取查询方式选择"""
    print("\n请选择查询方式:")
    print("1. 日期查询")
    print("2. MD5查询")
    print("0. 返回上一级")
    
    while True:
        choice = input("\n请输入选项 (0-2): ").strip()
        if choice in ["0", "1", "2"]:
            return choice
        print("无效选项，请重新选择")
```

#### 2.2 新增 `run_download_and_slice_by_md5()` 函数

```python
def run_download_and_slice_by_md5(downloader, md5_list, session_id: str, sql_file: str):
    """执行MD5查询的下载和切片流程"""
    logger.info(f"MD5列表: {md5_list}")
    print(f"\n解析到 {len(md5_list)} 个MD5")
    
    announcements = downloader.query_announcements_by_md5(md5_list, sql_file)
    
    if not announcements:
        print("未查询到符合条件的公告")
        logger.info("未查询到符合条件的公告")
        return None
    
    print(f"\n查询到 {len(announcements)} 条公告")
    logger.info(f"查询到 {len(announcements)} 条公告")
    
    download_folder = downloader.create_download_folder_by_md5(session_id)
    print(f"下载文件夹: {download_folder}")
    logger.info(f"下载文件夹: {download_folder}")
    
    # 后续流程与日期查询相同...
    print("\n开始下载公告...")
    downloaded_count, failed_count, failed_files = downloader.download_batch(
        announcements, download_folder
    )
    
    print(f"\n下载完成! 成功: {downloaded_count}, 失败: {failed_count}")
    logger.info(f"下载完成! 成功: {downloaded_count}, 失败: {failed_count}")
    
    if failed_files:
        print(f"失败的文件: {failed_files[:5]}...")
        logger.warning(f"失败的文件: {failed_files}")
    
    pdf_files = list(Path(download_folder).glob("*.pdf"))
    
    if not pdf_files:
        print("未找到下载的PDF文件")
        logger.warning("未找到下载的PDF文件")
        return None
    
    print(f"\n找到 {len(pdf_files)} 个PDF文件")
    logger.info(f"找到 {len(pdf_files)} 个PDF文件")
    
    pdf_processor = PDFProcessor()
    
    print("\n开始PDF切片处理...")
    success_count, failed_count = pdf_processor.process_batch([str(f) for f in pdf_files])
    
    logger.info(f"PDF处理完成! 成功: {success_count}, 失败: {failed_count}")
    
    processed_pdf_files = list(Path(download_folder).glob("*.pdf"))
    
    if not processed_pdf_files:
        print("未找到处理后的PDF文件")
        logger.warning("未找到处理后的PDF文件")
        return None
    
    print(f"\n找到 {len(processed_pdf_files)} 个处理后的PDF文件")
    return download_folder
```

#### 2.3 修改 `run_new_data_process()` 函数

```python
def run_new_data_process(session_id: str):
    """选项1：运行新增数据公告处理"""
    downloader = AnnouncementDownloader()
    
    query_mode = get_query_mode_choice()
    if query_mode == "0":
        return False
    
    if query_mode == "1":
        # 日期查询 - 原有流程
        start_date, end_date = downloader.get_date_range()
        if start_date is None:
            return False
        download_folder = run_download_and_slice(
            downloader, start_date, end_date, session_id, 'daily.sql'
        )
    else:
        # MD5查询 - 新流程
        md5_list = downloader.get_md5_list()
        if md5_list is None:
            return False
        download_folder = run_download_and_slice_by_md5(
            downloader, md5_list, session_id, 'daily.sql'
        )
    
    if download_folder is None:
        return False
    
    run_ai_extraction(download_folder, "新增")
    return True
```

#### 2.4 修改 `run_existing_data_process()` 函数

```python
def run_existing_data_process(session_id: str):
    """选项2：运行已有数据公告处理"""
    downloader = AnnouncementDownloader()
    
    query_mode = get_query_mode_choice()
    if query_mode == "0":
        return False
    
    if query_mode == "1":
        # 日期查询 - 原有流程
        start_date, end_date = downloader.get_date_range()
        if start_date is None:
            return False
        download_folder = run_download_and_slice(
            downloader, start_date, end_date, session_id, 'compare.sql'
        )
    else:
        # MD5查询 - 新流程
        md5_list = downloader.get_md5_list()
        if md5_list is None:
            return False
        download_folder = run_download_and_slice_by_md5(
            downloader, md5_list, session_id, 'compare.sql'
        )
    
    if download_folder is None:
        return False
    
    results = run_ai_extraction(download_folder, "比对", skip_excel_output=True)
    _run_comparison(results, session_id)
    return True
```

***

## 用户交互流程

```
请选择运行模式:
1. 运行新增数据公告处理
2. 运行已有数据公告处理
...
请输入选项: 1

请选择查询方式:
1. 日期查询
2. MD5查询
0. 返回上一级
请输入选项 (0-2): 2

请输入MD5列表（支持逗号、空格、换行分隔）:
输入 0 返回上一级

MD5列表: abc123, def456
         ghi789

解析到 3 个MD5
确认查询? (Y/n): y

查询到 3 条公告
...
```

