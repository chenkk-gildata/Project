# 新增运行模式3和4计划（修订版）

## 需求概述

新增两个运行模式，跳过下载和切片环节，直接对用户指定目录下的PDF进行提取或比对：

| 模式 | 名称         | 流程                                     |
| -- | ---------- | -------------------------------------- |
| 1  | 运行新增数据公告处理 | 询问日期 → 下载 → 切片 → AI提取 → Excel输出（新增）    |
| 2  | 运行已有数据公告处理 | 询问日期 → 下载 → 切片 → AI提取 → 比对 → 比对结果Excel |
| 3  | 仅提取-新增     | 询问目录 → AI提取 → Excel输出（新增）              |
| 4  | 仅提取比对-已有   | 询问目录 → AI提取 → 比对 → 比对结果Excel           |

## 重要变更点（用户反馈）

### 1. 模式2和4的流程修正

* 模式2/4在AI提取后**不输出**各个文件的提取结果Excel（像模式1那样）

* 直接进行比对，最终只输出**一个总的比对结果文件**

* 比对结果文件命名：`比对结果-{session_id}.xlsx`

### 2. 模式3和4无metadata.json

* 从文件名中提取GPDM和XXFBRQ

* 文件名格式固定：`GPDM-XXFBRQ-XXBT.pdf`

* 需新增 `_parse_pdf_filename` 函数解析文件名

### 3. 输出路径优化

* 模式1/3（新增）：`output/新增/{session_folder}/ZQJC-GPDM-XXFBRQ.xlsx`（每个公告一个Excel，保持不变）

* 模式2/4（比对）：`output/比对/比对结果-{session_id}.xlsx`（单个比对结果文件）

### 4. 去除证券简称(ZQJC)

* 从整个程序中移除ZQJC字段的获取和输出

* Excel命名改为 `GPDM-XXFBRQ.xlsx`（原来是 `ZQJC-GPDM-XXFBRQ.xlsx`）

* OUTPUT\_FIELDS中移除"证券简称"列

* metadata中不再依赖ZQJC

## 实现步骤

### 步骤1：修改 `data_processor.py` - 去除ZQJC

* `OUTPUT_FIELDS` 中移除 "证券简称"

* `_output_excel` 方法中移除ZQJC相关逻辑

* Excel命名从 `{zqjc}-{gpdm}-{xxfbrq}.xlsx` 改为 `{gpdm}-{xxfbrq}.xlsx`

### 步骤2：修改 `data_processor.py` - 支持从文件名提取元数据

* 新增 `_extract_metadata_from_filename` 方法

* 在 `process_all_files` 中：优先用metadata.json，若不存在则从文件名提取

### 步骤3：修改 `prospectus_leader.py` - 菜单扩展

* `get_main_menu_choice` 新增选项3和4

* 选项验证范围扩展为 `0-4`

### 步骤4：新增 `get_pdf_directory` 函数

* 询问用户输入PDF目录路径

* 验证目录存在且包含PDF文件

* 输入0返回上一级

* 返回目录路径或None

### 步骤5：新增 `run_extract_only_new` 函数（模式3）

* 调用 `get_pdf_directory` 获取用户指定目录

* 调用 `run_ai_extraction(directory, "新增")` 执行AI提取和Excel输出

### 步骤6：修改 `run_existing_data_process` 函数（模式2）

* AI提取后不再输出各文件的提取结果Excel

* 直接进行比对，输出单个比对结果文件 `比对结果-{session_id}.xlsx`

### 步骤7：新增 `run_extract_only_compare` 函数（模式4）

* 调用 `get_pdf_directory` 获取用户指定目录

* AI提取后直接比对，输出单个比对结果文件

### 步骤8：修改 `main` 函数

* 新增 `choice == "3"` 和 `choice == "4"` 的处理

### 步骤9：修改 `leader_comparison_processor.py`

* `generate_comparison_report` 支持自定义文件名（含session\_id）

* 去除比对结果中ZQJC相关字段

### 步骤10：验证语法

## 涉及文件

| 文件                               | 修改内容                               |
| -------------------------------- | ---------------------------------- |
| `data_processor.py`              | 去除ZQJC、支持从文件名提取元数据、模式2/4不输出提取Excel |
| `prospectus_leader.py`           | 新增菜单3/4、新增函数、修改模式2流程               |
| `leader_comparison_processor.py` | 报告命名含session\_id、去除ZQJC            |

