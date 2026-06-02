# 申报稿领导人AI处理系统

## 项目结构

```
申报稿领导人介绍/
├── prospectus_leader.py          # 主程序入口
├── ai_service_enhanced.py        # AI服务模块
├── config.py                     # 配置文件
├── data_processor.py             # 数据处理模块
├── database_manager.py           # 数据库管理
├── downloader.py                 # 下载模块
├── leader_comparison_processor.py # 比对处理模块
├── logger_config.py              # 日志配置
├── path_utils.py                 # 路径工具
├── pdf_processor.py              # PDF处理模块
├── daily.sql                     # SQL查询模板(新增)
├── compare.sql                   # SQL查询模板(比对)
├── prompt_step1_table.md         # AI提示词-表格转录
├── prompt_step2_rules.md         # AI提示词-规则推理
├── 领导人序号及业务分数.xlsx      # 参考数据
├── prospectus_leader.spec        # PyInstaller打包配置
└── README.md                     # 说明文档
```

## 运行环境

- Python 3.10+
- 依赖包：
  - openpyxl
  - pandas
  - requests
  - tenacity

## 安装依赖

```bash
pip install openpyxl pandas requests tenacity
```

## 运行程序

```bash
python prospectus_leader.py
```

## 打包为EXE

### 安装PyInstaller

```bash
pip install pyinstaller
```

### 执行打包

```bash
cd e:\Project\申报稿领导人介绍
pyinstaller prospectus_leader.spec
```

打包完成后，可执行文件位于 `dist/申报稿领导人AI处理系统 V1.0.exe`

### 打包后文件部署

将以下文件放在exe所在目录：
- `prompt_step1_table.md` - AI提示词文件
- `prompt_step2_rules.md` - AI提示词文件
- `daily.sql` - SQL查询模板
- `compare.sql` - SQL查询模板

**目录结构示例**：
```
申报稿领导人AI处理系统/
├── 申报稿领导人AI处理系统 V1.0.exe
├── prompt_step1_table.md
├── prompt_step2_rules.md
├── daily.sql
├── compare.sql
├── files/          (运行时自动创建)
├── logs/           (运行时自动创建)
└── output/         (运行时自动创建)
```

## 功能说明

### 模式1：运行新增数据公告处理
- 支持日期查询和MD5查询
- 下载公告PDF
- AI提取领导人数据
- 输出Excel文件

### 模式2：运行已有数据公告处理
- 支持日期查询和MD5查询
- 与数据库已有数据比对
- 生成比对报告

### 模式3：仅提取-新增
- 从指定目录提取PDF
- AI提取领导人数据
- 输出Excel文件

### 模式4：仅提取比对-已有
- 从指定目录提取PDF
- AI提取并与数据库比对
- 生成比对报告

## 输出目录

- `files/` - 下载的PDF文件
- `logs/` - 日志文件
- `output/新增/` - 新增数据Excel输出
- `output/比对/` - 比对结果Excel输出
