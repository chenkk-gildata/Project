# AI服务两步调用改造计划

## 目标

将 `ai_service_enhanced.py` 中的AI调用从一次改为两次，实现与 `test_v10_run.py` 相同的两步架构。

## 现状分析

### 当前 `ai_service_enhanced.py` 的调用方式

* `extract_data_from_file(file_id, system_prompt)` 方法

* 一次调用，直接返回JSON结果

* 始终引用文件ID

### 目标架构

| 步骤    | Prompt                            | 文件ID  | 返回格式 |
| ----- | --------------------------------- | ----- | ---- |
| Step1 | prompt\_step1\_table.md           | ✅ 引用  | 文本   |
| Step2 | prompt\_step2\_rules.md + Step1结果 | ❌ 不引用 | JSON |

## 实现步骤

### 1. 添加新的调用方法 `call_api`

* 参数：`prompt`, `json_mode=True`, `use_file=True`

* 功能：底层API调用方法，支持灵活配置

* 逻辑：

  * 如果 `use_file=True`，在 messages 中添加 `fileid://{file_id}`

  * 如果 `json_mode=True`，设置 `response_format={"type": "json_object"}`

### 2. 添加两步提取方法 `extract_data_two_step`

* 参数：`file_id`, `prompt1_path`, `prompt2_path`

* Step1：调用 `call_api(prompt1, json_mode=False, use_file=True)`

  * 返回格式化的表格文本

* Step2：调用 `call_api(step2_prompt, json_mode=True, use_file=False)`

  * step2\_prompt = 表格数据 + prompt2

  * 返回结构化JSON

### 3. 保留原有方法兼容性

* `extract_data_from_file` 保持不变（向后兼容）

* 新增 `extract_data_two_step` 作为新的主要方法

### 4. 添加Prompt文件路径配置

* 在 `config.py` 或类初始化时配置prompt文件路径

* 支持自定义prompt路径

## 代码修改清单

### `ai_service_enhanced.py`

1. 新增 `call_api()` 方法（底层调用）
2. 新增 `extract_data_two_step()` 方法（两步提取）
3. 新增 `_load_prompt()` 方法（加载prompt文件）

### 配置项

* `prompt_step1_path`: prompt\_step1\_table.md 路径

* `prompt_step2_path`: prompt\_step2\_rules.md 路径

## 方法签名设计

```python
def call_api(self, prompt: str, json_mode: bool = True, use_file: bool = True, file_id: str = None) -> str:
    """底层API调用方法"""
    pass

def extract_data_two_step(self, file_id: str, prompt1_path: str = None, prompt2_path: str = None) -> Dict[str, Any]:
    """两步提取：Step1表格转录 → Step2规则推理"""
    pass
```

## 风险与注意事项

1. 两步调用会增加API调用次数和耗时
2. Step1的输出可能过长，需要注意token限制
3. 需要处理Step1失败的情况

