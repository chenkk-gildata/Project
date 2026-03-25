# Tasks

- [x] Task 1: 优化模块处理统计表格，新增"等待处理"和"处理中"列
  - [x] SubTask 1.1: 修改 `_get_status_data` 函数，确保查询结果包含 `pending` 和 `processing` 状态
  - [x] SubTask 1.2: 修改 `_display_status_results` 函数，更新表格列定义和数据显示逻辑

- [x] Task 2: 优化失败模块列表入口，新增查看模式选择菜单
  - [x] SubTask 2.1: 新增 `_show_failed_menu` 函数，显示模式选择菜单（1.按时间查看 2.按模块查看）
  - [x] SubTask 2.2: 修改 `show_failed_modules` 函数，调用模式选择菜单

- [x] Task 3: 新增按模块查看失败记录功能
  - [x] SubTask 3.1: 新增 `_get_failed_by_module` 函数，按模块名和 `created_at` 日期查询失败记录
  - [x] SubTask 3.2: 新增 `_show_failed_by_module` 函数，展示模块选择菜单
  - [x] SubTask 3.3: 新增 `_show_module_failed_detail` 函数，展示选定模块的失败详情，支持按 `created_at` 动态筛选

# Task Dependencies
- Task 2 依赖 Task 1 完成
- Task 3 依赖 Task 2 完成
