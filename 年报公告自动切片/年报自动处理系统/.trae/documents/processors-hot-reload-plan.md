# 业务处理模块热更新方案（自动检测版）

## 背景

当前架构中，业务处理器在 `TaskDispatcher` 初始化时创建实例。如果需要优化处理器逻辑，必须停止程序、修改代码、重新启动。

## 目标

实现业务处理模块的自动热更新，**无需手动触发**，只需保存修改后的处理器文件即可自动重载。

## 方案设计

### 自动检测机制

程序自动监控 `processors` 目录下所有 `.py` 文件的修改时间，当检测到文件被修改时自动重载：

```
用户操作：修改 processors/zyzb_processor.py → 保存
程序行为：检测到文件变化 → 自动重载处理器
```

### 性能影响分析

| 操作 | 耗时 | 频率 |
|------|------|------|
| `os.path.getmtime()` | ~0.1ms | 每10秒×5文件 |
| `glob.glob()` | ~1ms | 每10秒1次 |
| 总计 | ~1.5ms/10秒 | 几乎可忽略 |

**结论**：文件修改时间检测是极其轻量的系统调用，每10秒执行一次对程序性能**几乎没有影响**。

### 优化策略

**智能检测**：只在系统空闲时检测文件变化，避免在处理任务时产生任何开销。

```python
# 只在队列为空且没有活跃任务时才检测
if proc_queue.empty() and active_tasks == 0:
    if self._check_files_changed():
        self._reload_processors()
```

### 核心实现

#### 修改 `task_dispatcher.py`

```python
import importlib
import os
import glob

class TaskDispatcher:
    def __init__(self):
        # ... 现有代码 ...
        self._processor_files = {}  # 文件路径 -> 修改时间
        self._processors_dir = os.path.join(BASE_DIR, "processors")
        
        # 初始化时记录所有处理器文件的修改时间
        self._init_file_timestamps()
    
    def _init_file_timestamps(self):
        """初始化处理器文件的时间戳"""
        pattern = os.path.join(self._processors_dir, "*.py")
        for file_path in glob.glob(pattern):
            if "__" not in file_path:  # 排除 __init__.py 等
                self._processor_files[file_path] = os.path.getmtime(file_path)
    
    def _check_files_changed(self) -> bool:
        """检查处理器文件是否有变化"""
        pattern = os.path.join(self._processors_dir, "*.py")
        current_files = set(glob.glob(pattern))
        
        for file_path in current_files:
            if "__" in file_path:
                continue
            current_mtime = os.path.getmtime(file_path)
            if file_path not in self._processor_files:
                self._processor_files[file_path] = current_mtime
                return True
            elif self._processor_files[file_path] < current_mtime:
                self._processor_files[file_path] = current_mtime
                return True
        
        return False
    
    def _reload_processors(self):
        """热重载所有处理器模块"""
        try:
            logger.info("检测到处理器文件变化，开始热重载...")
            
            # 重载各个处理器模块
            import processors.zyzb_processor
            import processors.ldrjs_processor
            import processors.yftr_processor
            import processors.zggc_processor
            import processors.ldrcg_processor
            
            importlib.reload(processors.zyzb_processor)
            importlib.reload(processors.ldrjs_processor)
            importlib.reload(processors.yftr_processor)
            importlib.reload(processors.zggc_processor)
            importlib.reload(processors.ldrcg_processor)
            
            # 重新创建处理器实例
            from processors.zyzb_processor import ZyzbProcessor
            from processors.ldrjs_processor import LdrjsProcessor
            from processors.yftr_processor import YftrProcessor
            from processors.zggc_processor import ZggcProcessor
            from processors.ldrcg_processor import LdrcgProcessor
            
            self.processors = {
                "主要指标": ZyzbProcessor(),
                "领导人介绍": LdrjsProcessor(),
                "研发投入": YftrProcessor(),
                "职工构成": ZggcProcessor(),
                "领导人持股": LdrcgProcessor()
            }
            
            logger.info("处理器模块热重载完成")
            return True
        except Exception as e:
            logger.error(f"处理器模块热重载失败: {e}")
            return False
```

#### 在分发循环中添加智能检测

```python
def _dispatch_loop(self):
    """分发主循环"""
    logger.info("任务分发器开始工作")
    
    self._recover_pending_tasks()
    
    last_recovery_time = time.time()
    last_check_time = time.time()
    recovery_interval = 300
    check_interval = 10  # 检测间隔10秒
    
    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
        self._executor = executor
        
        while self._running and not self._stop_event.is_set():
            try:
                process_queue = queue_manager.get_process_queue()
                
                # 从队列获取任务
                task = process_queue.get(block=True, timeout=1)
                
                if task:
                    executor.submit(self._process_task, task)
                
                # 智能检测：只在空闲时检查文件变化
                current_time = time.time()
                if current_time - last_check_time >= check_interval:
                    # 只有在队列为空且没有活跃任务时才检测
                    if process_queue.empty() and self._active_tasks == 0:
                        if self._check_files_changed():
                            self._reload_processors()
                    last_check_time = current_time
                
                # ... 其余代码不变 ...
```

## 使用方式

```bash
# 1. 直接修改处理器代码并保存
# 编辑 processors/zyzb_processor.py → Ctrl+S 保存

# 2. 等待程序空闲时自动重载（队列空、无活跃任务）
# 日志输出：检测到处理器文件变化，开始热重载...
#          处理器模块热重载完成

# 无需任何额外操作！
```

## 实现步骤

### Step 1: 修改 `task_dispatcher.py` 导入部分

添加必要的导入：
```python
import importlib
import glob
```

### Step 2: 修改 `__init__` 方法

添加文件监控相关属性和初始化

### Step 3: 实现文件变化检测方法

- `_init_file_timestamps()` - 初始化时间戳
- `_check_files_changed()` - 检测文件变化

### Step 4: 实现处理器重载方法

- `_reload_processors()` - 重载所有处理器

### Step 5: 修改分发循环

在 `_dispatch_loop` 中添加智能文件变化检测逻辑

## 性能对比

| 方案 | 检测频率 | 性能影响 | 适用场景 |
|------|----------|----------|----------|
| 固定间隔检测 | 每10秒 | ~1.5ms/10秒，可忽略 | 通用 |
| 智能检测（推荐） | 空闲时 | 几乎为零 | 推荐 |

## 优点

| 特性 | 说明 |
|------|------|
| **零操作** | 只需保存文件，无需额外触发 |
| **零影响** | 智能检测只在空闲时执行，对性能几乎无影响 |
| **安全重载** | 重载失败时保留原有处理器 |
| **无侵入** | 不影响正在执行的任务 |

## 注意事项

1. **空闲时重载**：程序会在队列空闲时自动检测并重载
2. **语法检查**：保存前确保代码无语法错误
3. **日志确认**：查看日志确认重载成功
