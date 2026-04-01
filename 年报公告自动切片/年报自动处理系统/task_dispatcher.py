"""
任务分发器 - 年报公告自动处理系统
负责从处理队列获取任务并分发给各个业务处理器
"""
import os
import time
import threading
import importlib
import glob
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional

from config import PROCESS_CONFIG, BASE_DIR
from models import ProcessTask, ProcessStatus, DownloadStatus
from database import db
from logger import logger
from queues import queue_manager

from processors.zyzb_processor import ZyzbProcessor
from processors.ldrjs_processor import LdrjsProcessor
from processors.yftr_processor import YftrProcessor
from processors.zggc_processor import ZggcProcessor
from processors.ldrcg_processor import LdrcgProcessor
from processors.bjjs_processor import BjjsProcessor


class TaskDispatcher:
    """任务分发器"""
    
    def __init__(self):
        self.max_workers = 8 # len(PROCESS_CONFIG["modules"])
        self._running = False
        self._stop_event = threading.Event()
        self._executor: Optional[ThreadPoolExecutor] = None
        self._dispatch_thread: Optional[threading.Thread] = None
        
        self.processors: Dict[str, object] = {
            "主要指标": ZyzbProcessor(),
            "领导人介绍": LdrjsProcessor(),
            "研发投入": YftrProcessor(),
            "职工构成": ZggcProcessor(),
            "领导人持股": LdrcgProcessor(),
            "股东背景介绍": BjjsProcessor()
        }
        
        self._active_tasks = 0
        self._lock = threading.Lock()
        self._announcement_status: Dict[str, dict] = {}
        self._file_names: Dict[str, str] = {}
        self._no_recovery_consecutive = 0
        
        # 热更新相关属性
        self._processor_files: Dict[str, float] = {}
        self._processors_dir = os.path.join(BASE_DIR, "processors")
        self._init_file_timestamps()
    
    def _init_file_timestamps(self):
        """初始化处理器文件的时间戳"""
        pattern = os.path.join(self._processors_dir, "*.py")
        for file_path in glob.glob(pattern):
            if "__" not in file_path:
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
            
            import processors.zyzb_processor
            import processors.ldrjs_processor
            import processors.yftr_processor
            import processors.zggc_processor
            import processors.ldrcg_processor
            import processors.bjjs_processor
            
            importlib.reload(processors.zyzb_processor)
            importlib.reload(processors.ldrjs_processor)
            importlib.reload(processors.yftr_processor)
            importlib.reload(processors.zggc_processor)
            importlib.reload(processors.ldrcg_processor)
            importlib.reload(processors.bjjs_processor)
            
            from processors.zyzb_processor import ZyzbProcessor
            from processors.ldrjs_processor import LdrjsProcessor
            from processors.yftr_processor import YftrProcessor
            from processors.zggc_processor import ZggcProcessor
            from processors.ldrcg_processor import LdrcgProcessor
            from processors.bjjs_processor import BjjsProcessor
            
            self.processors = {
                "主要指标": ZyzbProcessor(),
                "领导人介绍": LdrjsProcessor(),
                "研发投入": YftrProcessor(),
                "职工构成": ZggcProcessor(),
                "领导人持股": LdrcgProcessor(),
                "股东背景介绍": BjjsProcessor()
            }
            
            logger.info("处理器模块热重载完成")
            return True
        except Exception as e:
            logger.error(f"处理器模块热重载失败: {e}")
            return False
    
    def _recover_pending_tasks(self):
        """恢复待处理的任务"""
        try:
            pending_announcements = db.get_pending_processes(limit=100)
            
            if not pending_announcements:
                if self._no_recovery_consecutive == 0:
                    logger.info("没有需要恢复的处理任务")
                self._no_recovery_consecutive += 1
                return
            
            self._no_recovery_consecutive = 0
            
            process_queue = queue_manager.get_process_queue()
            recovered_announcements = 0
            recovered_modules = 0
            re_download_count = 0
            skipped_all_success = 0
            
            for announcement in pending_announcements:
                if not announcement.file_path or not os.path.exists(announcement.file_path):
                    re_download_count += 1
                    logger.warning(f"文件不存在,重新下载: {announcement.file_path or '无路径'}")
                    db.update_download_status(
                        announcement.hashcode, 
                        DownloadStatus.PENDING,
                        file_path=None
                    )
                    db.update_process_status(announcement.hashcode, ProcessStatus.PENDING)
                    continue
                
                all_modules_finished = True
                announcement_has_recoverable = False
                for module_name in PROCESS_CONFIG["modules"]:
                    module_status = db.get_module_status(announcement.hashcode, module_name)

                    # 终态: 不再入队
                    if module_status in (ProcessStatus.SUCCESS, ProcessStatus.NO_OUTPUT, ProcessStatus.SKIPPED):
                        continue

                    # 正在处理中的任务，不重复入队
                    if module_status == ProcessStatus.PROCESSING:
                        all_modules_finished = False
                        continue

                    # 待处理/失败/无状态，恢复入队
                    all_modules_finished = False
                    announcement_has_recoverable = True
                    task = ProcessTask(
                        hashcode=announcement.hashcode,
                        file_path=announcement.file_path,
                        module_name=module_name
                    )
                    if process_queue.put(task, block=False):
                        recovered_modules += 1
                
                if announcement_has_recoverable:
                    recovered_announcements += 1
                
                if all_modules_finished:
                    skipped_all_success += 1
                    db.update_process_status(announcement.hashcode, ProcessStatus.SUCCESS)
            
            if recovered_announcements > 0:
                logger.info(f"恢复了 {recovered_announcements} 个公告的 {recovered_modules} 个待处理模块任务")
            if re_download_count > 0:
                logger.info(f"重新下载 {re_download_count} 个文件丢失的任务")
            if skipped_all_success > 0:
                logger.info(f"更新 {skipped_all_success} 个所有模块已完成的公告状态")
            
        except Exception as e:
            logger.error(f"恢复待处理任务时出错: {e}")
    
    def _check_and_update_announcement_status(self, hashcode: str):
        """检查公告的所有模块处理状态，如果全部完成则更新公告整体状态并输出汇总信息"""
        try:
            all_modules = PROCESS_CONFIG["modules"]
            total_modules = len(all_modules)
            
            file_name = self._file_names.get(hashcode, hashcode[:8])
            
            status = self._announcement_status.get(hashcode, {})
            completed = status.get('completed', set())
            no_output = status.get('no_output', set())
            failed = status.get('failed', set())
            skipped = status.get('skipped', set())
            finished_modules = completed | no_output | failed | skipped
            
            if len(finished_modules) >= total_modules:
                if len(failed) > 0:
                    failed_modules = "/".join(sorted(failed))
                    logger.info(f"✗ {file_name} ({len(failed)}模块失败: {failed_modules})")
                    db.update_process_status(hashcode, ProcessStatus.FAILED)
                elif len(no_output) > 0 or len(skipped) > 0:
                    no_output_modules = "/".join(sorted(no_output | skipped))
                    logger.info(
                        f"△ {file_name} ({len(completed)}模块完成, {len(no_output | skipped)}模块无输出/跳过: {no_output_modules})"
                    )
                    db.update_process_status(hashcode, ProcessStatus.SUCCESS)
                else:
                    logger.info(f"✓ {file_name} ({total_modules}模块完成)")
                    db.update_process_status(hashcode, ProcessStatus.SUCCESS)
                
                with self._lock:
                    self._announcement_status.pop(hashcode, None)
                    self._file_names.pop(hashcode, None)
                    
        except Exception as e:
            logger.error(f"检查公告状态失败 {hashcode}: {e}")
    
    def _process_task(self, task: ProcessTask):
        """处理单个任务"""
        module_name = task.module_name
        file_name = os.path.basename(task.file_path)
        process_queue = queue_manager.get_process_queue()
        
        if self._stop_event.is_set():
            process_queue.task_done(task)
            return
        
        processor = self.processors.get(module_name)
        if not processor:
            logger.error(f"未找到处理器: {module_name}")
            process_queue.task_done(task)
            return
        
        with self._lock:
            self._active_tasks += 1
            self._file_names[task.hashcode] = file_name
            if task.hashcode not in self._announcement_status:
                self._announcement_status[task.hashcode] = {
                    'completed': set(),
                    'no_output': set(),
                    'failed': set(),
                    'skipped': set()
                }
        
        try:
            logger.debug(f"开始处理任务: {module_name} - {file_name}")
            success, message, final_status = processor.execute(task)

            with self._lock:
                announcement_status = self._announcement_status[task.hashcode]
                for key in ("completed", "no_output", "failed", "skipped"):
                    announcement_status[key].discard(module_name)

                if final_status == ProcessStatus.SUCCESS:
                    announcement_status['completed'].add(module_name)
                elif final_status == ProcessStatus.NO_OUTPUT:
                    announcement_status['no_output'].add(module_name)
                elif final_status == ProcessStatus.SKIPPED:
                    announcement_status['skipped'].add(module_name)
                else:
                    announcement_status['failed'].add(module_name)
            
            logger.debug(f"{module_name} 任务处理完成: {file_name} - {message}")
            
            self._check_and_update_announcement_status(task.hashcode)
                
        except Exception as e:
            logger.error(f"{module_name} 任务处理异常 {file_name}: {e}")
            with self._lock:
                self._announcement_status[task.hashcode]['failed'].add(module_name)
        finally:
            try:
                process_queue.task_done(task)
            except Exception as done_error:
                logger.error(f"处理队列 task_done 失败 {task.hashcode}/{module_name}: {done_error}")

            with self._lock:
                self._active_tasks -= 1
    
    def _dispatch_loop(self):
        """分发主循环"""
        logger.info("任务分发器开始工作")
        
        self._recover_pending_tasks()
        
        last_recovery_time = time.time()
        last_check_time = time.time()
        recovery_interval = 300
        check_interval = 10
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            self._executor = executor
            
            while self._running and not self._stop_event.is_set():
                try:
                    process_queue = queue_manager.get_process_queue()
                    
                    task = process_queue.get(block=True, timeout=1)
                    
                    if task:
                        executor.submit(self._process_task, task)
                    
                    current_time = time.time()
                    
                    if current_time - last_recovery_time > recovery_interval:
                        self._recover_pending_tasks()
                        last_recovery_time = current_time
                    
                    if current_time - last_check_time >= check_interval:
                        with self._lock:
                            active_tasks = self._active_tasks
                        if process_queue.empty() and active_tasks == 0:
                            if self._check_files_changed():
                                self._reload_processors()
                        last_check_time = current_time
                    
                except Exception as e:
                    if not self._stop_event.is_set():
                        logger.error(f"分发循环异常: {e}")
                    time.sleep(0.1)
        
        logger.info("任务分发器停止")
    
    def start(self):
        """启动任务分发器"""
        if self._running:
            logger.warning("任务分发器已在运行")
            return
        
        self._running = True
        self._stop_event.clear()
        
        self._dispatch_thread = threading.Thread(target=self._dispatch_loop, daemon=True)
        self._dispatch_thread.start()
        
        logger.info(f"任务分发器启动,工作线程数: {self.max_workers}")
    
    def stop(self):
        """停止任务分发器"""
        if not self._running:
            return
        
        logger.info("任务分发器停止中...")
        self._running = False
        self._stop_event.set()
        
        # 等待活跃任务完成
        timeout = 60
        start_time = time.time()
        while self._active_tasks > 0 and time.time() - start_time < timeout:
            logger.info(f"等待 {self._active_tasks} 个活跃任务完成...")
            time.sleep(1)
        
        if self._dispatch_thread and self._dispatch_thread.is_alive():
            self._dispatch_thread.join(timeout=5)
        
        logger.info("任务分发器已停止")
    
    def is_running(self) -> bool:
        """检查分发器是否正在运行"""
        return self._running
    
    def get_active_count(self) -> int:
        """获取当前活跃任务数"""
        with self._lock:
            return self._active_tasks
    
    def get_processor_stats(self) -> Dict[str, int]:
        """获取处理器统计信息"""
        stats = {}
        for name, processor in self.processors.items():
            # 这里可以添加更多统计信息
            stats[name] = 0
        return stats


# 全局任务分发器实例
task_dispatcher = TaskDispatcher()
