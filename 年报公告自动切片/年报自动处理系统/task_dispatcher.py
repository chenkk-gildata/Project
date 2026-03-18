"""
任务分发器 - 年报公告自动处理系统
负责从处理队列获取任务并分发给各个业务处理器
"""
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional

from config import PROCESS_CONFIG
from models import ProcessTask, ProcessStatus, DownloadStatus
from queues import queue_manager
from database import db
from logger import logger

# 导入所有处理器
from processors import (
    ZyzbProcessor, LdrjsProcessor, YftrProcessor,
    ZggcProcessor, LdrcgProcessor
)


class TaskDispatcher:
    """任务分发器"""
    
    def __init__(self):
        self.max_workers = len(PROCESS_CONFIG["modules"])
        self._running = False
        self._stop_event = threading.Event()
        self._executor: Optional[ThreadPoolExecutor] = None
        self._dispatch_thread: Optional[threading.Thread] = None
        
        self.processors: Dict[str, object] = {
            "主要指标": ZyzbProcessor(),
            "领导人介绍": LdrjsProcessor(),
            "研发投入": YftrProcessor(),
            "职工构成": ZggcProcessor(),
            "领导人持股": LdrcgProcessor()
        }
        
        self._active_tasks = 0
        self._lock = threading.Lock()
        self._announcement_status: Dict[str, dict] = {}
        self._file_names: Dict[str, str] = {}
        self._no_recovery_consecutive = 0
    
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
                
                all_modules_success = True
                announcement_has_pending = False
                for module_name in PROCESS_CONFIG["modules"]:
                    module_status = db.get_module_status(announcement.hashcode, module_name)
                    if module_status is None or module_status != ProcessStatus.SUCCESS:
                        all_modules_success = False
                        announcement_has_pending = True
                        task = ProcessTask(
                            hashcode=announcement.hashcode,
                            file_path=announcement.file_path,
                            module_name=module_name
                        )
                        if process_queue.put(task, block=False):
                            recovered_modules += 1
                
                if announcement_has_pending:
                    recovered_announcements += 1
                
                if all_modules_success:
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
            completed = status.get('completed', [])
            no_output = status.get('no_output', [])
            failed = status.get('failed', [])
            
            if len(completed) + len(no_output) + len(failed) == total_modules:
                if len(failed) > 0:
                    failed_modules = "/".join(failed)
                    logger.info(f"✗ {file_name} ({len(failed)}模块失败: {failed_modules})")
                    db.update_process_status(hashcode, ProcessStatus.FAILED)
                elif len(no_output) > 0:
                    no_output_modules = "/".join(no_output)
                    logger.info(f"△ {file_name} ({len(completed)}模块完成, {len(no_output)}模块无输出: {no_output_modules})")
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
        
        if self._stop_event.is_set():
            return
        
        processor = self.processors.get(module_name)
        if not processor:
            logger.error(f"未找到处理器: {module_name}")
            return
        
        with self._lock:
            self._active_tasks += 1
            self._file_names[task.hashcode] = file_name
            if task.hashcode not in self._announcement_status:
                self._announcement_status[task.hashcode] = {
                    'completed': [],
                    'no_output': [],
                    'failed': []
                }
        
        try:
            logger.debug(f"开始处理任务: {module_name} - {file_name}")
            success, message = processor.execute(task)
            
            with self._lock:
                if success and "处理成功" in message:
                    self._announcement_status[task.hashcode]['completed'].append(module_name)
                elif success and "无输出" in message:
                    self._announcement_status[task.hashcode]['no_output'].append(module_name)
                else:
                    self._announcement_status[task.hashcode]['failed'].append(module_name)
            
            logger.debug(f"{module_name} 任务处理完成: {file_name} - {message}")
            
            self._check_and_update_announcement_status(task.hashcode)
                
        except Exception as e:
            logger.error(f"{module_name} 任务处理异常 {file_name}: {e}")
            with self._lock:
                self._announcement_status[task.hashcode]['failed'].append(module_name)
        finally:
            with self._lock:
                self._active_tasks -= 1
    
    def _dispatch_loop(self):
        """分发主循环"""
        logger.info("任务分发器开始工作")
        
        self._recover_pending_tasks()
        
        last_recovery_time = time.time()
        recovery_interval = 300
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            self._executor = executor
            
            while self._running and not self._stop_event.is_set():
                try:
                    process_queue = queue_manager.get_process_queue()
                    
                    task = process_queue.get(block=True, timeout=1)
                    
                    if task:
                        executor.submit(self._process_task, task)
                        process_queue.task_done()
                    
                    current_time = time.time()
                    if current_time - last_recovery_time > recovery_interval:
                        self._recover_pending_tasks()
                        last_recovery_time = current_time
                    
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
