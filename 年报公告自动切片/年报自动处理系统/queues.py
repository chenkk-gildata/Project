"""
队列管理模块 - 年报公告自动处理系统
使用线程安全的队列实现任务管理
"""
import queue
import threading
from typing import Optional, List
from dataclasses import dataclass

from models import DownloadTask, ProcessTask, Announcement
from logger import logger


class TaskQueue:
    """任务队列基类"""
    
    def __init__(self, maxsize: int = 0):
        self._queue = queue.Queue(maxsize=maxsize)
        self._lock = threading.Lock()
        self._task_set = set()  # 用于去重
        self._total_put = 0
        self._total_get = 0
    
    def put(self, task, block: bool = True, timeout: Optional[float] = None) -> bool:
        """添加任务到队列"""
        task_key = None
        try:
            with self._lock:
                # 去重检查
                task_key = self._get_task_key(task)
                if task_key in self._task_set:
                    return False
                self._task_set.add(task_key)
            
            self._queue.put(task, block=block, timeout=timeout)
            with self._lock:
                self._total_put += 1
            return True
        except queue.Full:
            if task_key is not None:
                with self._lock:
                    self._task_set.discard(task_key)
            logger.warning("队列已满,无法添加任务")
            return False
        except Exception:
            if task_key is not None:
                with self._lock:
                    self._task_set.discard(task_key)
            raise
    
    def get(self, block: bool = True, timeout: Optional[float] = None):
        """从队列获取任务"""
        try:
            task = self._queue.get(block=block, timeout=timeout)
            with self._lock:
                self._total_get += 1
                task_key = self._get_task_key(task)
                self._task_set.discard(task_key)
            return task
        except queue.Empty:
            return None
    
    def task_done(self, task=None):
        """标记任务完成"""
        if task is not None:
            with self._lock:
                task_key = self._get_task_key(task)
                self._task_set.discard(task_key)
        self._queue.task_done()
    
    def qsize(self) -> int:
        """获取当前队列大小"""
        return self._queue.qsize()
    
    def empty(self) -> bool:
        """检查队列是否为空"""
        return self._queue.empty()
    
    def get_stats(self) -> dict:
        """获取队列统计信息"""
        with self._lock:
            return {
                "current_size": self.qsize(),
                "total_put": self._total_put,
                "total_get": self._total_get,
                "pending": self._total_put - self._total_get
            }
    
    def _get_task_key(self, task):
        """获取任务唯一标识(用于去重)"""
        raise NotImplementedError


class DownloadQueue(TaskQueue):
    """下载任务队列"""
    
    def __init__(self, maxsize: int = 1000):
        super().__init__(maxsize=maxsize)
        self._hashcode_set = set()
    
    def _get_task_key(self, task: DownloadTask) -> str:
        """下载任务使用hashcode作为唯一标识"""
        return task.announcement.hashcode
    
    def put(self, task: DownloadTask, block: bool = True, timeout: Optional[float] = None) -> bool:
        """添加下载任务"""
        with self._lock:
            if task.announcement.hashcode in self._hashcode_set:
                logger.debug(f"下载任务已存在,跳过: {task.announcement.hashcode}")
                return False
            self._hashcode_set.add(task.announcement.hashcode)
        
        result = super().put(task, block, timeout)
        if result:
            logger.debug(f"添加下载任务: {task.announcement.hashcode}")
        else:
            with self._lock:
                self._hashcode_set.discard(task.announcement.hashcode)
        return result
    
    def get(self, block: bool = True, timeout: Optional[float] = None) -> Optional[DownloadTask]:
        """获取下载任务"""
        task = super().get(block, timeout)
        if task:
            with self._lock:
                self._hashcode_set.discard(task.announcement.hashcode)
        return task
    
    def batch_put(self, announcements: List[Announcement]) -> int:
        """批量添加下载任务"""
        count = 0
        for announcement in announcements:
            task = DownloadTask(announcement=announcement)
            if self.put(task, block=False):
                count += 1
        return count


class ProcessQueue(TaskQueue):
    """处理任务队列"""
    
    def __init__(self, maxsize: int = 1000):
        super().__init__(maxsize=maxsize)
        self._task_keys = set()
        self._active_task_keys = set()
    
    def _get_task_key(self, task: ProcessTask) -> str:
        """处理任务使用hashcode+模块名作为唯一标识"""
        return f"{task.hashcode}_{task.module_name}"
    
    def put(self, task: ProcessTask, block: bool = True, timeout: Optional[float] = None) -> bool:
        """添加处理任务"""
        task_key = self._get_task_key(task)
        with self._lock:
            if task_key in self._task_keys or task_key in self._active_task_keys:
                logger.debug(f"处理任务已存在,跳过: {task_key}")
                return False
            self._task_keys.add(task_key)
        
        result = super().put(task, block, timeout)
        if result:
            logger.debug(f"添加处理任务: {task.hashcode}/{task.module_name}")
        else:
            with self._lock:
                self._task_keys.discard(task_key)
        return result
    
    def get(self, block: bool = True, timeout: Optional[float] = None) -> Optional[ProcessTask]:
        """获取处理任务"""
        task = super().get(block, timeout)
        if task:
            with self._lock:
                task_key = self._get_task_key(task)
                self._task_keys.discard(task_key)
                self._active_task_keys.add(task_key)
        return task

    def task_done(self, task: Optional[ProcessTask] = None):
        """标记处理任务完成，并从活跃任务集合中移除"""
        if task:
            with self._lock:
                task_key = self._get_task_key(task)
                self._active_task_keys.discard(task_key)
        super().task_done(task)
    
    def batch_put(self, announcement: Announcement, modules: List[str]) -> int:
        """为单个公告批量添加多个模块的处理任务"""
        if not announcement.file_path:
            logger.warning(f"公告没有文件路径,无法添加处理任务: {announcement.hashcode}")
            return 0
        
        count = 0
        for module_name in modules:
            task = ProcessTask(
                hashcode=announcement.hashcode,
                file_path=announcement.file_path,
                module_name=module_name
            )
            if self.put(task, block=False):
                count += 1
        
        if count > 0:
            logger.debug(f"为 {announcement.hashcode} 添加 {count} 个处理任务")
        return count


class QueueManager:
    """队列管理器 - 统一管理所有队列"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.download_queue = DownloadQueue(maxsize=5000)
        self.process_queue = ProcessQueue(maxsize=10000)
        self._initialized = True
        
        logger.info("队列管理器初始化完成")
    
    def get_download_queue(self) -> DownloadQueue:
        """获取下载队列"""
        return self.download_queue
    
    def get_process_queue(self) -> ProcessQueue:
        """获取处理队列"""
        return self.process_queue
    
    def get_all_stats(self) -> dict:
        """获取所有队列的统计信息"""
        return {
            "download_queue": self.download_queue.get_stats(),
            "process_queue": self.process_queue.get_stats()
        }
    
    def clear_all(self):
        """清空所有队列"""
        # 清空下载队列
        while not self.download_queue.empty():
            try:
                task = self.download_queue.get(block=False)
                if task:
                    self.download_queue.task_done(task)
            except:
                break
        
        # 清空处理队列
        while not self.process_queue.empty():
            try:
                task = self.process_queue.get(block=False)
                if task:
                    self.process_queue.task_done(task)
            except:
                break
        
        logger.info("所有队列已清空")


# 全局队列管理器实例
queue_manager = QueueManager()
