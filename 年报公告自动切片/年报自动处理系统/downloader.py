"""
下载器模块 - 年报公告自动处理系统
实现并发下载和动态线程池管理
"""
import os
import re
import time
import requests
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
from datetime import datetime

from config import DOWNLOAD_CONFIG, get_raw_dir, MODULE_NAMES
from models import DownloadTask, DownloadStatus, Announcement, ProcessStatus
from database import db
from queues import queue_manager
from logger import logger


class Downloader:
    """文件下载器"""
    
    def __init__(self):
        self.url_template = DOWNLOAD_CONFIG["url_template"]
        self.max_workers = DOWNLOAD_CONFIG["max_workers"]
        self.retry_times = DOWNLOAD_CONFIG["retry_times"]
        self.timeout = DOWNLOAD_CONFIG["timeout"]
        self.chunk_size = DOWNLOAD_CONFIG["chunk_size"]
        
        self.executor: Optional[ThreadPoolExecutor] = None
        self._running = False
        self._lock = threading.Lock()
        self._active_downloads = 0
        self._stop_event = threading.Event()
    
    def _get_raw_dir(self) -> str:
        """获取当前raw目录（支持自定义输出目录）"""
        custom_dir = db.get_system_status("custom_output_dir")
        return get_raw_dir(custom_dir if custom_dir else None)
    
    def _get_filename(self, announcement: Announcement) -> str:
        """生成文件名"""
        # 使用股票代码+发布日期+标题作为文件名
        gpdm = announcement.gpdm or "UNKNOWN"
        date = announcement.publish_date or datetime.now().strftime("%Y%m%d")
        title = announcement.title or "未知标题"
        
        # 清理标题中的非法字符
        title = re.sub(r'[\\/*?:"<>|]', '-', title)
        title = re.sub(r'-+', '-', title)
        title = title.strip('-')
        
        # 限制标题长度
        title = title[:50] if len(title) > 50 else title
        
        if not title:
            title = "未知标题"
        
        return f"{gpdm}-{date}-{title}.pdf"
    
    def _download_file(self, task: DownloadTask) -> bool:
        """
        下载单个文件
        
        Returns:
            bool: 下载是否成功
        """
        announcement = task.announcement
        hashcode = announcement.hashcode
        
        # 检查是否已停止
        if self._stop_event.is_set():
            logger.info(f"下载器已停止,跳过任务: {hashcode}")
            return False
        
        # 更新状态为下载中
        db.update_download_status(hashcode, DownloadStatus.DOWNLOADING)
        
        filename = self._get_filename(announcement)
        raw_dir = self._get_raw_dir()
        file_path = os.path.join(raw_dir, filename)
        
        try:
            os.makedirs(raw_dir, exist_ok=True)
        except Exception as e:
            error_msg = f"创建下载目录失败: {str(e)}"
            logger.error(f"下载异常 {hashcode}: {error_msg}")
            db.update_download_status(hashcode, DownloadStatus.FAILED, error=error_msg)
            return False
        
        # 如果文件已存在,先删除
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                logger.warning(f"删除已存在文件失败 {file_path}: {e}")
        
        url = self.url_template.format(hashcode=hashcode)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        proxies = {
            'http': None,
            'https': None
        }
        
        try:
            with self._lock:
                self._active_downloads += 1
            
            logger.debug(f"开始下载: {hashcode} -> {filename}")
            
            response = requests.get(
                url, 
                headers=headers, 
                timeout=self.timeout, 
                stream=True,
                proxies=proxies
            )
            response.raise_for_status()
            
            # 流式写入文件
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    if self._stop_event.is_set():
                        # 停止信号,中断下载
                        f.close()
                        os.remove(file_path)
                        logger.info(f"下载中断(停止信号): {hashcode}")
                        return False
                    if chunk:
                        f.write(chunk)
            
            # 更新状态为成功
            db.update_download_status(
                hashcode, 
                DownloadStatus.SUCCESS,
                file_path=file_path
            )
            
            # 更新announcement对象
            announcement.file_path = file_path
            announcement.download_status = DownloadStatus.SUCCESS
            announcement.download_time = datetime.now()
            
            # 为每个模块创建pending状态的记录
            for module_name in MODULE_NAMES:
                db.update_module_status(hashcode, module_name, ProcessStatus.PENDING)
            
            logger.info(f"下载成功: {filename}")
            return True
            
        except requests.exceptions.RequestException as e:
            error_msg = f"下载请求失败: {str(e)}"
            logger.error(f"下载失败 {hashcode}: {error_msg}")
            
            # 更新失败状态
            retry_count = announcement.download_retry_count + 1
            announcement.download_retry_count = retry_count
            
            if retry_count < self.retry_times:
                status = DownloadStatus.RETRYING
                logger.info(f"下载任务将重试 ({retry_count}/{self.retry_times}): {hashcode}")
            else:
                status = DownloadStatus.FAILED
                logger.error(f"下载任务达到最大重试次数: {hashcode}")
            
            db.update_download_status(
                hashcode,
                status,
                error=error_msg,
                retry_count=retry_count
            )
            return False
            
        except Exception as e:
            error_msg = f"下载异常: {str(e)}"
            logger.error(f"下载异常 {hashcode}: {error_msg}")
            
            retry_count = announcement.download_retry_count + 1
            announcement.download_retry_count = retry_count
            
            if retry_count < self.retry_times:
                status = DownloadStatus.RETRYING
            else:
                status = DownloadStatus.FAILED
            
            db.update_download_status(
                hashcode,
                status,
                error=error_msg,
                retry_count=retry_count
            )
            return False
            
        finally:
            with self._lock:
                self._active_downloads -= 1
    
    def _calculate_workers(self, queue_size: int) -> int:
        """
        根据队列大小动态计算线程数
        
        策略:
        - 队列 < 10: 1个线程
        - 队列 10-50: 2个线程
        - 队列 50-100: 3个线程
        - 队列 > 100: 5个线程(最大)
        """
        if queue_size < 10:
            return 1
        elif queue_size < 50:
            return 2
        elif queue_size < 100:
            return 3
        else:
            return self.max_workers
    
    def _recover_pending_downloads(self):
        """恢复待下载的任务"""
        try:
            pending_announcements = db.get_pending_downloads(limit=500)
            
            if not pending_announcements:
                return
            
            download_queue = queue_manager.get_download_queue()
            recovered_count = 0
            
            for announcement in pending_announcements:
                task = DownloadTask(announcement=announcement)
                if download_queue.put(task, block=False):
                    recovered_count += 1
            
            if recovered_count > 0:
                logger.info(f"恢复了 {recovered_count} 个待下载任务")
            
        except Exception as e:
            logger.error(f"恢复待下载任务时出错: {e}")
    
    def start(self):
        """启动下载器"""
        if self._running:
            logger.warning("下载器已在运行")
            return
        
        self._running = True
        self._stop_event.clear()
        
        logger.info("下载器启动")
        
        worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        worker_thread.start()
    
    def _worker_loop(self):
        """工作主循环"""
        self._recover_pending_downloads()
        
        last_recovery_time = time.time()
        recovery_interval = 300
        
        while self._running and not self._stop_event.is_set():
            try:
                download_queue = queue_manager.get_download_queue()
                queue_size = download_queue.qsize()
                
                if queue_size == 0:
                    # 队列为空,等待一段时间
                    time.sleep(1)
                    continue
                
                # 动态计算线程数
                workers = self._calculate_workers(queue_size)
                
                # 创建线程池执行任务
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    future_to_task = {}
                    
                    # 提交任务
                    for _ in range(min(workers, queue_size)):
                        task = download_queue.get(block=False)
                        if task:
                            future = executor.submit(self._download_file, task)
                            future_to_task[future] = task
                    
                    # 等待任务完成
                    for future in as_completed(future_to_task):
                        task = future_to_task[future]
                        try:
                            success = future.result()
                            if success:
                                # 下载成功,将任务添加到处理队列
                                from config import PROCESS_CONFIG
                                
                                process_queue = queue_manager.get_process_queue()
                                process_queue.batch_put(
                                    task.announcement, 
                                    PROCESS_CONFIG["modules"]
                                )
                            elif task.announcement.download_retry_count < self.retry_times:
                                # 可重试失败任务立即回队列，避免等待恢复周期造成下载停滞
                                download_queue.put(task, block=False)
                        except Exception as e:
                            logger.error(f"下载任务执行异常: {type(e).__name__}: {e}")
                            if task.announcement.download_retry_count < self.retry_times:
                                download_queue.put(task, block=False)
                        finally:
                            download_queue.task_done(task)
                
                current_time = time.time()
                if current_time - last_recovery_time > recovery_interval:
                    self._recover_pending_downloads()
                    last_recovery_time = current_time
                
            except Exception as e:
                logger.error(f"下载器工作循环异常: {e}")
                time.sleep(1)
        
        logger.debug("下载器工作循环结束")
    
    def stop(self):
        """停止下载器"""
        if not self._running:
            return
        
        logger.info("下载器停止中...")
        self._running = False
        self._stop_event.set()
        
        # 等待活跃下载完成
        timeout = 30
        start_time = time.time()
        while self._active_downloads > 0 and time.time() - start_time < timeout:
            logger.info(f"等待 {self._active_downloads} 个活跃下载完成...")
            time.sleep(1)
        
        logger.info("下载器已停止")
    
    def is_running(self) -> bool:
        """检查下载器是否正在运行"""
        return self._running
    
    def get_active_count(self) -> int:
        """获取当前活跃下载数"""
        with self._lock:
            return self._active_downloads


# 全局下载器实例
downloader = Downloader()
