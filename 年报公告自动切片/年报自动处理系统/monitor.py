"""
数据监控模块 - 年报公告自动处理系统
负责定期查询数据库,发现新公告并加入下载队列
"""
import os
import sys
import time
import pyodbc
import threading
from datetime import datetime, timedelta
from typing import List, Optional

from config import DB_CONFIG, MONITOR_CONFIG
from models import Announcement, DownloadStatus
from database import db
from queues import queue_manager
from logger import logger


def get_sql_file_path():
    """获取SQL文件路径，兼容开发和打包环境"""
    if getattr(sys, 'frozen', False):
        return os.path.join(sys._MEIPASS, 'query.sql')
    else:
        return MONITOR_CONFIG["sql_file"]


class Monitor:
    """数据监控器"""
    
    def __init__(self):
        self.interval = MONITOR_CONFIG["interval"]
        self._running = False
        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None
        self._zero_count_consecutive = 0
        
        self._sql_template = self._load_sql_template()
    
    def _load_sql_template(self) -> str:
        """加载SQL模板"""
        try:
            sql_file = get_sql_file_path()
            with open(sql_file, 'r', encoding='gbk') as f:
                return f.read()
        except Exception as e:
            try:
                sql_file = get_sql_file_path()
                with open(sql_file, 'r', encoding='utf-8') as f:
                    return f.read()
            except Exception as e:
                try:
                    sql_file = get_sql_file_path()
                    with open(sql_file, 'r', errors='replace') as f:
                        return f.read()
                except Exception as e:
                    logger.error(f"加载SQL模板失败: {e}")
                    return """
                        SELECT HASHCODE, B.GPDM, B.ZQJC, 
                               CONVERT(DATE, A.XXFBRQ) AS XXFBRQ,
                               A.XXBT, A.FBSJ
                        FROM [10.101.0.212].JYPRIME.dbo.usrGSGGYWFWB A
                        JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B 
                            ON A.INBBM = B.INBBM 
                            AND B.ZQSC IN (83, 90, 18) 
                            AND B.ZQLB IN (1, 2, 41)
                        WHERE A.XXLB = 20 
                          AND A.NRLB = 5 
                          AND A.XXLY IN (69, 70, 410007600)
                          AND A.MTCC IN ('上海证券交易所', '深圳证券交易所', '北京证券交易所')
                          AND A.XXFBRQ BETWEEN '2026-01-01' AND '2026-05-01'
                          AND A.XXBT NOT LIKE '%英文%' 
                          AND A.XXBT NOT LIKE '%网页已%'
                          AND A.XXBT NOT LIKE '%延期%'
                          AND A.XXBT NOT LIKE '%披露%'
                          {incremental_condition}
                        ORDER BY A.FBSJ DESC
                    """
    
    def _build_sql(self) -> str:
        """构建查询SQL(支持增量查询)"""
        last_query_time = db.get_last_query_time()
        
        if last_query_time:
            # 增量查询条件 - 查询上次查询时间之后的数据
            query_from = last_query_time
            incremental_condition = f"AND A.FBSJ >= '{query_from.strftime('%Y-%m-%d %H:%M:%S')}'"
            logger.info(f"使用增量查询: FBSJ >= {query_from}")
        else:
            # 首次查询,查询最近7天的数据
            query_from = datetime.now() - timedelta(days=7)
            incremental_condition = f"AND A.FBSJ >= '{query_from.strftime('%Y-%m-%d %H:%M:%S')}'"
            logger.info(f"首次查询,查询最近7天数据: FBSJ >= {query_from}")
        
        return self._sql_template.format(incremental_condition=incremental_condition)
    
    def _query_database(self) -> List[Announcement]:
        """查询数据库获取新公告"""
        announcements = []
        query_start_time = datetime.now()
        
        try:
            sql = self._build_sql()
            
            conn = pyodbc.connect(
                SERVER=DB_CONFIG["server"],
                UID=DB_CONFIG["username"],
                PWD=DB_CONFIG["password"],
                DRIVER=DB_CONFIG["driver"]
            )
            cursor = conn.cursor()
            cursor.execute(sql)
            
            rows = cursor.fetchall()
            for row in rows:
                try:
                    announcement = Announcement(
                        hashcode=row.HASHCODE.strip() if row.HASHCODE else "",
                        gpdm=row.GPDM.strip() if row.GPDM else None,
                        zqjc=row.ZQJC.strip() if row.ZQJC else None,
                        publish_date=row.XXFBRQ.strftime('%Y-%m-%d') if row.XXFBRQ else None,
                        title=row.XXBT.strip() if row.XXBT else None,
                        fbsj=row.FBSJ if isinstance(row.FBSJ, datetime) else datetime.now()
                    )
                    
                    # 检查是否已存在
                    existing = db.get_announcement(announcement.hashcode)
                    if existing:
                        # 已存在且下载成功,跳过
                        if existing.download_status == DownloadStatus.SUCCESS:
                            continue
                        # 已存在但下载失败,更新信息
                        announcement.download_retry_count = existing.download_retry_count
                    
                    announcements.append(announcement)
                    
                except Exception as e:
                    logger.error(f"处理查询结果时出错: {e}")
            
            conn.close()
            
            # 更新最后查询时间(使用查询开始时间,避免遗漏数据)
            db.set_last_query_time(query_start_time)
            
            if len(announcements) == 0:
                if self._zero_count_consecutive == 0:
                    logger.info(f"数据库查询完成,发现 0 个新公告")
                self._zero_count_consecutive += 1
            else:
                self._zero_count_consecutive = 0
                logger.info(f"数据库查询完成,发现 {len(announcements)} 个新公告")
            return announcements
            
        except pyodbc.Error as e:
            logger.error(f"数据库查询失败: {e}")
            return []
        except Exception as e:
            logger.error(f"查询过程异常: {e}")
            return []
    
    def _check_new_announcements(self):
        """检查新公告并加入队列"""
        try:
            # 查询数据库
            announcements = self._query_database()
            
            if not announcements:
                return
            
            # 保存到数据库
            new_count = 0
            for announcement in announcements:
                if db.save_announcement(announcement):
                    new_count += 1
            
            logger.info(f"保存 {new_count} 个新公告到数据库")
            
            # 加入下载队列
            download_queue = queue_manager.get_download_queue()
            added_count = download_queue.batch_put(announcements)
            
            logger.info(f"添加 {added_count} 个公告到下载队列")
            
        except Exception as e:
            logger.error(f"检查新公告时出错: {e}")
    
    def _monitor_loop(self):
        """监控主循环"""
        logger.info("监控器开始工作")
        
        # 首次立即执行一次
        self._check_new_announcements()
        
        while self._running and not self._stop_event.is_set():
            try:
                # 等待指定间隔
                for _ in range(self.interval):
                    if not self._running or self._stop_event.is_set():
                        break
                    time.sleep(1)
                
                if not self._running or self._stop_event.is_set():
                    break
                
                # 检查新公告
                self._check_new_announcements()
                
            except Exception as e:
                logger.error(f"监控循环异常: {e}")
                time.sleep(10)
        
        logger.info("监控器停止")
    
    def start(self):
        """启动监控器"""
        if self._running:
            logger.warning("监控器已在运行")
            return
        
        self._running = True
        self._stop_event.clear()
        
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        
        logger.info(f"监控器启动,查询间隔: {self.interval}秒")
    
    def stop(self):
        """停止监控器"""
        if not self._running:
            return
        
        logger.info("监控器停止中...")
        self._running = False
        self._stop_event.set()
        
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=5)
        
        logger.info("监控器已停止")
    
    def is_running(self) -> bool:
        """检查监控器是否正在运行"""
        return self._running
    
    def force_check(self):
        """强制立即检查一次"""
        logger.info("强制检查新公告")
        self._check_new_announcements()


# 全局监控器实例
monitor = Monitor()
