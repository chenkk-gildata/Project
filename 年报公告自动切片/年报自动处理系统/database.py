"""
数据库模块 - 年报公告自动处理系统
使用SQLite记录公告的下载和处理状态
"""
import sqlite3
import os
from datetime import datetime
from typing import Optional, List, Dict
from contextlib import contextmanager

from config import DB_PATH
from models import Announcement, DownloadStatus, ProcessStatus
from logger import logger


class Database:
    """数据库管理类"""
    
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()
    
    @contextmanager
    def _get_connection(self):
        """获取数据库连接的上下文管理器"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def _init_db(self):
        """初始化数据库表结构"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 创建公告主表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS announcements (
                    hashcode TEXT PRIMARY KEY,
                    gpdm TEXT,
                    zqjc TEXT,
                    publish_date TEXT,
                    title TEXT,
                    fbsj TEXT,
                    download_status TEXT DEFAULT 'pending',
                    download_time TEXT,
                    download_retry_count INTEGER DEFAULT 0,
                    download_error TEXT,
                    file_path TEXT,
                    process_status TEXT DEFAULT 'pending',
                    process_time TEXT,
                    process_retry_count INTEGER DEFAULT 0,
                    process_error TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 检查并添加 fbsj 字段（如果不存在）
            try:
                cursor.execute("ALTER TABLE announcements ADD COLUMN fbsj TEXT")
            except sqlite3.OperationalError:
                # 字段已存在，忽略错误
                pass
            
            # 创建模块处理记录表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS module_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    hashcode TEXT,
                    module_name TEXT,
                    status TEXT DEFAULT 'pending',
                    retry_count INTEGER DEFAULT 0,
                    error_msg TEXT,
                    process_time TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(hashcode, module_name)
                )
            """)
            
            # 创建系统状态表(记录上次查询时间等)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS system_status (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 创建索引
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_announcements_download_status 
                ON announcements(download_status)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_announcements_process_status 
                ON announcements(process_status)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_module_records_hashcode 
                ON module_records(hashcode)
            """)
            
            conn.commit()
            logger.info("数据库初始化完成")
    
    def cleanup_zombie_status(self):
        """清理僵尸状态（程序异常退出时处于进行中的状态）"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    UPDATE announcements 
                    SET download_status = 'pending' 
                    WHERE download_status = 'downloading'
                """)
                downloading_reset = cursor.rowcount
                
                cursor.execute("""
                    UPDATE announcements 
                    SET process_status = 'pending' 
                    WHERE process_status = 'processing'
                """)
                processing_reset = cursor.rowcount
                
                cursor.execute("""
                    UPDATE module_records 
                    SET status = 'pending' 
                    WHERE status = 'processing'
                """)
                module_reset = cursor.rowcount
                
                if downloading_reset > 0 or processing_reset > 0 or module_reset > 0:
                    logger.info(f"清理僵尸状态: 下载任务 {downloading_reset} 个, 处理任务 {processing_reset} 个, 模块任务 {module_reset} 个")
                    return True
                return False
        except Exception as e:
            logger.error(f"清理僵尸状态失败: {e}")
            return False
    
    def save_announcement(self, announcement: Announcement) -> bool:
        """保存或更新公告记录"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                data = announcement.to_db_dict()
                data["updated_at"] = datetime.now().isoformat()
                
                # 检查记录是否存在
                cursor.execute(
                    "SELECT hashcode FROM announcements WHERE hashcode = ?",
                    (announcement.hashcode,)
                )
                exists = cursor.fetchone()
                
                if exists:
                    # 更新记录
                    fields = []
                    values = []
                    for key, value in data.items():
                        if key != "hashcode":
                            fields.append(f"{key} = ?")
                            values.append(value)
                    values.append(announcement.hashcode)
                    
                    sql = f"UPDATE announcements SET {', '.join(fields)} WHERE hashcode = ?"
                    cursor.execute(sql, values)
                else:
                    # 插入新记录
                    fields = list(data.keys())
                    placeholders = ["?"] * len(fields)
                    sql = f"INSERT INTO announcements ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
                    cursor.execute(sql, list(data.values()))
                
                return True
        except Exception as e:
            logger.error(f"保存公告记录失败 {announcement.hashcode}: {e}")
            return False
    
    def get_announcement(self, hashcode: str) -> Optional[Announcement]:
        """根据hashcode获取公告记录"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM announcements WHERE hashcode = ?",
                    (hashcode,)
                )
                row = cursor.fetchone()
                
                if row:
                    data = dict(row)
                    # 获取模块处理状态
                    cursor.execute(
                        "SELECT module_name, status, retry_count, error_msg FROM module_records WHERE hashcode = ?",
                        (hashcode,)
                    )
                    module_rows = cursor.fetchall()
                    
                    announcement = Announcement.from_db_dict(data)
                    for mr in module_rows:
                        announcement.module_status[mr["module_name"]] = ProcessStatus(mr["status"])
                        announcement.module_retry_count[mr["module_name"]] = mr["retry_count"]
                        announcement.module_error[mr["module_name"]] = mr["error_msg"]
                    
                    return announcement
                return None
        except Exception as e:
            logger.error(f"获取公告记录失败 {hashcode}: {e}")
            return None
    
    def get_pending_downloads(self, limit: int = 100) -> List[Announcement]:
        """获取待下载的公告列表"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM announcements 
                    WHERE download_status IN ('pending', 'failed', 'retrying')
                    AND download_retry_count < 3
                    ORDER BY fbsj DESC
                    LIMIT ?
                """, (limit,))
                
                rows = cursor.fetchall()
                announcements = []
                for row in rows:
                    data = dict(row)
                    announcements.append(Announcement.from_db_dict(data))
                return announcements
        except Exception as e:
            logger.error(f"获取待下载列表失败: {e}")
            return []
    
    def get_pending_processes(self, limit: int = 100) -> List[Announcement]:
        """获取待处理的公告列表(下载成功但未处理或处理失败)"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM announcements 
                    WHERE download_status = 'success'
                    AND (process_status IN ('pending', 'failed') 
                         OR process_status = 'retrying')
                    AND file_path IS NOT NULL
                    ORDER BY download_time DESC
                    LIMIT ?
                """, (limit,))
                
                rows = cursor.fetchall()
                announcements = []
                for row in rows:
                    data = dict(row)
                    announcements.append(Announcement.from_db_dict(data))
                return announcements
        except Exception as e:
            logger.error(f"获取待处理列表失败: {e}")
            return []
    
    def update_download_status(
        self, 
        hashcode: str, 
        status: DownloadStatus, 
        file_path: Optional[str] = None,
        error: Optional[str] = None,
        retry_count: Optional[int] = None
    ) -> bool:
        """更新下载状态"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                updates = ["download_status = ?", "updated_at = ?"]
                values = [status.value, datetime.now().isoformat()]
                
                if file_path:
                    updates.append("file_path = ?")
                    values.append(file_path)
                
                if error:
                    updates.append("download_error = ?")
                    values.append(error)
                
                if retry_count is not None:
                    updates.append("download_retry_count = ?")
                    values.append(retry_count)
                
                if status == DownloadStatus.SUCCESS:
                    updates.append("download_time = ?")
                    values.append(datetime.now().isoformat())
                
                values.append(hashcode)
                sql = f"UPDATE announcements SET {', '.join(updates)} WHERE hashcode = ?"
                cursor.execute(sql, values)
                
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"更新下载状态失败 {hashcode}: {e}")
            return False
    
    def update_process_status(
        self,
        hashcode: str,
        status: ProcessStatus,
        error: Optional[str] = None,
        retry_count: Optional[int] = None
    ) -> bool:
        """更新处理状态"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                updates = ["process_status = ?", "updated_at = ?"]
                values = [status.value, datetime.now().isoformat()]
                
                if error:
                    updates.append("process_error = ?")
                    values.append(error)
                
                if retry_count is not None:
                    updates.append("process_retry_count = ?")
                    values.append(retry_count)
                
                if status == ProcessStatus.SUCCESS:
                    updates.append("process_time = ?")
                    values.append(datetime.now().isoformat())
                
                values.append(hashcode)
                sql = f"UPDATE announcements SET {', '.join(updates)} WHERE hashcode = ?"
                cursor.execute(sql, values)
                
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"更新处理状态失败 {hashcode}: {e}")
            return False
    
    def update_module_status(
        self,
        hashcode: str,
        module_name: str,
        status: ProcessStatus,
        error: Optional[str] = None,
        retry_count: Optional[int] = None
    ) -> bool:
        """更新模块处理状态"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # 检查记录是否存在
                cursor.execute(
                    "SELECT id FROM module_records WHERE hashcode = ? AND module_name = ?",
                    (hashcode, module_name)
                )
                exists = cursor.fetchone()
                
                now = datetime.now().isoformat()
                
                if exists:
                    # 更新
                    updates = ["status = ?", "updated_at = ?"]
                    values = [status.value, now]
                    
                    if error is not None:
                        updates.append("error_msg = ?")
                        values.append(error)
                    
                    if retry_count is not None:
                        updates.append("retry_count = ?")
                        values.append(retry_count)
                    
                    if status == ProcessStatus.SUCCESS:
                        updates.append("process_time = ?")
                        values.append(now)
                    
                    values.extend([hashcode, module_name])
                    sql = f"UPDATE module_records SET {', '.join(updates)} WHERE hashcode = ? AND module_name = ?"
                    cursor.execute(sql, values)
                else:
                    # 插入
                    cursor.execute("""
                        INSERT INTO module_records 
                        (hashcode, module_name, status, error_msg, retry_count, process_time, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        hashcode, module_name, status.value, error, 
                        retry_count or 0, 
                        now if status == ProcessStatus.SUCCESS else None,
                        now
                    ))
                
                return True
        except Exception as e:
            logger.error(f"更新模块状态失败 {hashcode}/{module_name}: {e}")
            return False
    
    def get_module_status(self, hashcode: str, module_name: str) -> Optional[ProcessStatus]:
        """获取模块处理状态"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT status FROM module_records WHERE hashcode = ? AND module_name = ?",
                    (hashcode, module_name)
                )
                row = cursor.fetchone()
                
                if row:
                    return ProcessStatus(row["status"])
                return None
        except Exception as e:
            logger.error(f"获取模块状态失败 {hashcode}/{module_name}: {e}")
            return None
    
    def set_system_status(self, key: str, value: str) -> bool:
        """设置系统状态值"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO system_status (key, value, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """, (key, value, datetime.now().isoformat()))
                return True
        except Exception as e:
            logger.error(f"设置系统状态失败 {key}: {e}")
            return False
    
    def get_system_status(self, key: str, default: str = None) -> Optional[str]:
        """获取系统状态值"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT value FROM system_status WHERE key = ?",
                    (key,)
                )
                row = cursor.fetchone()
                return row["value"] if row else default
        except Exception as e:
            logger.error(f"获取系统状态失败 {key}: {e}")
            return default
    
    def get_last_query_time(self) -> Optional[datetime]:
        """获取上次查询时间"""
        value = self.get_system_status("last_query_time")
        if value:
            try:
                return datetime.fromisoformat(value)
            except:
                pass
        return None
    
    def set_last_query_time(self, dt: datetime = None) -> bool:
        """设置上次查询时间"""
        if dt is None:
            dt = datetime.now()
        return self.set_system_status("last_query_time", dt.isoformat())
    
    def get_statistics(self) -> Dict[str, int]:
        """获取统计信息"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # 下载统计
                cursor.execute("""
                    SELECT download_status, COUNT(*) as count 
                    FROM announcements 
                    GROUP BY download_status
                """)
                download_stats = {row["download_status"]: row["count"] for row in cursor.fetchall()}
                
                # 处理统计
                cursor.execute("""
                    SELECT process_status, COUNT(*) as count 
                    FROM announcements 
                    WHERE download_status = 'success'
                    GROUP BY process_status
                """)
                process_stats = {row["process_status"]: row["count"] for row in cursor.fetchall()}
                
                return {
                    "total": sum(download_stats.values()),
                    "pending_download": download_stats.get("pending", 0),
                    "downloading": download_stats.get("downloading", 0),
                    "download_success": download_stats.get("success", 0),
                    "download_failed": download_stats.get("failed", 0) + download_stats.get("retrying", 0),
                    "pending_process": process_stats.get("pending", 0),
                    "processing": process_stats.get("processing", 0),
                    "process_success": process_stats.get("success", 0),
                    "process_failed": process_stats.get("failed", 0) + process_stats.get("retrying", 0),
                }
        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
            return {}


# 全局数据库实例
db = Database()
