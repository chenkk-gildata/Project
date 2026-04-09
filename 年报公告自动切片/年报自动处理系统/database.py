"""
数据库模块 - 年报公告自动处理系统
优化版：串行写入 + 连接池读取，彻底解决并发卡死问题

核心优化：
1. 专用写入线程：所有写操作串行化，无锁竞争
2. 读取连接池：并发读取，无阻塞
3. 异步写入：写操作立即返回，不阻塞调用线程
4. 连接复用：避免频繁创建/关闭连接的开销
"""
import sqlite3
import os
import queue
import threading
import atexit
from datetime import datetime
from typing import Optional, List, Dict
from contextlib import contextmanager

from config import DB_PATH
from models import Announcement, DownloadStatus, ProcessStatus
from logger import logger


class Database:
    """数据库管理类 - 优化版"""
    
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        
        self._write_queue = queue.Queue(maxsize=20000)
        self._write_thread = None
        self._write_conn = None
        self._running = True
        
        self._read_pool_size = 10
        self._read_pool = queue.Queue(maxsize=self._read_pool_size)
        self._read_pool_lock = threading.Lock()
        
        self._init_db()
        self._start_writer()
        
        atexit.register(self.close)
    
    def _init_db(self):
        """初始化数据库表结构"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA temp_store=MEMORY")
        
        cursor = conn.cursor()
        
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
        
        try:
            cursor.execute("ALTER TABLE announcements ADD COLUMN fbsj TEXT")
        except sqlite3.OperationalError:
            pass
        
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
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_status (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_announcements_download_status ON announcements(download_status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_announcements_process_status ON announcements(process_status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_module_records_hashcode ON module_records(hashcode)")
        
        conn.commit()
        conn.close()
        
        for _ in range(self._read_pool_size):
            self._read_pool.put(self._create_read_conn())
        
        logger.info("数据库初始化完成")
    
    def _create_read_conn(self):
        """创建读取连接"""
        conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA read_uncommitted=ON")
        conn.execute("PRAGMA cache_size=5000")
        return conn
    
    def _start_writer(self):
        """启动专用写入线程"""
        self._write_thread = threading.Thread(
            target=self._write_loop,
            daemon=True,
            name="DB-Writer"
        )
        self._write_thread.start()
        logger.info("数据库写入线程启动")
    
    def _write_loop(self):
        """写入主循环 - 串行处理所有写操作"""
        self._write_conn = sqlite3.connect(self.db_path, timeout=30.0)
        self._write_conn.row_factory = sqlite3.Row
        self._write_conn.execute("PRAGMA journal_mode=WAL")
        self._write_conn.execute("PRAGMA synchronous=NORMAL")
        self._write_conn.execute("PRAGMA cache_size=10000")
        self._write_conn.execute("PRAGMA busy_timeout=30000")
        self._write_conn.execute("PRAGMA temp_store=MEMORY")
        
        while self._running:
            try:
                task = self._write_queue.get(timeout=1)
                if task is None:
                    continue
                
                sql, params, result_queue, is_batch = task
                
                try:
                    cursor = self._write_conn.cursor()
                    
                    if is_batch and isinstance(params, list):
                        for p in params:
                            cursor.execute(sql, p)
                    else:
                        cursor.execute(sql, params)
                    
                    self._write_conn.commit()
                    
                    if result_queue:
                        result_queue.put((True, cursor.lastrowid, cursor.rowcount))
                    
                except Exception as e:
                    try:
                        self._write_conn.rollback()
                    except:
                        pass
                    
                    if result_queue:
                        result_queue.put((False, str(e), 0))
                    logger.error(f"数据库写入失败: {e}")
                    
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"写入线程异常: {e}")
        
        if self._write_conn:
            try:
                self._write_conn.close()
            except:
                pass
        logger.info("数据库写入线程停止")
    
    @contextmanager
    def _get_read_conn(self):
        """获取读取连接（从连接池）"""
        conn = None
        try:
            conn = self._read_pool.get(timeout=5)
            yield conn
        except queue.Empty:
            conn = self._create_read_conn()
            yield conn
        finally:
            if conn:
                try:
                    self._read_pool.put(conn, timeout=1)
                except queue.Full:
                    try:
                        conn.close()
                    except:
                        pass
    
    def _execute_write(self, sql: str, params: tuple = ()) -> bool:
        """执行写入操作（异步，立即返回）"""
        if not self._running:
            return False
        try:
            self._write_queue.put((sql, params, None, False), timeout=5)
            return True
        except queue.Full:
            logger.error("写入队列已满")
            return False
    
    def _execute_write_sync(self, sql: str, params: tuple = (), timeout: float = 10.0) -> tuple:
        """执行写入操作（同步，等待结果）"""
        if not self._running:
            return False, "数据库已关闭", 0
        
        result_queue = queue.Queue()
        try:
            self._write_queue.put((sql, params, result_queue, False), timeout=5)
            success, result, rowcount = result_queue.get(timeout=timeout)
            return success, result, rowcount
        except queue.Full:
            return False, "写入队列已满", 0
        except queue.Empty:
            return False, "写入超时", 0
    
    def _execute_batch_write(self, sql: str, params_list: List[tuple]) -> bool:
        """执行批量写入操作"""
        if not self._running:
            return False
        try:
            self._write_queue.put((sql, params_list, None, True), timeout=10)
            return True
        except queue.Full:
            logger.error("写入队列已满")
            return False
    
    def close(self):
        """关闭数据库连接"""
        if not self._running:
            return
        
        self._running = False
        
        try:
            self._write_queue.put(None, timeout=1)
        except:
            pass
        
        if self._write_thread and self._write_thread.is_alive():
            self._write_thread.join(timeout=3)
        
        while not self._read_pool.empty():
            try:
                conn = self._read_pool.get_nowait()
                conn.close()
            except:
                break
        
        logger.info("数据库连接已关闭")
    
    def cleanup_zombie_status(self) -> bool:
        """清理僵尸状态（程序异常退出时处于进行中的状态）"""
        sql1 = "UPDATE announcements SET download_status = 'pending' WHERE download_status = 'downloading'"
        sql2 = "UPDATE announcements SET process_status = 'pending' WHERE process_status = 'processing'"
        sql3 = "UPDATE module_records SET status = 'pending' WHERE status = 'processing'"
        
        success1, _, count1 = self._execute_write_sync(sql1)
        success2, _, count2 = self._execute_write_sync(sql2)
        success3, _, count3 = self._execute_write_sync(sql3)
        
        total = (count1 if success1 else 0) + (count2 if success2 else 0) + (count3 if success3 else 0)
        
        if total > 0:
            logger.info(f"清理僵尸状态: 下载任务 {count1 if success1 else 0} 个, 处理任务 {count2 if success2 else 0} 个, 模块任务 {count3 if success3 else 0} 个")
            return True
        return False
    
    def fix_sub_module_consistency(self) -> int:
        """修复子模块记录一致性（主模块有记录但子模块缺失的情况）
        
        Returns:
            int: 修复的记录数
        """
        try:
            with self._get_read_conn() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT m1.hashcode, m1.status, m1.process_time, m1.module_name
                    FROM module_records m1
                    WHERE m1.module_name = '主要指标'
                    AND m1.status IN ('success', 'no_output')
                    AND m1.hashcode NOT IN (
                        SELECT m2.hashcode FROM module_records m2 
                        WHERE m2.module_name = '主要指标-补充'
                    )
                """)
                
                missing_records = cursor.fetchall()
                
                if not missing_records:
                    return 0
                
                fixed_count = 0
                now = datetime.now().isoformat()
                
                for record in missing_records:
                    hashcode = record[0]
                    main_status = record[1]
                    process_time = record[2]
                    
                    sub_status = 'success' if main_status == 'success' else 'no_output'
                    
                    sql = """
                        INSERT INTO module_records (
                            hashcode, module_name, status, process_time, 
                            created_at, updated_at
                        )
                        VALUES (?, '主要指标-补充', ?, ?, ?, ?)
                    """
                    params = (hashcode, sub_status, process_time, now, now)
                    
                    success, result, _ = self._execute_write_sync(sql, params)
                    if success:
                        fixed_count += 1
                        logger.info(f"修复子模块记录: {hashcode} -> 主要指标-补充 ({sub_status})")
                    else:
                        logger.error(f"修复子模块记录失败: {hashcode} -> {result}")
                
                if fixed_count > 0:
                    logger.info(f"子模块一致性检查: 修复了 {fixed_count} 条缺失记录")
                
                return fixed_count
                
        except Exception as e:
            logger.error(f"子模块一致性检查失败: {e}")
            return 0
    
    def save_announcement(self, announcement: Announcement) -> bool:
        """保存或更新公告记录"""
        data = announcement.to_db_dict()
        data["updated_at"] = datetime.now().isoformat()
        
        fields = list(data.keys())
        placeholders = ["?"] * len(fields)
        updates = [f"{k}=excluded.{k}" for k in fields if k != "hashcode"]
        
        sql = f"""
            INSERT INTO announcements ({', '.join(fields)}) 
            VALUES ({', '.join(placeholders)})
            ON CONFLICT(hashcode) DO UPDATE SET {', '.join(updates)}
        """
        
        return self._execute_write(sql, tuple(data.values()))
    
    def get_announcement(self, hashcode: str) -> Optional[Announcement]:
        """根据hashcode获取公告记录"""
        try:
            with self._get_read_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM announcements WHERE hashcode = ?",
                    (hashcode,)
                )
                row = cursor.fetchone()
                
                if row:
                    data = dict(row)
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
            with self._get_read_conn() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM announcements 
                    WHERE download_status IN ('pending', 'failed', 'retrying')
                    AND download_retry_count < 3
                    ORDER BY fbsj DESC
                    LIMIT ?
                """, (limit,))
                
                rows = cursor.fetchall()
                return [Announcement.from_db_dict(dict(row)) for row in rows]
        except Exception as e:
            logger.error(f"获取待下载列表失败: {e}")
            return []
    
    def get_pending_processes(self, limit: int = 100) -> List[Announcement]:
        """获取待处理的公告列表(下载成功但未处理或处理失败)"""
        try:
            with self._get_read_conn() as conn:
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
                return [Announcement.from_db_dict(dict(row)) for row in rows]
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
        
        return self._execute_write(sql, tuple(values))
    
    def update_process_status(
        self,
        hashcode: str,
        status: ProcessStatus,
        error: Optional[str] = None,
        retry_count: Optional[int] = None
    ) -> bool:
        """更新处理状态"""
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
        
        return self._execute_write(sql, tuple(values))
    
    def update_module_status(
        self,
        hashcode: str,
        module_name: str,
        status: ProcessStatus,
        error: Optional[str] = None,
        retry_count: Optional[int] = None,
        sync: bool = False
    ) -> bool:
        """更新模块处理状态
        
        Args:
            hashcode: 公告哈希值
            module_name: 模块名称
            status: 处理状态
            error: 错误信息
            retry_count: 重试次数
            sync: 是否同步写入（默认异步，子模块建议使用同步）
        
        Returns:
            bool: 写入是否成功
        """
        now = datetime.now().isoformat()
        process_time = now if status == ProcessStatus.SUCCESS else None
        insert_retry_count = retry_count if retry_count is not None else 0

        sql = """
            INSERT INTO module_records (
                hashcode, module_name, status, error_msg,
                retry_count, process_time, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(hashcode, module_name) DO UPDATE SET
                status = excluded.status,
                error_msg = CASE
                    WHEN ? IS NOT NULL THEN excluded.error_msg
                    ELSE module_records.error_msg
                END,
                retry_count = CASE
                    WHEN ? IS NOT NULL THEN excluded.retry_count
                    ELSE module_records.retry_count
                END,
                process_time = CASE
                    WHEN excluded.process_time IS NOT NULL THEN excluded.process_time
                    ELSE module_records.process_time
                END,
                updated_at = excluded.updated_at
        """
        
        params = (
            hashcode,
            module_name,
            status.value,
            error,
            insert_retry_count,
            process_time,
            now,
            now,
            error,
            retry_count
        )
        
        if sync:
            success, result, rowcount = self._execute_write_sync(sql, params)
            if not success:
                logger.error(f"同步更新模块状态失败 {hashcode}/{module_name}: {result}")
            return success
        else:
            return self._execute_write(sql, params)
    
    def get_module_status(self, hashcode: str, module_name: str) -> Optional[ProcessStatus]:
        """获取模块处理状态"""
        try:
            with self._get_read_conn() as conn:
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
        sql = """
            INSERT INTO system_status (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        """
        return self._execute_write(sql, (key, value, datetime.now().isoformat()))
    
    def get_system_status(self, key: str, default: str = None) -> Optional[str]:
        """获取系统状态值"""
        try:
            with self._get_read_conn() as conn:
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
            with self._get_read_conn() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT download_status, COUNT(*) as count 
                    FROM announcements 
                    GROUP BY download_status
                """)
                download_stats = {row["download_status"]: row["count"] for row in cursor.fetchall()}
                
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
    
    def get_all_announcements(self, limit: int = 100, offset: int = 0) -> List[Announcement]:
        """获取所有公告列表（分页）"""
        try:
            with self._get_read_conn() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM announcements 
                    ORDER BY fbsj DESC
                    LIMIT ? OFFSET ?
                """, (limit, offset))
                
                rows = cursor.fetchall()
                return [Announcement.from_db_dict(dict(row)) for row in rows]
        except Exception as e:
            logger.error(f"获取公告列表失败: {e}")
            return []
    
    def count_announcements(self) -> int:
        """获取公告总数"""
        try:
            with self._get_read_conn() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) as count FROM announcements")
                row = cursor.fetchone()
                return row["count"] if row else 0
        except Exception as e:
            logger.error(f"获取公告总数失败: {e}")
            return 0
    
    def search_announcements(self, keyword: str, limit: int = 50) -> List[Announcement]:
        """搜索公告"""
        try:
            with self._get_read_conn() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM announcements 
                    WHERE title LIKE ? OR zqjc LIKE ? OR gpdm LIKE ?
                    ORDER BY fbsj DESC
                    LIMIT ?
                """, (f"%{keyword}%", f"%{keyword}%", f"%{keyword}%", limit))
                
                rows = cursor.fetchall()
                return [Announcement.from_db_dict(dict(row)) for row in rows]
        except Exception as e:
            logger.error(f"搜索公告失败: {e}")
            return []
    
    def delete_announcement(self, hashcode: str) -> bool:
        """删除公告"""
        sql1 = "DELETE FROM module_records WHERE hashcode = ?"
        sql2 = "DELETE FROM announcements WHERE hashcode = ?"
        
        self._execute_write(sql1, (hashcode,))
        return self._execute_write(sql2, (hashcode,))
    
    def clear_all_data(self) -> bool:
        """清空所有数据"""
        sql1 = "DELETE FROM module_records"
        sql2 = "DELETE FROM announcements"
        sql3 = "DELETE FROM system_status"
        
        success = True
        success &= self._execute_write(sql1)
        success &= self._execute_write(sql2)
        success &= self._execute_write(sql3)
        
        return success
    
    def get_queue_size(self) -> int:
        """获取写入队列当前大小"""
        return self._write_queue.qsize()
    
    def is_healthy(self) -> bool:
        """检查数据库是否健康"""
        return (
            self._running and 
            self._write_thread is not None and 
            self._write_thread.is_alive()
        )


db = Database()
