"""
数据库管理模块 - 提供连接池和安全的数据库操作
"""
import pyodbc
import threading
from contextlib import contextmanager
from typing import List, Dict, Any
import logging
from config import db_config

logger = logging.getLogger(__name__)


class DatabaseConnectionPool:
    """数据库连接池"""
    
    def __init__(self, max_connections: int = 10):
        self.max_connections = max_connections
        self._pool = []
        self._lock = threading.Lock()
        self._created_connections = 0
    
    def _create_connection(self):
        """创建新的数据库连接"""
        try:
            conn = pyodbc.connect(
                SERVER=db_config.server,
                UID=db_config.username,
                PWD=db_config.password,
                DRIVER=db_config.driver,
                timeout=db_config.connection_timeout
            )
            conn.timeout = db_config.command_timeout
            return conn
        except Exception as e:
            logger.error(f"创建数据库连接失败: {e}")
            raise
    
    def get_connection(self):
        """从连接池获取连接"""
        with self._lock:
            if self._pool:
                return self._pool.pop()
            elif self._created_connections < self.max_connections:
                self._created_connections += 1
                return self._create_connection()
            else:
                raise Exception("连接池已满，无法获取连接")
    
    def return_connection(self, conn):
        """将连接返回连接池"""
        if conn is None:
            return
            
        with self._lock:
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                
                if len(self._pool) < self.max_connections:
                    self._pool.append(conn)
                else:
                    conn.close()
                    self._created_connections -= 1
            except Exception as e:
                logger.warning(f"连接已失效，关闭连接: {e}")
                try:
                    conn.close()
                except:
                    pass
                self._created_connections -= 1
    
    def close_all(self):
        """关闭所有连接"""
        with self._lock:
            for conn in self._pool:
                try:
                    conn.close()
                except:
                    pass
            self._pool.clear()
            self._created_connections = 0


connection_pool = DatabaseConnectionPool()


class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self):
        self.pool = connection_pool
    
    @contextmanager
    def get_connection(self):
        """获取数据库连接的上下文管理器"""
        conn = None
        try:
            conn = self.pool.get_connection()
            yield conn
        except Exception as e:
            logger.error(f"数据库操作失败: {e}")
            raise
        finally:
            if conn:
                self.pool.return_connection(conn)
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """执行查询并返回结果"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                columns = [column[0] for column in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                
                for result in results:
                    for key, value in result.items():
                        if value is None:
                            result[key] = ""
                
                cursor.close()
                return results
                
        except Exception as e:
            logger.error(f"执行查询失败: {e}")
            raise
    
    def close_pool(self):
        """关闭连接池"""
        self.pool.close_all()


db_manager = DatabaseManager()
