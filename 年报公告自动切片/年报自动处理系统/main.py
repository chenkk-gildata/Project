"""
年报公告自动处理系统 - 主程序入口

功能:
1. 每3分钟自动查询数据库,发现新公告
2. 并发下载公告文件(动态线程池,最高5线程)
3. 自动分发给5个业务处理器进行切片处理
4. 记录处理状态,避免重复处理
5. 支持优雅停止
6. 集成数据库管理工具

停止方式:
- 按 Ctrl+C
- 或在程序目录下创建 .stop 文件
"""
import os
import sys
import time
import signal
import threading
import atexit
import sqlite3
from datetime import datetime

from config import STOP_SIGNAL_FILE, LOCK_FILE, DB_PATH, REPORTS_DIR, MODULE_NAMES, CUSTOM_OUTPUT_DIR_KEY, get_all_module_dirs
from logger import logger
from database import db
from queues import queue_manager
from monitor import monitor
from downloader import downloader
from task_dispatcher import task_dispatcher


def acquire_lock() -> bool:
    """
    获取进程锁,防止重复启动
    返回True表示获取锁成功,False表示已有实例在运行
    """
    try:
        # 检查锁文件是否存在
        if os.path.exists(LOCK_FILE):
            # 读取锁文件中的PID和时间戳
            try:
                with open(LOCK_FILE, 'r') as f:
                    content = f.read().strip()
                    pid = int(content)
                
                # 使用Windows API检查进程是否存在
                import ctypes
                kernel32 = ctypes.windll.kernel32
                handle = kernel32.OpenProcess(1, False, pid)
                if handle != 0:
                    kernel32.CloseHandle(handle)
                    return False
                else:
                    # 进程已不存在,删除旧锁文件
                    os.remove(LOCK_FILE)
            except (ValueError, OSError):
                # 无法读取PID或进程不存在,删除锁文件
                try:
                    os.remove(LOCK_FILE)
                except:
                    pass
        
        # 创建锁文件
        with open(LOCK_FILE, 'w') as f:
            f.write(str(os.getpid()))
        
        return True
    except Exception as e:
        logger.error(f"获取进程锁时出错: {e}")
        return False


def release_lock():
    """释放进程锁"""
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
            logger.info("进程锁已释放")
    except Exception as e:
        logger.error(f"释放进程锁时出错: {e}")


# 注册程序退出时释放锁
atexit.register(release_lock)


class SettingsMenu:
    """设置菜单"""
    
    @staticmethod
    def show_main_menu():
        while True:
            print("\n" + "=" * 60)
            print("设置菜单")
            print("=" * 60)
            print("  1. 重置查询时间")
            print("  2. 清理数据")
            print("  3. 修改业务模块输出目录")
            print("  0. 返回主菜单 (运行程序)")
            print("-" * 60)
            
            choice = input("请选择操作 (0-3): ").strip()
            
            if choice == "1":
                SettingsMenu.reset_query_time()
            elif choice == "2":
                SettingsMenu.clean_data()
            elif choice == "3":
                SettingsMenu.set_output_dir()
            elif choice == "0":
                break
            else:
                print("\n[错误] 无效选项，请重新选择")
    
    @staticmethod
    def _get_connection():
        if not os.path.exists(DB_PATH):
            print(f"数据库文件不存在: {DB_PATH}")
            return None
        return sqlite3.connect(DB_PATH)
    
    @staticmethod
    def reset_query_time():
        conn = SettingsMenu._get_connection()
        if not conn:
            return
        
        cursor = conn.cursor()
        
        print("\n" + "-" * 40)
        print("重置查询时间选项:")
        print("-" * 40)
        print("  1. 删除查询时间 (下次启动查询最近7天)")
        print("  2. 设置为指定时间")
        print("  3. 设置为当前时间")
        print("  0. 返回上级菜单")
        
        choice = input("\n请选择 (0-3): ").strip()
        
        if choice == "1":
            cursor.execute("DELETE FROM system_status WHERE key = 'last_query_time'")
            conn.commit()
            print("\n[成功] 已删除查询时间记录")
            print("下次启动程序将查询最近7天的数据")
            
        elif choice == "2":
            date_str = input("请输入日期时间 (格式: YYYY-MM-DD HH:MM:SS): ").strip()
            try:
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                iso_time = dt.isoformat()
                cursor.execute("""
                    INSERT INTO system_status (key, value, updated_at)
                    VALUES ('last_query_time', ?, ?)
                    ON CONFLICT(key) DO UPDATE SET 
                        value = excluded.value, 
                        updated_at = excluded.updated_at
                """, (iso_time, datetime.now().isoformat()))
                conn.commit()
                print(f"\n[成功] 已设置查询时间为: {iso_time}")
            except ValueError:
                print("\n[错误] 日期格式错误，请使用格式: YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS")
                
        elif choice == "3":
            iso_time = datetime.now().isoformat()
            cursor.execute("""
                INSERT INTO system_status (key, value, updated_at)
                VALUES ('last_query_time', ?, ?)
                ON CONFLICT(key) DO UPDATE SET 
                    value = excluded.value, 
                    updated_at = excluded.updated_at
            """, (iso_time, datetime.now().isoformat()))
            conn.commit()
            print(f"\n[成功] 已设置查询时间为当前时间: {iso_time}")
            
        elif choice == "0":
            pass
        else:
            print("\n[错误] 无效选项")
        
        conn.close()
    
    @staticmethod
    def clean_data():
        conn = SettingsMenu._get_connection()
        if not conn:
            return
        
        cursor = conn.cursor()
        
        print("\n" + "-" * 40)
        print("数据清理选项:")
        print("-" * 40)
        print("  1. 清空所有公告记录")
        print("  2. 清空模块处理记录")
        print("  3. 清空系统状态")
        print("  4. 清空所有数据 (重置数据库)")
        print("  0. 返回上级菜单")
        
        choice = input("\n请选择 (0-4): ").strip()
        
        if choice == "1":
            confirm = input("确认清空所有公告记录? (y/n): ").strip().lower()
            if confirm == "y":
                cursor.execute("DELETE FROM announcements")
                conn.commit()
                print("\n[成功] 已清空公告记录")
            else:
                print("\n[取消] 操作已取消")
            
        elif choice == "2":
            confirm = input("确认清空模块处理记录? (y/n): ").strip().lower()
            if confirm == "y":
                cursor.execute("DELETE FROM module_records")
                conn.commit()
                print("\n[成功] 已清空模块处理记录")
            else:
                print("\n[取消] 操作已取消")
            
        elif choice == "3":
            confirm = input("确认清空系统状态? (y/n): ").strip().lower()
            if confirm == "y":
                cursor.execute("DELETE FROM system_status")
                conn.commit()
                print("\n[成功] 已清空系统状态")
            else:
                print("\n[取消] 操作已取消")
            
        elif choice == "4":
            confirm = input("确认清空所有数据? 此操作不可恢复! (y/n): ").strip().lower()
            if confirm == "y":
                cursor.execute("DELETE FROM announcements")
                cursor.execute("DELETE FROM module_records")
                cursor.execute("DELETE FROM system_status")
                conn.commit()
                print("\n[成功] 已清空所有数据")
            else:
                print("\n[取消] 操作已取消")
            
        elif choice == "0":
            pass
        else:
            print("\n[错误] 无效选项")
        
        conn.close()
    
    @staticmethod
    def set_output_dir():
        """设置业务模块输出目录"""
        conn = SettingsMenu._get_connection()
        if not conn:
            return
        
        cursor = conn.cursor()
        
        current_dir = db.get_system_status(CUSTOM_OUTPUT_DIR_KEY)
        default_dir = REPORTS_DIR
        is_custom = current_dir and current_dir.strip()
        
        print("\n" + "-" * 60)
        print("业务模块输出目录设置")
        print("-" * 60)
        if is_custom:
            print(f"  当前自定义目录: {current_dir}")
        else:
            print(f"  当前使用默认目录: {default_dir}")
        print("-" * 60)
        print("  1. 设置新的输出目录")
        print("  2. 恢复默认目录")
        print("  0. 返回上级菜单")
        
        choice = input("\n请选择 (0-2): ").strip()
        
        if choice == "1":
            new_dir = input("请输入新的输出目录路径 (留空取消): ").strip()
            if not new_dir:
                print("\n[取消] 操作已取消")
            else:
                new_dir = os.path.abspath(new_dir)
                if not os.path.isabs(new_dir):
                    print("\n[错误] 请输入绝对路径")
                else:
                    try:
                        os.makedirs(new_dir, exist_ok=True)
                        for module_name in MODULE_NAMES:
                            module_path = os.path.join(new_dir, module_name)
                            os.makedirs(module_path, exist_ok=True)
                        
                        cursor.execute("""
                            INSERT INTO system_status (key, value, updated_at)
                            VALUES (?, ?, ?)
                            ON CONFLICT(key) DO UPDATE SET 
                                value = excluded.value, 
                                updated_at = excluded.updated_at
                        """, (CUSTOM_OUTPUT_DIR_KEY, new_dir, datetime.now().isoformat()))
                        conn.commit()
                        print(f"\n[成功] 已设置输出目录为: {new_dir}")
                        print("  将创建以下模块目录:")
                        for module_name in MODULE_NAMES:
                            print(f"    - {os.path.join(new_dir, module_name)}")
                    except Exception as e:
                        print(f"\n[错误] 创建目录失败: {e}")
                    
        elif choice == "2":
            if is_custom:
                confirm = input("确认恢复默认目录? (y/n): ").strip().lower()
                if confirm == "y":
                    cursor.execute("DELETE FROM system_status WHERE key = ?", (CUSTOM_OUTPUT_DIR_KEY,))
                    conn.commit()
                    print(f"\n[成功] 已恢复默认目录: {default_dir}")
                else:
                    print("\n[取消] 操作已取消")
            else:
                print("\n[提示] 当前已是默认目录")
            
        elif choice == "0":
            pass
        else:
            print("\n[错误] 无效选项")
        
        conn.close()


class Application:
    """应用程序主类"""
    
    def __init__(self):
        self._running = False
        self._stop_event = threading.Event()
        self._stats_thread: threading.Thread = None
        self._last_stats = None
        self._stats_unchanged_count = 0
        
        # 注册信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """信号处理函数"""
        logger.info(f"收到信号 {signum},准备停止...")
        self.stop()
    
    def _check_stop_signal(self) -> bool:
        """检查停止信号文件"""
        return os.path.exists(STOP_SIGNAL_FILE)
    
    def _create_stop_signal(self):
        """创建停止信号文件"""
        try:
            with open(STOP_SIGNAL_FILE, 'w') as f:
                f.write(f"stop at {datetime.now().isoformat()}")
        except Exception as e:
            logger.error(f"创建停止信号文件失败: {e}")
    
    def _remove_stop_signal(self):
        """移除停止信号文件"""
        try:
            if os.path.exists(STOP_SIGNAL_FILE):
                os.remove(STOP_SIGNAL_FILE)
        except Exception as e:
            logger.error(f"移除停止信号文件失败: {e}")
    
    def _print_stats(self):
        """打印统计信息"""
        while self._running and not self._stop_event.is_set():
            try:
                # 等待60秒
                for _ in range(60):
                    if not self._running or self._stop_event.is_set():
                        break
                    time.sleep(1)
                
                if not self._running or self._stop_event.is_set():
                    break
                
                # 获取统计信息
                db_stats = db.get_statistics()
                queue_stats = queue_manager.get_all_stats()
                
                # 打印统计信息
                current_stats = {
                    'total': db_stats.get('total', 0),
                    'pending_download': db_stats.get('pending_download', 0),
                    'downloading': db_stats.get('downloading', 0),
                    'download_success': db_stats.get('download_success', 0),
                    'download_failed': db_stats.get('download_failed', 0),
                    'pending_process': db_stats.get('pending_process', 0),
                    'processing': db_stats.get('processing', 0),
                    'process_success': db_stats.get('process_success', 0),
                    'process_failed': db_stats.get('process_failed', 0),
                    'download_queue': queue_stats['download_queue']['current_size'],
                    'process_queue': queue_stats['process_queue']['current_size'],
                    'active_download': downloader.get_active_count(),
                    'active_process': task_dispatcher.get_active_count(),
                }
                
                if self._last_stats == current_stats:
                    self._stats_unchanged_count += 1
                    continue
                
                self._last_stats = current_stats
                self._stats_unchanged_count = 0
                
                logger.info("=" * 60)
                logger.info("系统运行统计:")
                logger.info(f"  数据库记录总数: {db_stats.get('total', 0)}")
                logger.info(f"  下载状态 - 等待: {db_stats.get('pending_download', 0)}, "
                          f"下载中: {db_stats.get('downloading', 0)}, "
                          f"成功: {db_stats.get('download_success', 0)}, "
                          f"失败: {db_stats.get('download_failed', 0)}")
                logger.info(f"  处理状态 - 等待: {db_stats.get('pending_process', 0)}, "
                          f"处理中: {db_stats.get('processing', 0)}, "
                          f"成功: {db_stats.get('process_success', 0)}, "
                          f"失败: {db_stats.get('process_failed', 0)}")
                logger.info(f"  下载队列: {queue_stats['download_queue']['current_size']}, "
                          f"处理队列: {queue_stats['process_queue']['current_size']}")
                logger.info(f"  活跃下载: {downloader.get_active_count()}, "
                          f"活跃处理: {task_dispatcher.get_active_count()}")
                logger.info("=" * 60)
                
            except Exception as e:
                logger.error(f"打印统计信息时出错: {e}")
    
    def start(self):
        """启动应用程序"""
        logger.info("=" * 60)
        logger.info("年报公告自动处理系统启动")
        logger.info("=" * 60)
        logger.info("功能说明:")
        logger.info("  - 每3分钟自动查询数据库发现新公告")
        logger.info("  - 并发下载(动态线程池,最高5线程)")
        logger.info("  - 自动分发给5个业务处理器")
        logger.info("  - 记录处理状态,避免重复处理")
        logger.info("停止方式:")
        logger.info("  - 按 Ctrl+C")
        logger.info("=" * 60)
        
        # 移除旧的停止信号
        self._remove_stop_signal()
        
        self._running = True
        self._stop_event.clear()
        
        try:
            # 启动监控器
            monitor.start()
            
            # 启动下载器
            downloader.start()
            
            # 启动任务分发器
            task_dispatcher.start()
            
            # 启动统计线程
            self._stats_thread = threading.Thread(target=self._print_stats, daemon=True)
            self._stats_thread.start()
            
            logger.info("所有组件已启动,系统运行中...")
            
            # 主循环
            while self._running:
                # 检查停止信号文件
                if self._check_stop_signal():
                    logger.info("检测到停止信号文件")
                    self.stop()
                    break
                
                # 检查组件状态
                if not monitor.is_running() and self._running:
                    logger.warning("监控器异常停止,尝试重启...")
                    monitor.start()
                
                if not downloader.is_running() and self._running:
                    logger.warning("下载器异常停止,尝试重启...")
                    downloader.start()
                
                if not task_dispatcher.is_running() and self._running:
                    logger.warning("任务分发器异常停止,尝试重启...")
                    task_dispatcher.start()
                
                time.sleep(1)
        
        except Exception as e:
            logger.error(f"应用程序异常: {e}")
            self.stop()
    
    def stop(self):
        """停止应用程序"""
        if not self._running:
            return
        
        logger.info("=" * 60)
        logger.info("应用程序停止中...")
        logger.info("=" * 60)
        
        self._running = False
        self._stop_event.set()
        
        # 停止各个组件
        logger.info("停止监控器...")
        monitor.stop()
        
        logger.info("停止下载器...")
        downloader.stop()
        
        logger.info("停止任务分发器...")
        task_dispatcher.stop()
        
        # 等待统计线程
        if self._stats_thread and self._stats_thread.is_alive():
            self._stats_thread.join(timeout=5)
        
        # 移除停止信号文件
        self._remove_stop_signal()
        
        # 打印最终统计
        try:
            db_stats = db.get_statistics()
            logger.info("=" * 60)
            logger.info("最终统计:")
            logger.info(f"  总记录数: {db_stats.get('total', 0)}")
            logger.info(f"  下载成功: {db_stats.get('download_success', 0)}")
            logger.info(f"  下载失败: {db_stats.get('download_failed', 0)}")
            logger.info(f"  处理成功: {db_stats.get('process_success', 0)}")
            logger.info(f"  处理失败: {db_stats.get('process_failed', 0)}")
            logger.info("=" * 60)
        except Exception as e:
            logger.error(f"打印最终统计时出错: {e}")
        
        logger.info("应用程序已停止")


def show_startup_menu():
    """显示启动菜单"""
    custom_dir = db.get_system_status(CUSTOM_OUTPUT_DIR_KEY)
    output_dir = custom_dir.strip() if custom_dir and custom_dir.strip() else REPORTS_DIR
    
    while True:
        if custom_dir is None:
            print("\n" + "=" * 60)
            print("首次运行设置")
            print("=" * 60)
            print(f"默认输出目录: {REPORTS_DIR}")
            print("  处理后的文件将按模块存放在该目录下的子文件夹中")
            print("-" * 60)
            
            user_dir = input("请输入自定义输出目录 (直接回车使用默认目录): ").strip()
            
            if user_dir:
                user_dir = os.path.abspath(user_dir)
                try:
                    os.makedirs(user_dir, exist_ok=True)
                    for module_name in MODULE_NAMES:
                        module_path = os.path.join(user_dir, module_name)
                        os.makedirs(module_path, exist_ok=True)
                    
                    db.set_system_status(CUSTOM_OUTPUT_DIR_KEY, user_dir)
                    print(f"\n[成功] 已设置输出目录为: {user_dir}")
                    custom_dir = user_dir
                    output_dir = user_dir
                except Exception as e:
                    print(f"\n[警告] 创建目录失败，将使用默认目录: {e}")
                    db.set_system_status(CUSTOM_OUTPUT_DIR_KEY, "")
                    custom_dir = ""
                    output_dir = REPORTS_DIR
            else:
                db.set_system_status(CUSTOM_OUTPUT_DIR_KEY, "")
                print(f"\n[成功] 将使用默认目录: {REPORTS_DIR}")
                custom_dir = ""
                output_dir = REPORTS_DIR
            
            print("-" * 60)
        
        print("\n" + "=" * 60)
        print("年报公告自动处理系统")
        print("=" * 60)
        print(f"数据库位置: {DB_PATH}")
        print(f"输出目录: {output_dir}")
        last_query_time = db.get_last_query_time()
        if last_query_time:
            print(f"上次查询时间: {last_query_time.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print("上次查询时间: 无记录 (下次运行将查询最近7天)")
        print("-" * 60)
        print("  1. 运行程序")
        print("  2. 设置")
        print("  0. 退出")
        print("-" * 60)
        
        choice = input("请选择操作 (0-2): ").strip()
        
        if choice == "1":
            return True
        elif choice == "2":
            SettingsMenu.show_main_menu()
            custom_dir = db.get_system_status(CUSTOM_OUTPUT_DIR_KEY)
            output_dir = custom_dir if custom_dir else REPORTS_DIR
        elif choice == "0":
            return False
        else:
            print("\n[错误] 无效选项，请重新选择")


def main():
    """主函数"""
    if not acquire_lock():
        print("错误: 程序已在运行中,请勿重复启动!")
        print(f"如果确定程序未运行,请手动删除锁文件: {LOCK_FILE}")
        sys.exit(1)
    
    db.cleanup_zombie_status()
    
    while True:
        should_run = show_startup_menu()
        
        if not should_run:
            print("\n再见!")
            break
        
        app = Application()
        
        try:
            app.start()
        except KeyboardInterrupt:
            logger.info("收到键盘中断信号")
            app.stop()
        except Exception as e:
            logger.error(f"程序异常: {e}")
            app.stop()
        
        print("\n程序已停止，返回主菜单...")


if __name__ == "__main__":
    main()
