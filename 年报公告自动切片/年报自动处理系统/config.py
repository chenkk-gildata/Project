"""
配置文件 - 年报公告自动处理系统
"""
import os
import sys


def get_base_path():
    """获取程序基础路径，兼容开发和打包环境"""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))


BASE_DIR = get_base_path()
STORAGE_DIR = os.path.join(BASE_DIR, "storage")
REPORTS_DIR = os.path.join(BASE_DIR, "年度报告")
RAW_DIR = os.path.join(REPORTS_DIR, "raw")

# 数据库配置
DB_PATH = os.path.join(STORAGE_DIR, "records.db")

# SQL Server数据库连接配置
DB_CONFIG = {
    "server": "10.102.25.11,8080",
    "username": "WebResourceNew_Read",
    "password": "New_45ted",
    "driver": "ODBC Driver 17 for SQL Server"
}

# 下载配置
DOWNLOAD_CONFIG = {
    "url_template": "http://10.6.1.131/rfApi/file/downloadWithAppId/{hashcode}?appId=rc-as",
    "max_workers": 5,  # 最大并发下载线程数
    "retry_times": 3,  # 下载失败重试次数
    "timeout": 30,     # 下载超时时间(秒)
    "chunk_size": 8192 # 下载块大小
}

# 监控配置
MONITOR_CONFIG = {
    "interval": 180,  # 监控间隔(秒), 3分钟
    "sql_file": os.path.join(BASE_DIR, "query.sql")
}

# 处理配置
PROCESS_CONFIG = {
    "retry_times": 2,  # 处理失败重试次数
    "modules": [
        "主要指标",
        "领导人介绍",
        "研发投入",
        "职工构成",
        "领导人持股",
        "股东背景介绍"
    ]
}

MODULE_NAMES = PROCESS_CONFIG["modules"]

CUSTOM_OUTPUT_DIR_KEY = "custom_output_dir"

def get_raw_dir(custom_dir: str = None) -> str:
    """
    获取原始公告存放目录
    
    Args:
        custom_dir: 自定义输出根目录，如果为None则使用默认目录
        
    Returns:
        str: raw目录路径
    """
    if custom_dir:
        return os.path.join(custom_dir, "raw")
    return os.path.join(REPORTS_DIR, "raw")

def get_module_output_dir(module_name: str, custom_dir: str = None) -> str:
    """
    获取业务模块输出目录
    
    Args:
        module_name: 模块名称
        custom_dir: 自定义输出根目录，如果为None则使用默认目录
        
    Returns:
        str: 模块输出目录路径
    """
    if custom_dir:
        return os.path.join(custom_dir, module_name)
    return os.path.join(REPORTS_DIR, module_name)

def get_all_module_dirs(custom_dir: str = None) -> dict:
    """
    获取所有业务模块的输出目录
    
    Args:
        custom_dir: 自定义输出根目录
        
    Returns:
        dict: 模块名到目录路径的映射
    """
    return {name: get_module_output_dir(name, custom_dir) for name in MODULE_NAMES}

# 日志配置
LOG_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": os.path.join(STORAGE_DIR, "app.log"),
    "max_bytes": 10 * 1024 * 1024,  # 10MB
    "backup_count": 5
}

# 停止信号文件
STOP_SIGNAL_FILE = os.path.join(BASE_DIR, ".stop")

# 进程锁文件（防止重复启动）
LOCK_FILE = os.path.join(BASE_DIR, ".lock")
