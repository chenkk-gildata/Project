"""
日志配置模块
"""
import logging
import os
import sys
from datetime import datetime
from config import logging_config


def get_base_path():
    """获取程序基础路径，兼容开发和打包环境"""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

# 全局会话ID，在程序启动时生成一次
_session_id = None


def generate_session_id():
    """生成基于当前时间的会话ID"""
    global _session_id
    if _session_id is None:
        _session_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    return _session_id


def get_session_id():
    """获取当前会话ID"""
    global _session_id
    if _session_id is None:
        _session_id = generate_session_id()
    return _session_id


def setup_logging():
    """设置日志配置"""
    base_dir = get_base_path()
    log_dir = os.path.join(base_dir, "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 生成日志文件名，使用会话ID
    if logging_config.file_path:
        log_file = logging_config.file_path
    else:
        session_id = get_session_id()
        log_file = os.path.join(log_dir, f"main_indicator_{session_id}.log")

    # 配置日志格式
    formatter = logging.Formatter(logging_config.format)

    # 文件处理器
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(getattr(logging, logging_config.level.upper()))

    # 控制台处理器 - 只显示WARNING及以上级别的日志
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.WARNING)

    # 配置根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # 配置特定模块的日志级别
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)

    return log_file


def get_file_only_logger(name: str) -> logging.Logger:
    """获取只写入文件不输出到控制台的日志器"""
    logger = logging.getLogger(f"{name}_file_only")

    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    base_dir = get_base_path()
    log_dir = os.path.join(base_dir, "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 生成日志文件名，使用会话ID
    if logging_config.file_path:
        log_file = logging_config.file_path
    else:
        session_id = get_session_id()
        log_file = os.path.join(log_dir, f"main_indicator_{session_id}.log")

    # 配置日志格式
    formatter = logging.Formatter(logging_config.format)

    # 只添加文件处理器
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(getattr(logging, logging_config.level.upper()))
    logger.addHandler(file_handler)

    # 防止日志传播到根日志器
    logger.propagate = False

    return logger


def get_logger(name: str) -> logging.Logger:
    """获取指定名称的日志器"""
    return logging.getLogger(name)
