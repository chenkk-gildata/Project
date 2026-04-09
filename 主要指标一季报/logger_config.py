"""
日志配置模块
"""
import logging
import os
from datetime import datetime
from config import logging_config
from path_utils import get_logs_dir

_session_id = None


def generate_session_id():
    """生成基于当前时间的会话ID"""
    global _session_id
    if _session_id is None:
        _session_id = datetime.now().strftime('%H%M%S')
    return _session_id


def get_session_id():
    """获取当前会话ID"""
    global _session_id
    if _session_id is None:
        _session_id = generate_session_id()
    return _session_id


def setup_logging():
    """设置日志配置"""
    log_dir = get_logs_dir()

    if logging_config.file_path:
        log_file = logging_config.file_path
    else:
        session_id = get_session_id()
        log_file = os.path.join(log_dir, f"main_indicator_{session_id}.log")

    formatter = logging.Formatter(logging_config.format)

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(getattr(logging, logging_config.level.upper()))

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.WARNING)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)

    return log_file


def get_file_only_logger(name: str) -> logging.Logger:
    """获取只写入文件不输出到控制台的日志器"""
    logger = logging.getLogger(f"{name}_file_only")

    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    log_dir = get_logs_dir()

    if logging_config.file_path:
        log_file = logging_config.file_path
    else:
        session_id = get_session_id()
        log_file = os.path.join(log_dir, f"main_indicator_{session_id}.log")

    formatter = logging.Formatter(logging_config.format)

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(getattr(logging, logging_config.level.upper()))
    logger.addHandler(file_handler)

    logger.propagate = False

    return logger


def get_logger(name: str) -> logging.Logger:
    """获取指定名称的日志器"""
    return logging.getLogger(name)
