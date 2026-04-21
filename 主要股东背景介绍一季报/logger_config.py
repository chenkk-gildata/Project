"""
日志配置模块。
"""
import logging
import os
from datetime import datetime

from config import logging_config
from path_utils import get_logs_dir

_session_id = None


def _build_formatter():
    """构建日志格式化器，兼容异常环境变量配置。"""
    try:
        return logging.Formatter(logging_config.format)
    except Exception:
        return logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def generate_session_id():
    """生成基于当前时间的会话 ID。"""
    global _session_id
    if _session_id is None:
        _session_id = datetime.now().strftime("%H%M%S")
    return _session_id


def get_session_id():
    """获取当前会话 ID。"""
    global _session_id
    if _session_id is None:
        _session_id = generate_session_id()
    return _session_id


def _build_log_file():
    """构建日志文件路径。"""
    if logging_config.file_path:
        return logging_config.file_path
    return os.path.join(get_logs_dir(), f"main_SHBackground_{get_session_id()}.log")


def setup_logging():
    """设置日志配置。"""
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return _build_log_file()

    formatter = _build_formatter()
    log_file = _build_log_file()

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(getattr(logging, logging_config.level.upper()))

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.WARNING)

    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    return log_file


def get_file_only_logger(name: str) -> logging.Logger:
    """获取仅写文件的 logger。"""
    logger = logging.getLogger(f"{name}_file_only")
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    formatter = _build_formatter()

    file_handler = logging.FileHandler(_build_log_file(), encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(getattr(logging, logging_config.level.upper()))

    logger.addHandler(file_handler)
    logger.propagate = False
    return logger


def get_logger(name: str) -> logging.Logger:
    """获取命名 logger。"""
    return logging.getLogger(name)
