"""
日志模块 - 年报公告自动处理系统
"""
import logging
import logging.handlers
import os
import sys
import threading
from contextlib import contextmanager
from typing import Optional
from config import LOG_CONFIG

_console_handler: Optional[logging.Handler] = None
_console_handler_lock = threading.Lock()


def setup_logger(name: str = None) -> logging.Logger:
    """
    设置并返回logger实例
    
    Args:
        name: logger名称,默认为None返回root logger
        
    Returns:
        logging.Logger: 配置好的logger实例
    """
    logger = logging.getLogger(name)
    
    # 避免重复添加handler
    if logger.handlers:
        return logger
    
    # 设置日志级别
    level = getattr(logging, LOG_CONFIG["level"].upper(), logging.INFO)
    logger.setLevel(level)
    
    # 创建格式化器
    formatter = logging.Formatter(LOG_CONFIG["format"])
    
    # 控制台处理器
    global _console_handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    _console_handler = console_handler
    
    # 文件处理器(带轮转)
    log_file = LOG_CONFIG["file"]
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=LOG_CONFIG["max_bytes"],
        backupCount=LOG_CONFIG["backup_count"],
        encoding="utf-8"
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger


# 全局logger实例
logger = setup_logger("年报自动处理系统")


def set_console_log_level(level: int) -> Optional[int]:
    """Set console handler level only. Return previous level."""
    with _console_handler_lock:
        if _console_handler is None:
            return None
        old_level = _console_handler.level
        _console_handler.setLevel(level)
        return old_level


@contextmanager
def temporarily_raise_console_level(level: int = logging.CRITICAL):
    """Temporarily raise console log threshold while keeping file logs unchanged."""
    old_level = set_console_log_level(level)
    try:
        yield
    finally:
        if old_level is not None:
            set_console_log_level(old_level)
