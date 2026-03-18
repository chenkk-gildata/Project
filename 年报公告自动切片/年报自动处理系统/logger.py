"""
日志模块 - 年报公告自动处理系统
"""
import logging
import logging.handlers
import os
import sys
from config import LOG_CONFIG


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
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
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
