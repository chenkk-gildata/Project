"""
日志配置模块
"""
import logging
import os
from datetime import datetime
from config import logging_config

# 全局会话ID，在需要时生成
session_id = None

def setup_logging():
    """设置日志配置"""
    # 配置日志格式
    formatter = logging.Formatter(logging_config.format)
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.WARNING)  # 控制台只显示WARNING及以上级别的日志
    
    # 配置根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    
    # 配置特定模块的日志级别
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    return None

def get_session_id():
    """获取当前会话ID，如果不存在则生成新的"""
    global session_id
    if session_id is None:
        session_id = generate_session_id()
    return session_id

def generate_session_id():
    """生成基于当前时间的会话ID"""
    global session_id
    session_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    return session_id

def setup_file_logging():
    """设置文件日志配置，在需要记录到文件时调用"""
    # 创建logs目录
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 生成日志文件名
    if logging_config.file_path:
        log_file = logging_config.file_path
    else:
        session_id = get_session_id()
        log_file = os.path.join(log_dir, f"voting_analysis_{session_id}.log")
    
    # 配置日志格式
    formatter = logging.Formatter(logging_config.format)
    
    # 文件处理器
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(getattr(logging, logging_config.level.upper()))
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.WARNING)  # 控制台只显示WARNING及以上级别的日志
    
    # 配置根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # 为httpx创建专门的文件处理器，只将httpx的INFO日志输出到文件
    httpx_logger = logging.getLogger('httpx')
    httpx_file_handler = logging.FileHandler(log_file, encoding='utf-8')
    httpx_file_handler.setFormatter(formatter)
    httpx_file_handler.setLevel(logging.INFO)
    httpx_logger.addHandler(httpx_file_handler)
    
    # 确保httpx日志器不继承根日志器的handlers，避免在控制台输出
    httpx_logger.propagate = False
    
    return log_file


def get_logger(name: str) -> logging.Logger:
    """获取指定名称的日志器"""
    return logging.getLogger(name)
