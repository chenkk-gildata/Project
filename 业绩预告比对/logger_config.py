"""
日志配置模块
"""
import logging
import os
from datetime import datetime
from config_PerformFore import logging_config
from path_utils import get_output_dir, get_log_dir

# 全局会话ID，在程序启动时生成一次
_session_id = None

# 日志配置标志，用于延迟日志文件生成
_is_logging_configured = False


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


def reset_session_id():
    """重置会话ID，生成新的会话ID"""
    global _session_id, _is_logging_configured
    _session_id = None
    _is_logging_configured = False  # 重置日志配置标志，确保下次使用时重新配置
    return get_session_id()


def setup_logging():
    """设置日志配置，延迟日志文件生成，确保使用正确的session_id"""
    global _is_logging_configured

    # 使用路径工具获取日志目录
    log_dir = get_log_dir()

    # 生成日志文件名，使用当前会话ID
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
    
    # 总是清除根日志器的处理器，避免重复
    root_logger.handlers.clear()

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # 配置特定模块的日志级别
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)

    # 标记日志已配置
    _is_logging_configured = True

    return log_file


def get_file_only_logger(name: str) -> logging.Logger:
    """获取只写入文件不输出到控制台的日志器"""
    logger = logging.getLogger(f"{name}_file_only")

    # 如果已经配置过处理器，则直接返回
    if logger.handlers:
        return logger

    # 设置日志级别
    logger.setLevel(logging.DEBUG)

    # 使用路径工具获取日志目录
    log_dir = get_log_dir()

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
