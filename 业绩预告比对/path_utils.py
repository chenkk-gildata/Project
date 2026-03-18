"""
路径工具模块 - 解决打包后的路径问题

在PyInstaller打包后，__file__会指向临时解压目录，
需要特殊处理来获取正确的程序运行目录
"""
import os
import sys


def get_program_dir():
    """
    获取程序运行目录
    
    在开发环境中：返回脚本所在目录
    在打包环境中：返回可执行文件所在目录
    
    Returns:
        str: 程序运行目录的绝对路径
    """
    if getattr(sys, 'frozen', False):
        # 打包后的可执行文件环境
        if hasattr(sys, '_MEIPASS'):
            # PyInstaller临时解压目录（用于读取资源文件）
            return sys._MEIPASS
        else:
            # 可执行文件所在目录（用于输出文件）
            return os.path.dirname(sys.executable)
    else:
        # 开发环境
        return os.path.dirname(os.path.abspath(__file__))


def get_resource_dir():
    """
    获取资源文件目录
    
    Returns:
        str: 资源文件所在目录
    """
    if getattr(sys, 'frozen', False):
        # 打包后，资源文件在临时解压目录
        if hasattr(sys, '_MEIPASS'):
            return sys._MEIPASS
        else:
            return os.path.dirname(sys.executable)
    else:
        # 开发环境
        return os.path.dirname(os.path.abspath(__file__))


def get_output_dir():
    """
    获取输出文件目录（logs、report、files等）
    
    在开发环境和打包后，都应该使用程序运行目录
    
    Returns:
        str: 输出文件目录
    """
    if getattr(sys, 'frozen', False):
        # 打包后，使用可执行文件所在目录
        return os.path.dirname(sys.executable)
    else:
        # 开发环境，使用脚本所在目录
        return os.path.dirname(os.path.abspath(__file__))


def get_resource_path(relative_path):
    """
    获取资源文件的完整路径
    
    Args:
        relative_path: 相对于资源目录的相对路径
        
    Returns:
        str: 资源文件的完整路径
    """
    return os.path.join(get_resource_dir(), relative_path)


def get_output_path(relative_path):
    """
    获取输出文件的完整路径
    
    Args:
        relative_path: 相对于输出目录的相对路径
        
    Returns:
        str: 输出文件的完整路径
    """
    return os.path.join(get_output_dir(), relative_path)


def ensure_dir(path):
    """
    确保目录存在，不存在则创建
    
    Args:
        path: 目录路径
    """
    if not os.path.exists(path):
        os.makedirs(path)


def get_log_dir():
    """
    获取日志目录
    
    Returns:
        str: 日志目录路径
    """
    output_dir = get_output_dir()
    log_dir = os.path.join(output_dir, "业绩预告小程序比对", "logs")
    ensure_dir(log_dir)
    return log_dir


def get_report_dir():
    """
    获取报告目录
    
    Returns:
        str: 报告目录路径
    """
    output_dir = get_output_dir()
    report_dir = os.path.join(output_dir, "业绩预告小程序比对", "report")
    ensure_dir(report_dir)
    return report_dir


def get_files_dir(session_id=None):
    """
    获取文件目录
    
    Args:
        session_id: 会话ID，如果提供则返回session_id子目录
        
    Returns:
        str: 文件目录路径
    """
    output_dir = get_output_dir()
    files_dir = os.path.join(output_dir, "业绩预告小程序比对", "files")
    if session_id:
        files_dir = os.path.join(files_dir, session_id)
    ensure_dir(files_dir)
    return files_dir
