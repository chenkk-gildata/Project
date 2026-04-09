"""
路径工具模块 - 处理打包前后的路径问题
打包后，程序使用当前工作目录作为基础目录
"""
import os
import sys


def get_base_dir():
    """
    获取基础目录路径
    
    打包后（frozen）：使用当前工作目录
    开发环境：使用脚本所在目录
    """
    if getattr(sys, 'frozen', False):
        return os.getcwd()
    else:
        return os.path.dirname(os.path.abspath(__file__))


def get_prompt_path(filename: str = "主要指标季度报告.md") -> str:
    """
    获取提示词文件路径
    
    打包后从当前目录读取，方便用户修改优化
    """
    base_dir = get_base_dir()
    return os.path.join(base_dir, filename)


def get_files_dir() -> str:
    """获取files目录路径"""
    base_dir = get_base_dir()
    files_dir = os.path.join(base_dir, "files")
    if not os.path.exists(files_dir):
        os.makedirs(files_dir)
    return files_dir


def get_logs_dir() -> str:
    """获取logs目录路径"""
    base_dir = get_base_dir()
    logs_dir = os.path.join(base_dir, "logs")
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    return logs_dir


def get_reports_dir() -> str:
    """获取reports目录路径"""
    base_dir = get_base_dir()
    reports_dir = os.path.join(base_dir, "reports")
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)
    return reports_dir
