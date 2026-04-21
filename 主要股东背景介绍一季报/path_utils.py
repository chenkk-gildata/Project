"""
路径工具模块。
"""
import os
import sys


def get_base_dir():
    """获取基础目录路径。"""
    if getattr(sys, "frozen", False):
        return os.getcwd()
    return os.path.dirname(os.path.abspath(__file__))


def get_prompt_path(filename: str = "主要股东背景介绍一季报.md") -> str:
    """获取提示词文件路径。"""
    return os.path.join(get_base_dir(), filename)


def get_files_dir() -> str:
    """获取 files 目录路径。"""
    files_dir = os.path.join(get_base_dir(), "files")
    os.makedirs(files_dir, exist_ok=True)
    return files_dir


def get_logs_dir() -> str:
    """获取 logs 目录路径。"""
    logs_dir = os.path.join(get_base_dir(), "logs")
    os.makedirs(logs_dir, exist_ok=True)
    return logs_dir


def get_reports_dir() -> str:
    """获取 reports 目录路径。"""
    reports_dir = os.path.join(get_base_dir(), "reports")
    os.makedirs(reports_dir, exist_ok=True)
    return reports_dir
