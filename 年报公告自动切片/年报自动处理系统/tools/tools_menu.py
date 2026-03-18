"""
状态查看工具

功能: 在程序运行时独立查看数据库状态、失败模块列表和公告列表
使用: python tools\tools_menu.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.status_viewer import show_status, show_failed_modules
from tools.announcement_viewer import show_announcements


def show_menu():
    while True:
        print("\n" + "=" * 60)
        print("状态查看工具")
        print("=" * 60)
        print("  1. 查看数据库状态")
        print("  2. 查看失败模块列表")
        print("  3. 查看公告列表")
        print("  0. 退出")
        print("-" * 60)
        
        choice = input("请选择操作 (0-3): ").strip()
        
        if choice == "1":
            show_status()
        elif choice == "2":
            show_failed_modules()
        elif choice == "3":
            show_announcements()
        elif choice == "0":
            print("\n已退出状态查看工具")
            break
        else:
            print("\n[错误] 无效选项，请重新选择")


if __name__ == "__main__":
    show_menu()
