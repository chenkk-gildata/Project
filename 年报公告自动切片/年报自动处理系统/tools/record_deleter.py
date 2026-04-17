"""
记录删除工具

功能: 根据hashcode删除数据库中的公告记录和模块记录
使用: python -m tools.record_deleter
"""
import os
import sys
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DB_PATH


def delete_records_by_hashcode(hashcodes):
    """根据hashcode列表删除数据库记录
    
    Args:
        hashcodes: hashcode列表
        
    Returns:
        dict: 删除结果统计
    """
    if not os.path.exists(DB_PATH):
        print(f"数据库文件不存在: {DB_PATH}")
        return None
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    result = {
        'announcements_deleted': 0,
        'module_records_deleted': 0,
        'details': []
    }
    
    for hc in hashcodes:
        hc = hc.strip()
        if not hc:
            continue
            
        cursor.execute("SELECT hashcode, gpdm, zqjc, title FROM announcements WHERE LOWER(hashcode) = LOWER(?)", (hc,))
        ann_row = cursor.fetchone()
        
        cursor.execute("SELECT COUNT(*) FROM module_records WHERE LOWER(hashcode) = LOWER(?)", (hc,))
        module_count = cursor.fetchone()[0]
        
        actual_hc = ann_row[0] if ann_row else hc
        
        detail = {
            'hashcode': actual_hc,
            'gpdm': ann_row[1] if ann_row else None,
            'zqjc': ann_row[2] if ann_row else None,
            'title': ann_row[3] if ann_row else None,
            'ann_existed': ann_row is not None,
            'module_count': module_count,
            'ann_deleted': 0,
            'modules_deleted': 0
        }
        
        cursor.execute("DELETE FROM module_records WHERE LOWER(hashcode) = LOWER(?)", (hc,))
        detail['modules_deleted'] = cursor.rowcount
        result['module_records_deleted'] += cursor.rowcount
        
        cursor.execute("DELETE FROM announcements WHERE LOWER(hashcode) = LOWER(?)", (hc,))
        detail['ann_deleted'] = cursor.rowcount
        result['announcements_deleted'] += cursor.rowcount
        
        result['details'].append(detail)
    
    conn.commit()
    conn.close()
    
    return result


def show_delete_menu():
    """删除记录交互界面"""
    while True:
        os.system('cls')
        print("=" * 60)
        print("删除数据库记录")
        print("=" * 60)
        print("\n说明:")
        print("  根据hashcode删除announcements表和module_records表中的相关记录")
        print("  支持批量删除，多个hashcode用逗号或空格分隔")
        print("-" * 60)
        print("  1. 输入hashcode删除记录")
        print("  0. 返回上级")
        print("-" * 60)
        
        choice = input("请选择 (0-1): ").strip()
        
        if choice == "0":
            break
        elif choice == "1":
            _interactive_delete()
        else:
            print("\n[错误] 无效选项")
            input("按回车继续...")


def _interactive_delete():
    """交互式删除流程"""
    print("\n" + "-" * 60)
    print("请输入要删除的hashcode:")
    print("  - 支持逗号、空格、换行分隔")
    print("  - 输入空行结束输入")
    print("  - 输入 'cancel' 取消操作")
    print("-" * 60)
    
    lines = []
    while True:
        line = input().strip()
        if line.lower() == 'cancel':
            print("\n已取消操作")
            input("按回车继续...")
            return
        if line == '':
            break
        lines.append(line)
    
    if not lines:
        print("\n[错误] 未输入任何hashcode")
        input("按回车继续...")
        return
    
    hashcodes = []
    for line in lines:
        for item in line.replace(',', ' ').split():
            item = item.strip()
            if item:
                hashcodes.append(item)
    
    if not hashcodes:
        print("\n[错误] 未解析到有效的hashcode")
        input("按回车继续...")
        return
    
    print(f"\n解析到 {len(hashcodes)} 个hashcode:")
    for i, hc in enumerate(hashcodes, 1):
        print(f"  {i}. {hc}")
    
    confirm = input("\n确认删除以上记录? (y/n): ").strip().lower()
    if confirm != 'y':
        print("\n已取消删除")
        input("按回车继续...")
        return
    
    print("\n正在删除...")
    result = delete_records_by_hashcode(hashcodes)
    
    if result is None:
        input("按回车继续...")
        return
    
    print("\n" + "=" * 60)
    print("删除结果")
    print("=" * 60)
    
    for detail in result['details']:
        hc = detail['hashcode']
        gpdm = detail['gpdm'] or 'N/A'
        zqjc = detail['zqjc'] or 'N/A'
        print(f"\n{hc}...")
        print(f"  股票: {gpdm} {zqjc}")
        print(f"  announcements删除: {detail['ann_deleted']}条")
        print(f"  module_records删除: {detail['modules_deleted']}条")
    
    print("\n" + "-" * 60)
    print(f"总计: announcements删除{result['announcements_deleted']}条, module_records删除{result['module_records_deleted']}条")
    
    input("\n按回车继续...")


if __name__ == "__main__":
    show_delete_menu()
