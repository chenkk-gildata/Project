"""
遗漏公告查看工具

功能: 根据SQL查询结果对比本地存储目录，查看原文件(raw)和各模块的遗漏公告
使用: python -m tools.missing_viewer
"""
import os
import sys
import pyodbc
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DB_CONFIG, DB_PATH, MODULE_NAMES, get_raw_dir, get_module_output_dir

MISSING_SQL = """
SELECT DISTINCT B.GPDM, B.ZQJC, CONVERT(DATE, A.XXFBRQ) AS XXFBRQ
FROM [10.101.0.212].JYPRIME.dbo.usrGSCWZYZB A
JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B ON A.INBBM = B.INBBM 
    AND B.ZQSC IN (18, 83, 90) 
    AND B.ZQLB IN (1, 2, 41) 
    AND B.SSZT = 1
WHERE A.XXFBRQ > '2026-01-01' 
    AND A.GGLB = 20 
    AND A.XXLYBM = 110101
"""


def get_custom_output_dir():
    """从本地数据库获取自定义输出目录"""
    if not os.path.exists(DB_PATH):
        return None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM system_status WHERE key = 'custom_output_dir'")
        row = cursor.fetchone()
        conn.close()
        if row and row[0] and row[0].strip():
            return row[0].strip()
        return None
    except Exception:
        return None


def get_processed_no_output_records():
    """获取本地数据库中已处理但无输出或跳过的记录
    
    Returns:
        dict: {module_name: set((gpdm, date), ...)}
    """
    result = {m: set() for m in MODULE_NAMES}
    result['raw'] = set()
    
    if not os.path.exists(DB_PATH):
        return result
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        for module_name in MODULE_NAMES:
            cursor.execute("""
                SELECT m.hashcode, a.gpdm, a.file_path
                FROM module_records m
                LEFT JOIN announcements a ON m.hashcode = a.hashcode
                WHERE m.module_name = ? 
                AND m.status IN ('no_output', 'skipped')
            """, (module_name,))
            
            for row in cursor.fetchall():
                hashcode, gpdm, file_path = row
                if file_path:
                    filename = os.path.basename(file_path)
                    parts = filename.split('-')
                    if len(parts) >= 4:
                        gpdm = parts[0]
                        date = parts[1] + parts[2] + parts[3]
                        result[module_name].add((gpdm, date))
        
        conn.close()
    except Exception as e:
        print(f"查询本地数据库失败: {e}")
    
    return result


def query_announcements_from_db():
    """从SQL Server查询公告列表"""
    try:
        conn = pyodbc.connect(
            SERVER=DB_CONFIG["server"],
            UID=DB_CONFIG["username"],
            PWD=DB_CONFIG["password"],
            DRIVER=DB_CONFIG["driver"]
        )
        cursor = conn.cursor()
        cursor.execute(MISSING_SQL)
        rows = cursor.fetchall()
        conn.close()
        return [(row.GPDM.strip() if row.GPDM else "", 
                 row.ZQJC.strip() if row.ZQJC else "",
                 row.XXFBRQ.strftime('%Y-%m-%d') if row.XXFBRQ else "") 
                for row in rows]
    except Exception as e:
        print(f"查询数据库失败: {e}")
        return []


def get_existing_files_in_dir(directory):
    """获取目录中所有PDF文件的股票代码和日期集合
    
    文件名格式: {gpdm}-{YYYY}-{MM}-{DD}-{公司名}-{标题}.pdf
    例如: 000001-2026-03-21-平安银行-2025年年度报告.pdf
    """
    if not os.path.isdir(directory):
        return set()
    
    existing = set()
    try:
        for f in os.listdir(directory):
            if f.lower().endswith('.pdf'):
                parts = f.split('-')
                if len(parts) >= 4:
                    gpdm = parts[0]
                    date = parts[1] + parts[2] + parts[3]
                    existing.add((gpdm, date))
    except Exception:
        pass
    return existing


def check_missing_announcements():
    """检查遗漏公告"""
    print("正在查询数据库...")
    announcements = query_announcements_from_db()
    
    if not announcements:
        print("未查询到公告数据")
        return
    
    print(f"数据库中共有 {len(announcements)} 条公告记录")
    print()
    
    custom_dir = get_custom_output_dir()
    
    raw_dir = get_raw_dir(custom_dir)
    raw_existing = get_existing_files_in_dir(raw_dir)
    
    module_existing = {}
    for module_name in MODULE_NAMES:
        module_dir = get_module_output_dir(module_name, custom_dir)
        module_existing[module_name] = get_existing_files_in_dir(module_dir)
    
    print("正在查询本地已处理记录...")
    no_output_records = get_processed_no_output_records()
    
    raw_missing = []
    module_missing = {m: [] for m in MODULE_NAMES}
    
    for gpdm, zqjc, xxfbrq in announcements:
        if not gpdm or not xxfbrq:
            continue
        
        key = (gpdm, xxfbrq.replace('-', '')[:8])
        
        if key not in raw_existing:
            raw_missing.append((gpdm, zqjc, xxfbrq))
        
        for module_name in MODULE_NAMES:
            if key not in module_existing[module_name]:
                if key not in no_output_records[module_name]:
                    module_missing[module_name].append((gpdm, zqjc, xxfbrq))
    
    print("=" * 70)
    print("遗漏公告统计")
    print("=" * 70)
    print(f"输出目录: {custom_dir if custom_dir else '默认目录'}")
    print()
    
    print("-" * 70)
    print(f"原文件(raw)遗漏: {len(raw_missing)} 条")
    print("-" * 70)
    if raw_missing:
        _display_missing_list(raw_missing, limit=20)
    
    for module_name in MODULE_NAMES:
        missing = module_missing[module_name]
        no_output_count = len(no_output_records[module_name])
        print()
        print("-" * 70)
        print(f"{module_name} 遗漏: {len(missing)} 条 (已排除 {no_output_count} 条无输出/跳过)")
        print("-" * 70)
        if missing:
            _display_missing_list(missing, limit=10)


def _display_missing_list(missing_list, limit=20):
    """显示遗漏列表"""
    from prettytable import PrettyTable
    
    if not missing_list:
        return
    
    table = PrettyTable()
    table.field_names = ["股票代码", "证券简称", "发布日期"]
    table.align["股票代码"] = "l"
    table.align["证券简称"] = "l"
    table.align["发布日期"] = "l"
    
    for item in missing_list[:limit]:
        table.add_row([item[0], item[1], item[2]])
    
    print(table)
    
    if len(missing_list) > limit:
        print(f"  ... 还有 {len(missing_list) - limit} 条未显示")


def show_missing_by_module():
    """按模块查看遗漏公告"""
    import readchar
    
    while True:
        os.system('cls')
        print("=" * 60)
        print("查看遗漏公告 - 按模块查看")
        print("=" * 60)
        print("\n请选择要查看的模块:")
        for i, name in enumerate(MODULE_NAMES, 1):
            print(f"  {i}. {name}")
        print("  7. 查看原文件(raw)遗漏")
        print("  8. 查看全部统计")
        print("  0. 返回上级")
        print("-" * 60)
        
        choice = input("请选择 (0-8): ").strip()
        
        if choice == "0":
            break
        elif choice == "7":
            _show_raw_missing()
        elif choice == "8":
            check_missing_announcements()
            input("\n按回车继续...")
        elif choice.isdigit():
            idx = int(choice)
            if 1 <= idx <= len(MODULE_NAMES):
                module_name = MODULE_NAMES[idx - 1]
                _show_module_missing(module_name)
        else:
            print("\n[错误] 无效选项")
            input("按回车继续...")


def _show_raw_missing():
    """显示原文件遗漏"""
    announcements = query_announcements_from_db()
    if not announcements:
        print("未查询到公告数据")
        input("按回车继续...")
        return
    
    custom_dir = get_custom_output_dir()
    raw_dir = get_raw_dir(custom_dir)
    raw_existing = get_existing_files_in_dir(raw_dir)
    
    missing = []
    for gpdm, zqjc, xxfbrq in announcements:
        if not gpdm or not xxfbrq:
            continue
        key = (gpdm, xxfbrq.replace('-', '')[:8])
        if key not in raw_existing:
            missing.append((gpdm, zqjc, xxfbrq))
    
    os.system('cls')
    print("=" * 60)
    print(f"原文件(raw)遗漏 - 共 {len(missing)} 条")
    print("=" * 60)
    print(f"目录: {raw_dir}")
    print()
    
    if missing:
        _display_missing_list(missing, limit=50)
    else:
        print("无遗漏文件")
    
    input("\n按回车继续...")


def _show_module_missing(module_name):
    """显示指定模块遗漏"""
    announcements = query_announcements_from_db()
    if not announcements:
        print("未查询到公告数据")
        input("按回车继续...")
        return
    
    custom_dir = get_custom_output_dir()
    module_dir = get_module_output_dir(module_name, custom_dir)
    module_existing = get_existing_files_in_dir(module_dir)
    
    no_output_records = get_processed_no_output_records()
    
    missing = []
    for gpdm, zqjc, xxfbrq in announcements:
        if not gpdm or not xxfbrq:
            continue
        key = (gpdm, xxfbrq.replace('-', '')[:8])
        if key not in module_existing:
            if key not in no_output_records[module_name]:
                missing.append((gpdm, zqjc, xxfbrq))
    
    no_output_count = len(no_output_records[module_name])
    
    os.system('cls')
    print("=" * 60)
    print(f"{module_name} 遗漏 - 共 {len(missing)} 条 (已排除 {no_output_count} 条无输出/跳过)")
    print("=" * 60)
    print(f"目录: {module_dir}")
    print()
    
    if missing:
        _display_missing_list(missing, limit=50)
    else:
        print("无遗漏文件")
    
    input("\n按回车继续...")


def show_missing_menu():
    """遗漏公告查看主菜单"""
    while True:
        os.system('cls')
        print("=" * 60)
        print("查看遗漏公告")
        print("=" * 60)
        print("\n请选择查看模式:")
        print("  1. 查看全部统计")
        print("  2. 按模块查看")
        print("  0. 返回上级")
        print("-" * 60)
        
        choice = input("请选择 (0-2): ").strip()
        
        if choice == "0":
            break
        elif choice == "1":
            check_missing_announcements()
            input("\n按回车继续...")
        elif choice == "2":
            show_missing_by_module()
        else:
            print("\n[错误] 无效选项")
            input("按回车继续...")


if __name__ == "__main__":
    show_missing_menu()
