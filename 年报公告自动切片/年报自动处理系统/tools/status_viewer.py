"""
数据库状态查看工具

功能: 查看系统状态、公告统计、模块处理统计、失败模块列表
使用: python -m tools.status_viewer
"""
import os
import sys
import sqlite3

from config import DB_PATH


def _extract_title(title: str) -> str:
    """
    从标题中提取实际标题内容
    
    标题格式通常为"简称：标题"或"简称:标题"
    以第一个":"或"："为分割，只返回后面的标题部分
    """
    if not title:
        return "N/A"
    
    for sep in ['：', ':']:
        idx = title.find(sep)
        if idx != -1:
            return title[idx + 1:].strip()
    
    return title


def get_connection():
    if not os.path.exists(DB_PATH):
        print(f"数据库文件不存在: {DB_PATH}")
        return None
    return sqlite3.connect(DB_PATH)


def _get_status_data(date_filter=""):
    """获取数据库状态数据"""
    conn = get_connection()
    if not conn:
        return None
    
    cursor = conn.cursor()
    result = {}
    
    # 系统状态（不受日期筛选影响）
    cursor.execute("SELECT * FROM system_status")
    result['system_status'] = cursor.fetchall()
    
    # 公告统计
    if date_filter:
        like_pattern = f"{date_filter}%"
        cursor.execute("SELECT COUNT(*) FROM announcements WHERE fbsj LIKE ?", (like_pattern,))
    else:
        cursor.execute("SELECT COUNT(*) FROM announcements")
    result['total_announcements'] = cursor.fetchone()[0]
    
    # 下载状态
    if date_filter:
        like_pattern = f"{date_filter}%"
        cursor.execute("SELECT download_status, COUNT(*) FROM announcements WHERE fbsj LIKE ? GROUP BY download_status", (like_pattern,))
    else:
        cursor.execute("SELECT download_status, COUNT(*) FROM announcements GROUP BY download_status")
    result['download_status'] = cursor.fetchall()
    
    # 处理状态
    if date_filter:
        like_pattern = f"{date_filter}%"
        cursor.execute("SELECT process_status, COUNT(*) FROM announcements WHERE fbsj LIKE ? GROUP BY process_status", (like_pattern,))
    else:
        cursor.execute("SELECT process_status, COUNT(*) FROM announcements GROUP BY process_status")
    result['process_status'] = cursor.fetchall()
    
    # 模块处理统计
    if date_filter:
        like_pattern = f"{date_filter}%"
        cursor.execute("""
            SELECT m.module_name, m.status, COUNT(*) 
            FROM module_records m
            JOIN announcements a ON m.hashcode = a.hashcode
            WHERE a.fbsj LIKE ?
            GROUP BY m.module_name, m.status
        """, (like_pattern,))
    else:
        cursor.execute("SELECT module_name, status, COUNT(*) FROM module_records GROUP BY module_name, status")
    result['module_records'] = cursor.fetchall()
    
    conn.close()
    return result


def _display_status_results(date_filter, data):
    """显示数据库状态结果"""
    from prettytable import PrettyTable
    
    # 清屏
    os.system('cls')
    
    print("=" * 60)
    print("数据库状态")
    print("=" * 60)
    print(f"\n请输入日期进行筛选 (格式: YYYY-MM-DD，按 ESC 退出):")
    print(f"> {date_filter}")
    
    date_display = date_filter if date_filter else "全部日期"
    
    # 系统状态
    print("\n" + "-" * 60)
    print("系统状态")
    print("-" * 60)
    if data['system_status']:
        from datetime import datetime
        key_map = {
            'last_query_time': '上次查询时间',
            'custom_output_dir': '自定义输出目录'
        }
        for row in data['system_status']:
            key_cn = key_map.get(row[0], row[0])
            value = row[1]
            # 格式化时间显示
            if row[0] == 'last_query_time' and value:
                try:
                    dt = datetime.fromisoformat(value)
                    value = dt.strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    pass
            print(f"  {key_cn}: {value}")
    else:
        print("  (空表 - 首次运行将查询最近7天数据)")
    
    # 公告统计
    print("\n" + "-" * 60)
    print(f"公告统计 ({date_display})")
    print("-" * 60)
    print(f"  总记录数: {data['total_announcements']}")
    
    if data['total_announcements'] > 0:
        # 下载状态表格
        status_map = {
            'pending': '等待下载',
            'downloading': '下载中',
            'success': '下载成功',
            'failed': '下载失败',
            'retrying': '重试中'
        }
        
        download_table = PrettyTable()
        download_table.field_names = ["等待下载", "下载中", "下载成功", "下载失败", "重试中", "总计"]
        for field in download_table.field_names:
            download_table.align[field] = "r"
        
        download_stats = {row[0]: row[1] for row in data['download_status']}
        pending = download_stats.get('pending', 0)
        downloading = download_stats.get('downloading', 0)
        success = download_stats.get('success', 0)
        failed = download_stats.get('failed', 0)
        retrying = download_stats.get('retrying', 0)
        total = pending + downloading + success + failed + retrying
        download_table.add_row([pending, downloading, success, failed, retrying, total])
        
        print("\n  下载状态:")
        print(download_table)
    
    # 模块处理统计
    print("\n" + "-" * 60)
    print(f"模块处理统计 ({date_display})")
    print("-" * 60)
    if data['module_records']:
        module_stats = {}
        for row in data['module_records']:
            module_name, status, count = row
            if module_name not in module_stats:
                module_stats[module_name] = {}
            module_stats[module_name][status] = count
        
        table = PrettyTable()
        table.field_names = ["模块名", "等待处理", "处理中", "成功", "无输出", "失败", "跳过", "总计"]
        table.align["模块名"] = "l"
        for field in ["等待处理", "处理中", "成功", "无输出", "失败", "跳过", "总计"]:
            table.align[field] = "r"
        
        for module_name in MODULE_NAMES:
            stats = module_stats.get(module_name, {})
            pending = stats.get('pending', 0)
            processing = stats.get('processing', 0)
            success = stats.get('success', 0)
            no_output = stats.get('no_output', 0)
            failed = stats.get('failed', 0)
            skipped = stats.get('skipped', 0)
            total = pending + processing + success + no_output + failed + skipped
            table.add_row([module_name, pending, processing, success, no_output, failed, skipped, total])
            
            if module_name == "主要指标":
                for sub_name in ZYZB_SUB_MODULES:
                    sub_stats = module_stats.get(sub_name, {})
                    sub_pending = sub_stats.get('pending', 0)
                    sub_processing = sub_stats.get('processing', 0)
                    sub_success = sub_stats.get('success', 0)
                    sub_no_output = sub_stats.get('no_output', 0)
                    sub_failed = sub_stats.get('failed', 0)
                    sub_skipped = sub_stats.get('skipped', 0)
                    sub_total = sub_pending + sub_processing + sub_success + sub_no_output + sub_failed + sub_skipped
                    table.add_row([f"  └─ {sub_name}", sub_pending, sub_processing, sub_success, sub_no_output, sub_failed, sub_skipped, sub_total])
        
        print(table)
    else:
        print("  (无记录)")


def show_status():
    """显示数据库状态（支持实时日期筛选）"""
    import readchar
    
    date_filter = ""
    
    # 初始显示
    data = _get_status_data(date_filter)
    if data:
        _display_status_results(date_filter, data)
    
    while True:
        try:
            ch = readchar.readchar()
            
            # ESC 键退出
            if ch == readchar.key.ESC:
                print("\n\n已退出数据库状态查看")
                break
            
            # 退格键删除
            elif ch == readchar.key.BACKSPACE or ch == '\x08':
                date_filter = date_filter[:-1]
            
            # 回车键忽略
            elif ch == '\r' or ch == '\n':
                continue
            
            # 普通字符输入（只允许数字和横杠）
            elif ch.isdigit() or ch == '-':
                if len(date_filter) < 10:
                    date_filter += ch
            
            # 更新显示
            data = _get_status_data(date_filter)
            if data:
                _display_status_results(date_filter, data)
            
        except KeyboardInterrupt:
            print("\n\n已退出数据库状态查看")
            break


def _get_failed_data(date_filter=""):
    """获取失败模块数据"""
    conn = get_connection()
    if not conn:
        return [], []
    
    cursor = conn.cursor()
    
    if date_filter:
        like_pattern = f"{date_filter}%"
        cursor.execute("""
            SELECT 
                a.gpdm, a.zqjc, a.publish_date, a.title,
                m.module_name, m.status
            FROM module_records m
            JOIN announcements a ON m.hashcode = a.hashcode
            WHERE m.status != 'success' AND a.fbsj LIKE ?
            AND NOT (m.module_name = '股东背景介绍' AND a.gpdm LIKE '688%')
            ORDER BY a.fbsj, a.gpdm
        """, (like_pattern,))
    else:
        cursor.execute("""
            SELECT 
                a.gpdm, a.zqjc, a.publish_date, a.title,
                m.module_name, m.status
            FROM module_records m
            JOIN announcements a ON m.hashcode = a.hashcode
            WHERE m.status != 'success'
            AND NOT (m.module_name = '股东背景介绍' AND a.gpdm LIKE '688%')
            ORDER BY a.fbsj, a.gpdm
        """)
    
    failed_records = cursor.fetchall()
    
    if date_filter:
        like_pattern = f"{date_filter}%"
        cursor.execute("""
            SELECT module_name, status, COUNT(*) 
            FROM module_records m
            JOIN announcements a ON m.hashcode = a.hashcode
            WHERE m.status != 'success' AND a.fbsj LIKE ?
            AND NOT (m.module_name = '股东背景介绍' AND a.gpdm LIKE '688%')
            GROUP BY module_name, status
        """, (like_pattern,))
    else:
        cursor.execute("""
            SELECT module_name, status, COUNT(*) 
            FROM module_records m
            JOIN announcements a ON m.hashcode = a.hashcode
            WHERE m.status != 'success'
            AND NOT (m.module_name = '股东背景介绍' AND a.gpdm LIKE '688%')
            GROUP BY module_name, status
        """)
    
    module_stats_raw = cursor.fetchall()
    
    module_stats = {}
    for row in module_stats_raw:
        module_name, status, count = row
        if module_name not in module_stats:
            module_stats[module_name] = {}
        module_stats[module_name][status] = count
    
    conn.close()
    
    return failed_records, module_stats


def _display_failed_results(date_filter, failed_records, module_stats):
    """显示失败模块结果"""
    from prettytable import PrettyTable
    
    # 清屏
    os.system('cls')
    
    print("=" * 60)
    print("失败模块列表")
    print("=" * 60)
    print(f"\n请输入日期进行筛选 (格式: YYYY-MM-DD，按 ESC 退出):")
    print(f"> {date_filter}")
    
    # 显示筛选日期范围
    date_display = date_filter if date_filter else "全部日期"
    print(f"\n模块失败统计 ({date_display}):")
    
    if module_stats:
        table = PrettyTable()
        table.field_names = ["模块名", "无输出", "失败", "跳过", "总计"]
        table.align["模块名"] = "l"
        for field in ["无输出", "失败", "跳过", "总计"]:
            table.align[field] = "r"
        
        for module_name, stats in module_stats.items():
            no_output = stats.get('no_output', 0)
            failed = stats.get('failed', 0)
            skipped = stats.get('skipped', 0)
            total = no_output + failed + skipped
            table.add_row([module_name, no_output, failed, skipped, total])
        
        print(table)
    else:
        print("  (无失败记录)")
    
    # 显示失败详情
    print(f"\n失败详情:")
    if failed_records:
        detail_table = PrettyTable()
        detail_table.field_names = ["股票代码", "证券简称", "发布日期", "公告标题", "模块名称", "失败状态"]
        detail_table.align["股票代码"] = "l"
        detail_table.align["证券简称"] = "l"
        detail_table.align["发布日期"] = "l"
        detail_table.align["公告标题"] = "l"
        detail_table.align["模块名称"] = "l"
        detail_table.align["失败状态"] = "l"
        
        status_map = {
            'no_output': '无输出',
            'failed': '失败',
            'skipped': '跳过'
        }
        
        for row in failed_records:
            gpdm, zqjc, pub_date, title, module_name, status = row
            status_cn = status_map.get(status, status)
            title_display = _extract_title(title)
            detail_table.add_row([gpdm, zqjc, pub_date, title_display, module_name, status_cn])
        
        print(detail_table)
    else:
        print("  (无失败记录)")
    
    print("\n状态说明: 无输出=模块执行完成但无结果 | 失败=执行出错 | 跳过=被跳过处理")


MODULE_NAMES = ["主要指标", "领导人介绍", "研发投入", "职工构成", "领导人持股", "股东背景介绍"]

ZYZB_SUB_MODULES = ["主要指标-补充"]


def _get_failed_by_module(module_name: str, date_filter=""):
    """按模块获取失败记录（按 fbsj 筛选）"""
    conn = get_connection()
    if not conn:
        return []
    
    cursor = conn.cursor()
    
    kcb_filter = "AND a.gpdm NOT LIKE '688%'" if module_name == "股东背景介绍" else ""
    
    if date_filter:
        like_pattern = f"{date_filter}%"
        cursor.execute(f"""
            SELECT 
                a.gpdm, a.zqjc, a.publish_date, a.title,
                m.module_name, m.status, a.fbsj
            FROM module_records m
            JOIN announcements a ON m.hashcode = a.hashcode
            WHERE m.module_name = ? AND m.status != 'success' AND a.fbsj LIKE ?
            {kcb_filter}
            ORDER BY a.fbsj DESC, a.gpdm
        """, (module_name, like_pattern))
    else:
        cursor.execute(f"""
            SELECT 
                a.gpdm, a.zqjc, a.publish_date, a.title,
                m.module_name, m.status, a.fbsj
            FROM module_records m
            JOIN announcements a ON m.hashcode = a.hashcode
            WHERE m.module_name = ? AND m.status != 'success'
            {kcb_filter}
            ORDER BY a.fbsj DESC, a.gpdm
        """, (module_name,))
    
    records = cursor.fetchall()
    conn.close()
    
    return records


def _display_module_failed_detail(module_name: str, date_filter, records):
    """显示模块失败详情"""
    from prettytable import PrettyTable
    
    os.system('cls')
    
    print("=" * 60)
    print(f"失败模块详情 - {module_name}")
    print("=" * 60)
    print(f"\n请输入日期进行筛选 (格式: YYYY-MM-DD，按 fbsj 筛选，按 ESC 返回):")
    print(f"> {date_filter}")
    
    # 当选择"主要指标"时，同时显示"主要指标-补充"的失败记录
    if module_name == "主要指标":
        records.extend(_get_failed_by_module("主要指标-补充", date_filter))
    
    date_display = date_filter if date_filter else "全部日期"
    print(f"\n筛选结果 ({date_display}):")
    
    if records:
        no_output_records = [r for r in records if r[5] == 'no_output']
        failed_records = [r for r in records if r[5] == 'failed']
        skipped_records = [r for r in records if r[5] == 'skipped']
        
        status_map = {
            'no_output': '无输出',
            'failed': '失败',
            'skipped': '跳过'
        }
        
        if no_output_records:
            print(f"\n--- 无输出 ({len(no_output_records)}条) ---")
            table = PrettyTable()
            table.field_names = ["模块名称", "股票代码", "证券简称", "发布日期", "公告标题", "信息时间"]
            table.align["公告标题"] = "l"
            for r in no_output_records:
                fbsj = r[6][:19].replace('T', ' ') if r[6] else "N/A"
                title_display = _extract_title(r[3])
                table.add_row([r[4] or "N/A", r[0] or "N/A", r[1] or "N/A", r[2] or "N/A", title_display, fbsj])
            print(table)
        
        if failed_records:
            print(f"\n--- 失败 ({len(failed_records)}条) ---")
            table = PrettyTable()
            table.field_names = ["模块名称", "股票代码", "证券简称", "发布日期", "公告标题", "信息时间"]
            table.align["公告标题"] = "l"
            for r in failed_records:
                fbsj = r[6][:19].replace('T', ' ') if r[6] else "N/A"
                title_display = _extract_title(r[3])
                table.add_row([r[4] or "N/A", r[0] or "N/A", r[1] or "N/A", r[2] or "N/A", title_display, fbsj])
            print(table)
        
        if skipped_records:
            print(f"\n--- 跳过 ({len(skipped_records)}条) ---")
            table = PrettyTable()
            table.field_names = ["模块名称", "股票代码", "证券简称", "发布日期", "公告标题", "信息时间"]
            table.align["公告标题"] = "l"
            for r in skipped_records:
                fbsj = r[6][:19].replace('T', ' ') if r[6] else "N/A"
                title_display = _extract_title(r[3])
                table.add_row([r[4] or "N/A", r[0] or "N/A", r[1] or "N/A", r[2] or "N/A", title_display, fbsj])
            print(table)
        
        print(f"\n总计: {len(records)} 条记录")
    else:
        print("  (无失败记录)")
    
    print("\n状态说明: 无输出=模块执行完成但无结果 | 失败=执行出错 | 跳过=被跳过处理")


def _show_module_failed_detail(module_name: str):
    """显示模块失败详情（支持动态日期筛选）"""
    import readchar
    
    date_filter = ""
    
    records = _get_failed_by_module(module_name, date_filter)
    _display_module_failed_detail(module_name, date_filter, records)
    
    while True:
        try:
            ch = readchar.readchar()
            
            if ch == readchar.key.ESC:
                print("\n\n返回模块选择")
                break
            
            elif ch == readchar.key.BACKSPACE or ch == '\x08':
                date_filter = date_filter[:-1]
            
            elif ch == '\r' or ch == '\n':
                continue
            
            elif ch.isdigit() or ch == '-':
                if len(date_filter) < 10:
                    date_filter += ch
            
            records = _get_failed_by_module(module_name, date_filter)
            _display_module_failed_detail(module_name, date_filter, records)
            
        except KeyboardInterrupt:
            print("\n\n返回模块选择")
            break


def _show_failed_by_module():
    """按模块查看失败记录"""
    while True:
        os.system('cls')
        print("=" * 60)
        print("按模块查看失败记录")
        print("=" * 60)
        print("\n请选择模块:")
        for i, name in enumerate(MODULE_NAMES, 1):
            print(f"  {i}. {name}")
        print("  0. 返回上级")
        print("-" * 60)
        
        choice = input(f"请选择 (0-{len(MODULE_NAMES)}): ").strip()
        
        if choice == "0":
            break
        elif choice.isdigit() and 1 <= int(choice) <= len(MODULE_NAMES):
            module_name = MODULE_NAMES[int(choice) - 1]
            _show_module_failed_detail(module_name)
        else:
            print("\n[错误] 无效选项，请重新选择")
            input("按回车继续...")


def show_failed_modules():
    """显示失败模块列表入口"""
    while True:
        os.system('cls')
        print("=" * 60)
        print("查看失败模块列表")
        print("=" * 60)
        print("\n请选择查看模式:")
        print("  1. 按时间查看")
        print("  2. 按模块查看")
        print("  0. 返回上级")
        print("-" * 60)
        
        choice = input("请选择 (0-2): ").strip()
        
        if choice == "0":
            break
        elif choice == "1":
            _show_failed_by_time()
        elif choice == "2":
            _show_failed_by_module()
        else:
            print("\n[错误] 无效选项，请重新选择")
            input("按回车继续...")


def _show_failed_by_time():
    """按时间查看失败模块列表（原有逻辑）"""
    import readchar
    
    date_filter = ""
    
    failed_records, module_stats = _get_failed_data(date_filter)
    _display_failed_results(date_filter, failed_records, module_stats)
    
    while True:
        try:
            ch = readchar.readchar()
            
            if ch == readchar.key.ESC:
                print("\n\n返回上级菜单")
                break
            
            elif ch == readchar.key.BACKSPACE or ch == '\x08':
                date_filter = date_filter[:-1]
            
            elif ch == '\r' or ch == '\n':
                continue
            
            elif ch.isdigit() or ch == '-':
                if len(date_filter) < 10:
                    date_filter += ch
            
            failed_records, module_stats = _get_failed_data(date_filter)
            _display_failed_results(date_filter, failed_records, module_stats)
            
        except KeyboardInterrupt:
            print("\n\n返回上级菜单")
            break


if __name__ == "__main__":
    show_status()
