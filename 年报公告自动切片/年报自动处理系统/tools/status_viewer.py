"""
数据库状态查看工具

功能: 查看系统状态、公告统计、模块处理统计、失败模块列表
使用: python -m tools.status_viewer
"""
import os
import sys
import sqlite3

from config import DB_PATH


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
        cursor.execute("SELECT COUNT(*) FROM announcements WHERE publish_date LIKE ?", (like_pattern,))
    else:
        cursor.execute("SELECT COUNT(*) FROM announcements")
    result['total_announcements'] = cursor.fetchone()[0]
    
    # 下载状态
    if date_filter:
        like_pattern = f"{date_filter}%"
        cursor.execute("SELECT download_status, COUNT(*) FROM announcements WHERE publish_date LIKE ? GROUP BY download_status", (like_pattern,))
    else:
        cursor.execute("SELECT download_status, COUNT(*) FROM announcements GROUP BY download_status")
    result['download_status'] = cursor.fetchall()
    
    # 处理状态
    if date_filter:
        like_pattern = f"{date_filter}%"
        cursor.execute("SELECT process_status, COUNT(*) FROM announcements WHERE publish_date LIKE ? GROUP BY process_status", (like_pattern,))
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
            WHERE a.publish_date LIKE ?
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
        table.field_names = ["模块名", "成功", "无输出", "失败", "跳过", "总计"]
        table.align["模块名"] = "l"
        for field in ["成功", "无输出", "失败", "跳过", "总计"]:
            table.align[field] = "r"
        
        for module_name, stats in module_stats.items():
            success = stats.get('success', 0)
            no_output = stats.get('no_output', 0)
            failed = stats.get('failed', 0)
            skipped = stats.get('skipped', 0)
            total = success + no_output + failed + skipped
            table.add_row([module_name, success, no_output, failed, skipped, total])
        
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
    
    # 构建查询条件
    if date_filter:
        like_pattern = f"{date_filter}%"
        cursor.execute("""
            SELECT 
                a.gpdm, a.zqjc, a.publish_date, 
                m.module_name, m.status
            FROM module_records m
            JOIN announcements a ON m.hashcode = a.hashcode
            WHERE m.status != 'success' AND a.publish_date LIKE ?
            ORDER BY a.publish_date DESC, a.gpdm
        """, (like_pattern,))
    else:
        cursor.execute("""
            SELECT 
                a.gpdm, a.zqjc, a.publish_date, 
                m.module_name, m.status
            FROM module_records m
            JOIN announcements a ON m.hashcode = a.hashcode
            WHERE m.status != 'success'
            ORDER BY a.publish_date DESC, a.gpdm
        """)
    
    failed_records = cursor.fetchall()
    
    # 获取模块统计（按日期筛选）
    if date_filter:
        like_pattern = f"{date_filter}%"
        cursor.execute("""
            SELECT module_name, status, COUNT(*) 
            FROM module_records m
            JOIN announcements a ON m.hashcode = a.hashcode
            WHERE m.status != 'success' AND a.publish_date LIKE ?
            GROUP BY module_name, status
        """, (like_pattern,))
    else:
        cursor.execute("""
            SELECT module_name, status, COUNT(*) 
            FROM module_records m
            JOIN announcements a ON m.hashcode = a.hashcode
            WHERE m.status != 'success'
            GROUP BY module_name, status
        """)
    
    module_stats_raw = cursor.fetchall()
    
    # 整理模块统计
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
        detail_table.field_names = ["股票代码", "证券简称", "发布日期", "模块名称", "失败状态"]
        detail_table.align["股票代码"] = "l"
        detail_table.align["证券简称"] = "l"
        detail_table.align["发布日期"] = "l"
        detail_table.align["模块名称"] = "l"
        detail_table.align["失败状态"] = "l"
        
        status_map = {
            'no_output': '无输出',
            'failed': '失败',
            'skipped': '跳过'
        }
        
        for row in failed_records:
            gpdm, zqjc, pub_date, module_name, status = row
            status_cn = status_map.get(status, status)
            detail_table.add_row([gpdm, zqjc, pub_date, module_name, status_cn])
        
        print(detail_table)
    else:
        print("  (无失败记录)")
    
    print("\n状态说明: 无输出=模块执行完成但无结果 | 失败=执行出错 | 跳过=被跳过处理")


def show_failed_modules():
    """显示失败模块列表（支持实时日期筛选）"""
    import readchar
    
    date_filter = ""
    
    # 初始显示
    failed_records, module_stats = _get_failed_data(date_filter)
    _display_failed_results(date_filter, failed_records, module_stats)
    
    while True:
        try:
            ch = readchar.readchar()
            
            # ESC 键退出
            if ch == readchar.key.ESC:
                print("\n\n已退出失败模块列表")
                break
            
            # 退格键删除
            elif ch == readchar.key.BACKSPACE or ch == '\x08':
                date_filter = date_filter[:-1]
            
            # 回车键忽略
            elif ch == '\r' or ch == '\n':
                continue
            
            # 普通字符输入（只允许数字和横杠）
            elif ch.isdigit() or ch == '-':
                if len(date_filter) < 10:  # 限制日期格式长度
                    date_filter += ch
            
            # 更新显示
            failed_records, module_stats = _get_failed_data(date_filter)
            _display_failed_results(date_filter, failed_records, module_stats)
            
        except KeyboardInterrupt:
            print("\n\n已退出失败模块列表")
            break


if __name__ == "__main__":
    show_status()
