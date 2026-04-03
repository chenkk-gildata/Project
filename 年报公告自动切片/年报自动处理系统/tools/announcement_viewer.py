"""
公告列表查看工具

功能: 查看公告列表及其下载和处理状态（支持实时日期筛选）
使用: python -m tools.announcement_viewer
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


def _get_announcements_data(date_filter=""):
    """获取公告列表数据"""
    conn = get_connection()
    if not conn:
        return []
    
    cursor = conn.cursor()
    
    if date_filter:
        like_pattern = f"{date_filter}%"
        cursor.execute("""
            SELECT gpdm, zqjc, publish_date, title, download_status, process_status 
            FROM announcements 
            WHERE fbsj LIKE ?
            ORDER BY fbsj ASC
        """, (like_pattern,))
    else:
        cursor.execute("""
            SELECT gpdm, zqjc, publish_date, title, download_status, process_status 
            FROM announcements 
            ORDER BY fbsj ASC
        """)
    
    rows = cursor.fetchall()
    conn.close()
    return rows


def _display_announcements_results(date_filter, rows):
    """显示公告列表结果"""
    from prettytable import PrettyTable
    
    os.system('cls')
    
    print("=" * 60)
    print("公告列表")
    print("=" * 60)
    print(f"\n请输入日期进行筛选 (格式: YYYY-MM-DD，按 ESC 退出):")
    print(f"> {date_filter}")
    
    date_display = date_filter if date_filter else "全部日期"
    print(f"\n筛选结果 ({date_display}):")
    
    if rows:
        table = PrettyTable()
        table.field_names = ["序号", "股票代码", "证券简称", "发布日期", "公告标题", "下载状态", "处理状态"]
        table.align["序号"] = "r"
        table.align["股票代码"] = "l"
        table.align["证券简称"] = "l"
        table.align["发布日期"] = "l"
        table.align["公告标题"] = "l"
        table.align["下载状态"] = "l"
        table.align["处理状态"] = "l"
        
        for idx, row in enumerate(rows, 1):
            gpdm = row[0] or "N/A"
            zqjc = row[1] or "N/A"
            pub_date = row[2] or "N/A"
            title = _extract_title(row[3])
            dl_status = row[4] or "N/A"
            proc_status = row[5] or "N/A"
            table.add_row([idx, gpdm, zqjc, pub_date, title, dl_status, proc_status])
        
        print(table)
        print(f"\n共 {len(rows)} 条记录")
    else:
        print("  (无记录)")


def show_announcements():
    """显示公告列表（支持实时日期筛选）"""
    import readchar
    
    date_filter = ""
    
    # 初始显示
    rows = _get_announcements_data(date_filter)
    _display_announcements_results(date_filter, rows)
    
    while True:
        try:
            ch = readchar.readchar()
            
            # ESC 键退出
            if ch == readchar.key.ESC:
                print("\n\n已退出公告列表查看")
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
            rows = _get_announcements_data(date_filter)
            _display_announcements_results(date_filter, rows)
            
        except KeyboardInterrupt:
            print("\n\n已退出公告列表查看")
            break


if __name__ == "__main__":
    show_announcements()
