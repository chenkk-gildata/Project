"""
年报公告自动处理系统 - 主程序入口
"""
import os
import re
import sys
import time
import atexit
import signal
import sqlite3
import threading
import uuid
import pyodbc
from datetime import datetime
from typing import Optional

from config import (
    STOP_SIGNAL_FILE,
    LOCK_FILE,
    DB_PATH,
    REPORTS_DIR,
    MODULE_NAMES,
    CUSTOM_OUTPUT_DIR_KEY,
    DB_CONFIG,
)
from logger import logger, temporarily_raise_console_level
from database import db
from queues import queue_manager
from monitor import monitor
from downloader import downloader
from task_dispatcher import task_dispatcher
from models import Announcement, DownloadTask, ProcessTask, ProcessStatus, DownloadStatus


def acquire_lock() -> bool:
    """获取进程锁，防止重复启动"""
    try:
        if os.path.exists(LOCK_FILE):
            try:
                with open(LOCK_FILE, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    pid = int(content)

                import ctypes

                kernel32 = ctypes.windll.kernel32
                handle = kernel32.OpenProcess(1, False, pid)
                if handle != 0:
                    kernel32.CloseHandle(handle)
                    return False
                os.remove(LOCK_FILE)
            except (ValueError, OSError):
                try:
                    os.remove(LOCK_FILE)
                except OSError:
                    pass

        with open(LOCK_FILE, "w", encoding="utf-8") as f:
            f.write(str(os.getpid()))
        return True
    except Exception as e:
        logger.error(f"获取进程锁失败: {e}")
        return False


def release_lock():
    """释放进程锁"""
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
            logger.info("进程锁已释放")
    except Exception as e:
        logger.error(f"释放进程锁失败: {e}")


atexit.register(release_lock)


class SettingsMenu:
    """设置菜单"""

    @staticmethod
    def show_main_menu():
        while True:
            print("\n" + "=" * 60)
            print("设置菜单")
            print("=" * 60)
            print("  1. 重置查询时间")
            print("  2. 清理数据")
            print("  3. 修改业务模块输出目录")
            print("  0. 返回上级")
            print("-" * 60)

            choice = input("请选择操作 (0-3): ").strip()

            if choice == "1":
                SettingsMenu.reset_query_time()
            elif choice == "2":
                SettingsMenu.clean_data()
            elif choice == "3":
                SettingsMenu.set_output_dir()
            elif choice == "0":
                return
            else:
                print("\n[错误] 无效选项")

    @staticmethod
    def _get_connection() -> Optional[sqlite3.Connection]:
        if not os.path.exists(DB_PATH):
            print(f"数据库文件不存在: {DB_PATH}")
            return None
        return sqlite3.connect(DB_PATH)

    @staticmethod
    def reset_query_time():
        conn = SettingsMenu._get_connection()
        if not conn:
            return

        cursor = conn.cursor()
        print("\n" + "-" * 40)
        print("重置查询时间")
        print("-" * 40)
        print("  1. 删除查询时间(下次查询最近7天)")
        print("  2. 设置为指定时间")
        print("  3. 设置为当前时间")
        print("  0. 返回")

        choice = input("\n请选择 (0-3): ").strip()

        if choice == "1":
            cursor.execute("DELETE FROM system_status WHERE key = 'last_query_time'")
            conn.commit()
            print("\n[成功] 已删除查询时间")
        elif choice == "2":
            date_str = input("请输入日期时间 (YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS): ").strip()
            try:
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                iso_time = dt.isoformat()
                cursor.execute(
                    """
                    INSERT INTO system_status (key, value, updated_at)
                    VALUES ('last_query_time', ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value,
                        updated_at = excluded.updated_at
                    """,
                    (iso_time, datetime.now().isoformat()),
                )
                conn.commit()
                print(f"\n[成功] 已设置查询时间为: {iso_time}")
            except ValueError:
                print("\n[错误] 日期格式不正确")
        elif choice == "3":
            iso_time = datetime.now().isoformat()
            cursor.execute(
                """
                INSERT INTO system_status (key, value, updated_at)
                VALUES ('last_query_time', ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (iso_time, datetime.now().isoformat()),
            )
            conn.commit()
            print(f"\n[成功] 已设置为当前时间: {iso_time}")

        conn.close()

    @staticmethod
    def clean_data():
        conn = SettingsMenu._get_connection()
        if not conn:
            return

        cursor = conn.cursor()
        print("\n" + "-" * 40)
        print("数据清理")
        print("-" * 40)
        print("  1. 清空所有公告记录")
        print("  2. 清空模块处理记录")
        print("  3. 清空系统状态")
        print("  4. 清空所有数据")
        print("  0. 返回")

        choice = input("\n请选择 (0-4): ").strip()

        if choice == "1":
            if input("确认清空公告记录? (y/n): ").strip().lower() == "y":
                cursor.execute("DELETE FROM announcements")
                conn.commit()
                print("\n[成功] 公告记录已清空")
        elif choice == "2":
            if input("确认清空模块记录? (y/n): ").strip().lower() == "y":
                cursor.execute("DELETE FROM module_records")
                conn.commit()
                print("\n[成功] 模块记录已清空")
        elif choice == "3":
            if input("确认清空系统状态? (y/n): ").strip().lower() == "y":
                cursor.execute("DELETE FROM system_status")
                conn.commit()
                print("\n[成功] 系统状态已清空")
        elif choice == "4":
            if input("确认清空全部数据? (y/n): ").strip().lower() == "y":
                cursor.execute("DELETE FROM announcements")
                cursor.execute("DELETE FROM module_records")
                cursor.execute("DELETE FROM system_status")
                conn.commit()
                print("\n[成功] 全部数据已清空")

        conn.close()

    @staticmethod
    def set_output_dir():
        conn = SettingsMenu._get_connection()
        if not conn:
            return

        cursor = conn.cursor()
        current_dir = db.get_system_status(CUSTOM_OUTPUT_DIR_KEY)
        default_dir = REPORTS_DIR
        is_custom = bool(current_dir and current_dir.strip())

        print("\n" + "-" * 60)
        print("业务模块输出目录设置")
        print("-" * 60)
        print(f"当前目录: {current_dir if is_custom else default_dir}")
        print("  1. 设置新的输出目录")
        print("  2. 恢复默认目录")
        print("  0. 返回")

        choice = input("\n请选择 (0-2): ").strip()

        if choice == "1":
            new_dir = input("请输入输出目录 (留空取消): ").strip()
            if new_dir:
                new_dir = os.path.abspath(new_dir)
                try:
                    os.makedirs(new_dir, exist_ok=True)
                    for module_name in MODULE_NAMES:
                        os.makedirs(os.path.join(new_dir, module_name), exist_ok=True)
                    cursor.execute(
                        """
                        INSERT INTO system_status (key, value, updated_at)
                        VALUES (?, ?, ?)
                        ON CONFLICT(key) DO UPDATE SET
                            value = excluded.value,
                            updated_at = excluded.updated_at
                        """,
                        (CUSTOM_OUTPUT_DIR_KEY, new_dir, datetime.now().isoformat()),
                    )
                    conn.commit()
                    print(f"\n[成功] 已设置输出目录: {new_dir}")
                except Exception as e:
                    print(f"\n[错误] 设置失败: {e}")
        elif choice == "2":
            if is_custom and input("确认恢复默认目录? (y/n): ").strip().lower() == "y":
                cursor.execute("DELETE FROM system_status WHERE key = ?", (CUSTOM_OUTPUT_DIR_KEY,))
                conn.commit()
                print(f"\n[成功] 已恢复默认目录: {default_dir}")

        conn.close()


class SemiAutoMenu:
    """半自动补处理菜单"""

    @staticmethod
    def show_main_menu():
        while True:
            print("\n" + "=" * 60)
            print("半自动补处理")
            print("=" * 60)
            print("  1. 按MD5全量重跑(重新下载+全模块切片)")
            print("  2. 已下载公告重处理(可选模块)")
            print("  0. 返回")
            print("-" * 60)
            choice = input("请选择 (0-2): ").strip()

            if choice == "1":
                SemiAutoMenu.run_full_rerun_by_md5()
            elif choice == "2":
                SemiAutoMenu.run_reprocess_downloaded()
            elif choice == "0":
                return
            else:
                print("\n[错误] 无效选项")

    @staticmethod
    def _read_multiline_tokens(prompt: str) -> list[str]:
        print(prompt)
        lines: list[str] = []
        while True:
            line = input("> ").strip()
            if not line:
                break
            lines.append(line)
        raw = " ".join(lines).strip()
        if not raw:
            return []
        return [x.strip() for x in re.split(r"[\s,;，；]+", raw) if x.strip()]

    @staticmethod
    def _parse_md5_list(tokens: list[str]) -> tuple[list[str], list[str]]:
        valid: list[str] = []
        invalid: list[str] = []
        seen: set[str] = set()
        for token in tokens:
            md5 = token.strip().upper()
            if re.fullmatch(r"[0-9a-fA-F]{32}", md5):
                if md5 not in seen:
                    valid.append(md5)
                    seen.add(md5)
            else:
                invalid.append(token)
        return valid, invalid

    @staticmethod
    def _ensure_workers_started() -> tuple[bool, bool]:
        started_downloader = False
        started_dispatcher = False
        if not downloader.is_running():
            downloader.start()
            started_downloader = True
        if not task_dispatcher.is_running():
            task_dispatcher.start()
            started_dispatcher = True
        return started_downloader, started_dispatcher

    @staticmethod
    def _stop_workers_if_needed(started_downloader: bool, started_dispatcher: bool):
        if started_downloader:
            downloader.stop()
        if started_dispatcher:
            task_dispatcher.stop()

    @staticmethod
    def _build_task_announcement(hashcode: str) -> Announcement:
        ann = db.get_announcement(hashcode)
        if ann:
            ann.download_status = DownloadStatus.PENDING
            ann.process_status = ProcessStatus.PENDING
            ann.file_path = None
            ann.download_retry_count = 0
            ann.process_retry_count = 0
            ann.download_error = None
            ann.process_error = None
            return ann
        return Announcement(hashcode=hashcode)

    @staticmethod
    def _wait_full_rerun(hashcodes: list[str], batch_id: str) -> list[tuple[str, str, str]]:
        state_cache: dict[str, tuple[str, str]] = {}
        while True:
            total = len(hashcodes)
            finished = 0
            details: list[tuple[str, str, str]] = []

            for h in hashcodes:
                ann = db.get_announcement(h)
                if not ann:
                    details.append((h, "pending", "pending"))
                    continue
                ds = ann.download_status.value
                ps = ann.process_status.value
                details.append((h, ds, ps))

                prev = state_cache.get(h)
                if prev is None or prev[0] != ds:
                    file_name = os.path.basename(ann.file_path) if ann.file_path else h
                    if ds == "success":
                        print(f"  {file_name} 下载成功")
                    elif ds == "failed":
                        print(f"  {h} 下载失败")

                if ds == "success" and ps in ("success", "failed") and (prev is None or prev[1] != ps):
                    file_name = os.path.basename(ann.file_path) if ann.file_path else h
                    suc = sum(1 for st in ann.module_status.values() if st.value == "success")
                    no_or_skip_modules = [m for m, st in ann.module_status.items() if st.value in ("no_output", "skipped")]
                    failed_modules = [m for m, st in ann.module_status.items() if st.value == "failed"]
                    result_parts = [f"{suc}模块完成"]
                    if no_or_skip_modules:
                        result_parts.append(f"{'/'.join(no_or_skip_modules)}模块无输出/跳过")
                    if failed_modules:
                        result_parts.append(f"{'/'.join(failed_modules)}模块失败")
                    print(f"  {file_name} 处理结果: {', '.join(result_parts)}")

                state_cache[h] = (ds, ps)

                if ds == "success" and ps in ("success", "failed"):
                    finished += 1
                elif ds == "failed":
                    finished += 1

            if finished >= total:
                return details
            # 仅在状态变化时打印，缩短轮询间隔可提升“下载成功/处理结果”提示的实时性
            time.sleep(0.2)

    @staticmethod
    def _print_module_summary(hashcodes: list[str]):
        count = {
            "success": 0,
            "no_output": 0,
            "skipped": 0,
            "failed": 0,
            "pending": 0,
            "processing": 0,
        }
        for h in hashcodes:
            ann = db.get_announcement(h)
            if not ann:
                continue
            for st in ann.module_status.values():
                count[st.value] = count.get(st.value, 0) + 1

        print("\n模块执行汇总")
        print("-" * 60)
        labels = {
            "success": "成功",
            "no_output": "无输出",
            "skipped": "跳过",
            "failed": "失败",
            "pending": "待处理",
            "processing": "处理中",
        }
        try:
            from prettytable import PrettyTable

            table = PrettyTable()
            table.field_names = ["状态", "数量"]
            table.align["状态"] = "l"
            table.align["数量"] = "r"
            for k in ["success", "no_output", "skipped", "failed", "pending", "processing"]:
                table.add_row([labels[k], count.get(k, 0)])
            print(table)
        except Exception:
            for k in ["success", "no_output", "skipped", "failed", "pending", "processing"]:
                print(f"  {labels[k]}: {count.get(k, 0)}")

    @staticmethod
    def _print_full_rerun_item_results(hashcodes: list[str]):
        print("\n单公告结果")
        print("-" * 60)
        for h in hashcodes:
            ann = db.get_announcement(h)
            if not ann:
                print(f"  {h} 下载失败(无记录)")
                continue
            file_name = os.path.basename(ann.file_path) if ann.file_path else h
            dl_ok = ann.download_status == DownloadStatus.SUCCESS
            print(f"  {file_name} 下载{'成功' if dl_ok else '失败'}")

            suc = sum(1 for st in ann.module_status.values() if st.value == "success")
            no_or_skip_modules = [m for m, st in ann.module_status.items() if st.value in ("no_output", "skipped")]
            failed_modules = [m for m, st in ann.module_status.items() if st.value == "failed"]
            result_parts = [f"{suc}模块完成"]
            if no_or_skip_modules:
                result_parts.append(f"{'/'.join(no_or_skip_modules)}模块无输出/跳过")
            if failed_modules:
                result_parts.append(f"{'/'.join(failed_modules)}模块失败")
            print(f"    处理结果: {', '.join(result_parts)}")

    @staticmethod
    def run_full_rerun_by_md5():
        tokens = SemiAutoMenu._read_multiline_tokens("请输入需要全量重跑的公告MD5:")
        md5_list, invalid = SemiAutoMenu._parse_md5_list(tokens)
        if invalid:
            print(f"\n[警告] 以下MD5格式不合法，已忽略: {', '.join(invalid)}")
        if not md5_list:
            print("\n[提示] 没有可用MD5")
            return

        batch_id = datetime.now().strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]
        logger.info(f"[半自动全量重跑] 批次 {batch_id} 开始，数量={len(md5_list)}")

        enqueue_ok = 0
        enqueue_failed: list[str] = []
        detail: list[tuple[str, str, str]] = []
        with temporarily_raise_console_level():
            download_queue = queue_manager.get_download_queue()
            for h in md5_list:
                source_ann = SemiAutoMenu._fetch_announcement_by_hash_from_source(h)
                if source_ann:
                    db.save_announcement(source_ann, sync=True)
                if not db.reset_announcement_for_full_rerun(h):
                    enqueue_failed.append(h)
                    continue
                if source_ann:
                    ann = source_ann
                    ann.download_status = DownloadStatus.PENDING
                    ann.process_status = ProcessStatus.PENDING
                    ann.file_path = None
                    ann.download_retry_count = 0
                    ann.process_retry_count = 0
                    ann.download_error = None
                    ann.process_error = None
                else:
                    ann = SemiAutoMenu._build_task_announcement(h)
                if download_queue.put(DownloadTask(announcement=ann), block=False):
                    enqueue_ok += 1
                else:
                    enqueue_failed.append(h)

            print(f"\n已提交 {enqueue_ok} 条任务到下载队列")
            if enqueue_failed:
                print(f"[警告] 提交失败MD5: {', '.join(enqueue_failed)}")

            started_downloader = False
            started_dispatcher = False
            if enqueue_ok > 0:
                started_downloader, started_dispatcher = SemiAutoMenu._ensure_workers_started()
            try:
                detail = SemiAutoMenu._wait_full_rerun(md5_list, batch_id) if enqueue_ok > 0 else []
            finally:
                SemiAutoMenu._stop_workers_if_needed(started_downloader, started_dispatcher)

        # 停止后按真实模块状态重算一次，避免汇总出现“进行中”偏差
        for h in md5_list:
            db.recompute_announcement_process_status(h, list(MODULE_NAMES))
        detail = []
        for h in md5_list:
            ann = db.get_announcement(h)
            if ann:
                detail.append((h, ann.download_status.value, ann.process_status.value))
            else:
                detail.append((h, "pending", "pending"))

        dl_fail = [h for h, ds, _ in detail if ds == "failed"]
        pr_fail = [h for h, ds, ps in detail if ds == "success" and ps == "failed"]
        dl_success = sum(1 for _, ds, _ in detail if ds == "success")
        proc_success = sum(1 for _, ds, ps in detail if ds == "success" and ps == "success")
        running = sum(
            1
            for _, ds, ps in detail
            if not (ds == "failed" or (ds == "success" and ps in ("success", "failed")))
        )

        print("\n" + "=" * 60)
        print("按MD5全量重跑完成")
        print("=" * 60)
        print(
            f"[批次 {batch_id}] 总{len(md5_list)} | 下载成功 {dl_success} | 下载失败 {len(dl_fail)} | "
            f"处理成功 {proc_success} | 处理失败 {len(pr_fail)} | 进行中 {running}"
        )
        if dl_fail:
            print(f"  下载失败MD5: {', '.join(dl_fail)}")
        if pr_fail:
            print(f"  处理失败MD5: {', '.join(pr_fail)}")
        SemiAutoMenu._print_module_summary(md5_list)

        logger.info(f"[半自动全量重跑] 批次 {batch_id} 完成")

    @staticmethod
    def _print_announcement_candidates(items: list[Announcement]):
        print("\n可选公告(最多50条)")
        print("-" * 110)
        print(f"{'序号':<6}{'MD5':<34}{'股票':<10}{'简称':<14}{'日期':<12}{'标题'}")
        print("-" * 110)
        for i, ann in enumerate(items, 1):
            title = (ann.title or "").replace("\n", " ").strip()
            if len(title) > 36:
                title = title[:33] + "..."
            print(f"{i:<6}{ann.hashcode:<34}{(ann.gpdm or ''):<10}{(ann.zqjc or ''):<14}{(ann.publish_date or ''):<12}{title}")
        print("-" * 110)

    @staticmethod
    def _parse_index_tokens(tokens: list[str], max_index: int) -> tuple[list[int], list[str]]:
        ok: list[int] = []
        bad: list[str] = []
        seen: set[int] = set()
        for t in tokens:
            if t.isdigit():
                idx = int(t)
                if 1 <= idx <= max_index and idx not in seen:
                    ok.append(idx)
                    seen.add(idx)
                else:
                    bad.append(t)
            else:
                bad.append(t)
        return ok, bad

    @staticmethod
    def _select_announcements_for_reprocess() -> list[Announcement]:
        keyword_tokens = SemiAutoMenu._read_multiline_tokens(
            "可选：输入筛选关键词(MD5/股票代码/简称/标题，支持多行，空行结束；直接空行=默认最近50条):"
        )
        keyword = " ".join(keyword_tokens).strip()
        items = db.get_recent_downloaded_announcements(limit=50, keyword=keyword)
        if not items:
            print("\n[提示] 没有可重处理公告")
            return []

        SemiAutoMenu._print_announcement_candidates(items)
        tokens = SemiAutoMenu._read_multiline_tokens("请输入要重处理的序号或MD5(支持混合，直接空行=全选当前列表):")
        if not tokens:
            return items

        idx_tokens = [t for t in tokens if t.isdigit()]
        md5_tokens = [t for t in tokens if not t.isdigit()]
        idx_ok, idx_bad = SemiAutoMenu._parse_index_tokens(idx_tokens, len(items))
        md5_ok, md5_bad = SemiAutoMenu._parse_md5_list(md5_tokens)
        if idx_bad or md5_bad:
            print(f"\n[警告] 无效输入已忽略: {', '.join(idx_bad + md5_bad)}")

        selected: dict[str, Announcement] = {}
        for idx in idx_ok:
            ann = items[idx - 1]
            selected[ann.hashcode] = ann

        for h in md5_ok:
            ann = db.get_announcement(h)
            if ann and ann.download_status == DownloadStatus.SUCCESS and ann.file_path:
                selected[h] = ann
            else:
                print(f"[提示] MD5不可用或未下载: {h}")

        usable: list[Announcement] = []
        for ann in selected.values():
            if ann.file_path and os.path.exists(ann.file_path):
                usable.append(ann)
            else:
                print(f"[提示] 文件缺失，请先走模式A重新下载: {ann.hashcode}")
        return usable

    @staticmethod
    def _select_modules() -> list[str]:
        modules = list(MODULE_NAMES)
        print("\n业务模块(默认全选):")
        for i, m in enumerate(modules, 1):
            print(f"  {i}. {m}")

        cancel_raw = input("输入要取消的模块序号(逗号分隔，回车=全选): ").strip()
        if not cancel_raw:
            return modules

        tokens = [x.strip() for x in re.split(r"[\s,;，；]+", cancel_raw) if x.strip()]
        idx_ok, idx_bad = SemiAutoMenu._parse_index_tokens(tokens, len(modules))
        if idx_bad:
            print(f"[警告] 无效序号已忽略: {', '.join(idx_bad)}")

        cancel = {i - 1 for i in idx_ok}
        selected = [m for i, m in enumerate(modules) if i not in cancel]
        return selected

    @staticmethod
    def _wait_selected_module_tasks(tasks: list[tuple[str, str]], batch_id: str) -> dict[str, int]:
        terminal = {"success", "no_output", "skipped", "failed"}
        last_snapshot = None
        last_change_time = time.time()
        while True:
            count = {
                "success": 0,
                "no_output": 0,
                "skipped": 0,
                "failed": 0,
                "pending": 0,
                "processing": 0,
            }
            finished = 0
            for h, m in tasks:
                st = db.get_module_status(h, m)
                value = st.value if st else "pending"
                count[value] = count.get(value, 0) + 1
                if value in terminal:
                    finished += 1

            current_snapshot = (
                count["success"],
                count["no_output"],
                count["skipped"],
                count["failed"],
                count["pending"],
                count["processing"],
            )
            if current_snapshot != last_snapshot:
                last_snapshot = current_snapshot
                last_change_time = time.time()

            if finished >= len(tasks):
                return count

            if time.time() - last_change_time > 20:
                queue_stats = queue_manager.get_all_stats()
                idle = (
                    queue_stats["process_queue"]["current_size"] == 0
                    and task_dispatcher.get_active_count() == 0
                )
                if idle:
                    logger.warning(f"[半自动重处理] 批次 {batch_id} 提前结束: 检测到系统空闲但状态无变化")
                    return count
            time.sleep(2)

    @staticmethod
    def _collect_module_name_by_status(hashcode: str, modules: list[str]) -> dict[str, list[str]]:
        names = {
            "success": [],
            "no_output": [],
            "skipped": [],
            "failed": [],
            "pending": [],
            "processing": [],
        }
        for module_name in modules:
            st = db.get_module_status(hashcode, module_name)
            key = st.value if st else "pending"
            if key not in names:
                key = "pending"
            names[key].append(module_name)
        return names

    @staticmethod
    def _print_reprocess_announcement_table(anns: list[Announcement], modules: list[str]):
        print("\n公告处理明细")
        print("-" * 60)
        try:
            from prettytable import PrettyTable

            table = PrettyTable()
            table.field_names = ["股票代码", "信息发布日期", "完成数", "无输出/跳过模块", "失败模块"]
            table.align["股票代码"] = "l"
            table.align["信息发布日期"] = "l"
            table.align["完成数"] = "r"
            table.align["无输出/跳过模块"] = "l"
            table.align["失败模块"] = "l"

            for ann in anns:
                module_names = SemiAutoMenu._collect_module_name_by_status(ann.hashcode, modules)
                no_or_skip = module_names["no_output"] + module_names["skipped"]
                table.add_row(
                    [
                        ann.gpdm or "-",
                        ann.publish_date or "-",
                        len(module_names["success"]),
                        "/".join(no_or_skip) if no_or_skip else "-",
                        "/".join(module_names["failed"]) if module_names["failed"] else "-",
                    ]
                )
            print(table)
        except Exception:
            for ann in anns:
                module_names = SemiAutoMenu._collect_module_name_by_status(ann.hashcode, modules)
                no_or_skip = module_names["no_output"] + module_names["skipped"]
                parts = [f"完成{len(module_names['success'])}模块"]
                if no_or_skip:
                    parts.append(f"无输出/跳过: {'/'.join(no_or_skip)}")
                if module_names["failed"]:
                    parts.append(f"失败: {'/'.join(module_names['failed'])}")
                print(f"  {(ann.gpdm or '-')} | {(ann.publish_date or '-')} | {' | '.join(parts)}")

    @staticmethod
    def _fetch_announcement_by_hash_from_source(hashcode: str) -> Optional[Announcement]:
        """按HASHCODE从源库补齐元数据"""
        sql = """
            SELECT HASHCODE, B.GPDM, B.ZQJC,
                   CONVERT(DATE, A.XXFBRQ) AS XXFBRQ,
                   A.XXBT, A.FBSJ
            FROM [10.101.0.212].JYPRIME.dbo.usrGSGGYWFWB A
            JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B
              ON A.INBBM = B.INBBM
             AND B.ZQSC IN (83, 90, 18)
             AND B.ZQLB IN (1, 2, 41)
            WHERE A.HASHCODE = ?
        """
        try:
            conn = pyodbc.connect(
                SERVER=DB_CONFIG["server"],
                UID=DB_CONFIG["username"],
                PWD=DB_CONFIG["password"],
                DRIVER=DB_CONFIG["driver"],
            )
            try:
                cursor = conn.cursor()
                cursor.execute(sql, (hashcode.upper(),))
                row = cursor.fetchone()
                if not row:
                    logger.warning(f"[半自动全量重跑] 源库未查到HASHCODE: {hashcode}")
                    return None
                return Announcement(
                    hashcode=row.HASHCODE.strip() if row.HASHCODE else hashcode,
                    gpdm=row.GPDM.strip() if row.GPDM else None,
                    zqjc=row.ZQJC.strip() if row.ZQJC else None,
                    publish_date=row.XXFBRQ.strftime("%Y-%m-%d") if row.XXFBRQ else None,
                    title=row.XXBT.strip() if row.XXBT else None,
                    fbsj=row.FBSJ if isinstance(row.FBSJ, datetime) else datetime.now(),
                )
            finally:
                conn.close()
        except Exception as e:
            logger.warning(f"[半自动全量重跑] 源库回查失败 {hashcode}: {e}")
            return None

    @staticmethod
    def run_reprocess_downloaded():
        anns = SemiAutoMenu._select_announcements_for_reprocess()
        if not anns:
            return

        modules = SemiAutoMenu._select_modules()
        if not modules:
            print("\n[提示] 需要至少选择一个模块")
            return

        batch_id = datetime.now().strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]
        logger.info(f"[半自动重处理] 批次 {batch_id} 开始，公告={len(anns)}，模块={len(modules)}")

        tasks: list[tuple[str, str]] = []
        with temporarily_raise_console_level():
            process_queue = queue_manager.get_process_queue()
            for ann in anns:
                db.update_process_status(ann.hashcode, ProcessStatus.PENDING, error="", retry_count=0)
                for m in modules:
                    db.update_module_status(ann.hashcode, m, ProcessStatus.PENDING, error="", retry_count=0, sync=True)
                    task = ProcessTask(hashcode=ann.hashcode, file_path=ann.file_path, module_name=m, retry_count=0)
                    if process_queue.put(task, block=False):
                        tasks.append((ann.hashcode, m))
                    else:
                        logger.warning(f"[半自动重处理] 入队失败 {ann.hashcode}/{m}")

            print(f"\n已提交 {len(tasks)} 个模块任务")
            if not tasks:
                return

            started_downloader, started_dispatcher = SemiAutoMenu._ensure_workers_started()
            try:
                count = SemiAutoMenu._wait_selected_module_tasks(tasks, batch_id)
            finally:
                for h in sorted({ann.hashcode for ann in anns}):
                    db.recompute_announcement_process_status(h, list(MODULE_NAMES))
                SemiAutoMenu._stop_workers_if_needed(started_downloader, started_dispatcher)

        print("\n" + "=" * 60)
        print("已下载公告重处理完成")
        print("=" * 60)
        print(f"  批次ID: {batch_id}")
        print(f"  公告数量: {len(anns)}")
        print(f"  模块数量: {len(modules)}")
        print(f"  任务总数: {len(tasks)}")
        print(f"  成功: {count.get('success', 0)}")
        print(f"  无输出: {count.get('no_output', 0)}")
        print(f"  跳过: {count.get('skipped', 0)}")
        print(f"  失败: {count.get('failed', 0)}")
        print(f"  待处理: {count.get('pending', 0)}")
        print(f"  处理中: {count.get('processing', 0)}")
        SemiAutoMenu._print_reprocess_announcement_table(anns, modules)

        logger.info(f"[半自动重处理] 批次 {batch_id} 完成")


class Application:
    """应用主类"""

    def __init__(self):
        self._running = False
        self._stop_event = threading.Event()
        self._stats_thread: Optional[threading.Thread] = None
        self._last_stats = None

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info(f"收到信号 {signum}，准备停止...")
        self.stop()

    def _check_stop_signal(self) -> bool:
        return os.path.exists(STOP_SIGNAL_FILE)

    def _remove_stop_signal(self):
        try:
            if os.path.exists(STOP_SIGNAL_FILE):
                os.remove(STOP_SIGNAL_FILE)
        except Exception as e:
            logger.error(f"移除停止信号失败: {e}")

    def _print_stats(self):
        while self._running and not self._stop_event.is_set():
            try:
                for _ in range(300):
                    if not self._running or self._stop_event.is_set():
                        break
                    time.sleep(1)
                if not self._running or self._stop_event.is_set():
                    break

                db_stats = db.get_statistics()
                queue_stats = queue_manager.get_all_stats()
                current = {
                    "total": db_stats.get("total", 0),
                    "dl_success": db_stats.get("download_success", 0),
                    "dl_failed": db_stats.get("download_failed", 0),
                    "proc_success": db_stats.get("process_success", 0),
                    "proc_failed": db_stats.get("process_failed", 0),
                    "dl_queue": queue_stats["download_queue"]["current_size"],
                    "proc_queue": queue_stats["process_queue"]["current_size"],
                    "active_dl": downloader.get_active_count(),
                    "active_proc": task_dispatcher.get_active_count(),
                }
                if current == self._last_stats:
                    continue
                self._last_stats = current
                logger.info(
                    "[状态] "
                    f"总 {current['total']} | 下载: ✓{current['dl_success']} ✗{current['dl_failed']} | "
                    f"处理: ✓{current['proc_success']} ✗{current['proc_failed']} | "
                    f"队列 {current['dl_queue']}->{current['proc_queue']} | "
                    f"活跃 {current['active_dl']}->{current['active_proc']}"
                )
            except Exception as e:
                logger.error(f"统计线程异常: {e}")

    def start(self):
        logger.info("=" * 60)
        logger.info("年报公告自动处理系统启动")
        logger.info("=" * 60)

        self._remove_stop_signal()
        self._running = True
        self._stop_event.clear()

        try:
            db.fix_sub_module_consistency()
            monitor.start()
            downloader.start()
            task_dispatcher.start()

            self._stats_thread = threading.Thread(target=self._print_stats, daemon=True)
            self._stats_thread.start()

            logger.info("所有组件已启动，系统运行中...")
            while self._running:
                if self._check_stop_signal():
                    logger.info("检测到停止信号文件")
                    self.stop()
                    break

                if not monitor.is_running() and self._running:
                    logger.warning("监控器异常停止，尝试重启...")
                    monitor.start()
                if not downloader.is_running() and self._running:
                    logger.warning("下载器异常停止，尝试重启...")
                    downloader.start()
                if not task_dispatcher.is_running() and self._running:
                    logger.warning("分发器异常停止，尝试重启...")
                    task_dispatcher.start()

                time.sleep(1)
        except Exception as e:
            logger.error(f"程序异常: {e}")
            self.stop()

    def stop(self):
        if not self._running:
            return

        logger.info("=" * 60)
        logger.info("应用停止中...")
        logger.info("=" * 60)

        self._running = False
        self._stop_event.set()

        monitor.stop()
        downloader.stop()
        task_dispatcher.stop()

        if self._stats_thread and self._stats_thread.is_alive():
            self._stats_thread.join(timeout=5)

        self._remove_stop_signal()

        try:
            s = db.get_statistics()
            logger.info("=" * 60)
            logger.info("最终统计")
            logger.info(f"  总记录数: {s.get('total', 0)}")
            logger.info(f"  下载成功: {s.get('download_success', 0)}")
            logger.info(f"  下载失败: {s.get('download_failed', 0)}")
            logger.info(f"  处理成功: {s.get('process_success', 0)}")
            logger.info(f"  处理失败: {s.get('process_failed', 0)}")
            logger.info("=" * 60)
        except Exception as e:
            logger.error(f"打印最终统计失败: {e}")

        logger.info("应用已停止")


def show_startup_menu() -> bool:
    custom_dir = db.get_system_status(CUSTOM_OUTPUT_DIR_KEY)
    output_dir = custom_dir.strip() if custom_dir and custom_dir.strip() else REPORTS_DIR

    while True:
        if custom_dir is None:
            print("\n" + "=" * 60)
            print("首次运行设置")
            print("=" * 60)
            print(f"默认输出目录: {REPORTS_DIR}")

            user_dir = input("请输入自定义输出目录(直接回车使用默认): ").strip()
            if user_dir:
                user_dir = os.path.abspath(user_dir)
                try:
                    os.makedirs(user_dir, exist_ok=True)
                    for module_name in MODULE_NAMES:
                        os.makedirs(os.path.join(user_dir, module_name), exist_ok=True)
                    db.set_system_status(CUSTOM_OUTPUT_DIR_KEY, user_dir)
                    custom_dir = user_dir
                    output_dir = user_dir
                    print(f"\n[成功] 已设置输出目录: {user_dir}")
                except Exception as e:
                    print(f"\n[警告] 创建目录失败，使用默认目录: {e}")
                    db.set_system_status(CUSTOM_OUTPUT_DIR_KEY, "")
                    custom_dir = ""
                    output_dir = REPORTS_DIR
            else:
                db.set_system_status(CUSTOM_OUTPUT_DIR_KEY, "")
                custom_dir = ""
                output_dir = REPORTS_DIR
                print(f"\n[成功] 使用默认目录: {REPORTS_DIR}")

        print("\n" + "=" * 60)
        print("年报公告自动处理系统")
        print("=" * 60)
        print(f"数据库: {DB_PATH}")
        print(f"输出目录: {output_dir}")
        last_query_time = db.get_last_query_time()
        if last_query_time:
            print(f"上次查询时间: {last_query_time.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print("上次查询时间: 无记录(下次运行将查询最近7天)")
        print("-" * 60)
        print("  1. 运行程序")
        print("  2. 设置")
        print("  3. 半自动补处理")
        print("  0. 退出")
        print("-" * 60)

        choice = input("请选择操作 (0-3): ").strip()
        if choice == "1":
            return True
        if choice == "2":
            SettingsMenu.show_main_menu()
            custom_dir = db.get_system_status(CUSTOM_OUTPUT_DIR_KEY)
            output_dir = custom_dir.strip() if custom_dir and custom_dir.strip() else REPORTS_DIR
            continue
        if choice == "3":
            SemiAutoMenu.show_main_menu()
            custom_dir = db.get_system_status(CUSTOM_OUTPUT_DIR_KEY)
            output_dir = custom_dir.strip() if custom_dir and custom_dir.strip() else REPORTS_DIR
            continue
        if choice == "0":
            return False

        print("\n[错误] 无效选项")


def main():
    if not acquire_lock():
        print("错误: 程序已在运行，请勿重复启动")
        print(f"如确认程序未运行，请手动删除锁文件: {LOCK_FILE}")
        sys.exit(1)

    db.cleanup_zombie_status()

    while True:
        should_run = show_startup_menu()
        if not should_run:
            print("\n再见!")
            break

        app = Application()
        try:
            app.start()
        except KeyboardInterrupt:
            logger.info("收到键盘中断")
            app.stop()
        except Exception as e:
            logger.error(f"程序异常: {e}")
            app.stop()

        print("\n程序已停止，返回主菜单...")


if __name__ == "__main__":
    main()
