"""
主要股东背景介绍一季报 AI 比对系统主入口。
"""
import os
from datetime import datetime
from pathlib import Path

from comparison_processor import ComparisonProcessor
from downloader import AnnouncementDownloader
from logger_config import get_logger, get_session_id, setup_logging
from path_utils import get_files_dir
from pdf_processor import PDFProcessor

logger = get_logger(__name__)

BACK_OPTION = "0"


def get_main_menu_choice():
    """获取主菜单选项。"""
    print("\n请选择运行模式:")
    print("1. 完整流程（下载 + 切片 + 比对）")
    print("2. 仅下载和切片（不进行比对）")
    print("3. 仅运行比对（对已处理的公告进行比对）")
    print("0. 退出程序")

    while True:
        choice = input("\n请输入选项 (0-3): ").strip()
        if choice in ["0", "1", "2", "3"]:
            return choice
        print("无效选项，请重新选择")


def get_comparison_source_choice():
    """获取比对数据源选项。"""
    print("\n请选择比对数据源:")
    print("1. 已处理公告（从 files 文件夹选择）")
    print("2. 自定义路径")
    print("0. 返回上一级")

    while True:
        choice = input("\n请输入选项 (0-2): ").strip()
        if choice in ["0", "1", "2"]:
            return choice
        print("无效选项，请重新选择")


def get_processed_folder():
    """从 files 目录中选择已处理的 PDF 文件夹。"""
    files_dir = get_files_dir()

    if not os.path.exists(files_dir):
        print(f"files 文件夹不存在: {files_dir}")
        return None

    folders = []
    for item in os.listdir(files_dir):
        item_path = os.path.join(files_dir, item)
        if os.path.isdir(item_path):
            folders.append((item, item_path, os.path.getmtime(item_path)))

    if not folders:
        print("files 文件夹下没有子文件夹")
        return None

    folders.sort(key=lambda item: item[2], reverse=True)

    print("\n已处理的公告文件夹（按时间倒序）")
    print("-" * 60)
    for idx, (name, path, mtime) in enumerate(folders, 1):
        mtime_str = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
        pdf_count = len(list(Path(path).glob("*.pdf")))
        print(f"{idx}. {name} (修改时间: {mtime_str}, PDF 文件数: {pdf_count})")
    print("0. 返回上一级")
    print("-" * 60)

    while True:
        choice = input(f"\n请选择文件夹 (0-{len(folders)}): ").strip()
        if choice == BACK_OPTION:
            return None

        try:
            index = int(choice) - 1
        except ValueError:
            print("请输入有效的数字")
            continue

        if 0 <= index < len(folders):
            return folders[index][1]

        print(f"请输入 0-{len(folders)} 之间的数字")


def get_custom_path():
    """获取用户输入的自定义文件夹路径。"""
    print("\n提示：输入 0 返回上一级")

    while True:
        path = input("\n请输入 PDF 文件所在路径: ").strip().strip('"').strip("'")
        if path == BACK_OPTION:
            return None

        if not os.path.exists(path):
            print(f"路径不存在: {path}")
            continue

        if not os.path.isdir(path):
            print(f"路径不是文件夹: {path}")
            continue

        pdf_files = list(Path(path).glob("*.pdf"))
        if not pdf_files:
            print(f"该文件夹下没有 PDF 文件: {path}")
            continue

        print(f"找到 {len(pdf_files)} 个 PDF 文件")
        return path


def run_full_process(session_id: str):
    """运行完整流程：下载 + 切片 + 比对。"""
    downloader = AnnouncementDownloader()
    start_date, end_date = downloader.get_date_choice()
    if start_date is None:
        return False

    logger.info("用户选择的日期范围: %s ~ %s", start_date, end_date)
    print(f"\n选择的日期范围: {start_date} ~ {end_date}")

    announcements = downloader.query_announcements(start_date, end_date)
    if not announcements:
        print("未查询到符合条件的公告")
        logger.info("未查询到符合条件的公告")
        return False

    print(f"\n查询到 {len(announcements)} 条公告")
    logger.info("查询到 %s 条公告", len(announcements))

    download_folder = downloader.create_download_folder(start_date, end_date, session_id)
    print(f"下载文件夹: {download_folder}")
    logger.info("下载文件夹: %s", download_folder)

    print("\n开始下载公告...")
    downloaded_count, failed_count, failed_files = downloader.download_batch(
        announcements, download_folder
    )
    print(f"\n下载完成! 成功: {downloaded_count}, 失败: {failed_count}")
    logger.info("下载完成! 成功: %s, 失败: %s", downloaded_count, failed_count)

    if failed_files:
        print(f"失败的文件: {failed_files[:5]}...")
        logger.warning("失败的文件: %s", failed_files)

    pdf_files = list(Path(download_folder).glob("*.pdf"))
    if not pdf_files:
        print("未找到下载的 PDF 文件")
        logger.warning("未找到下载的 PDF 文件")
        return False

    print(f"\n找到 {len(pdf_files)} 个 PDF 文件")
    logger.info("找到 %s 个 PDF 文件", len(pdf_files))

    pdf_processor = PDFProcessor()
    print("\n开始 PDF 切片处理...")
    success_count, failed_count = pdf_processor.process_batch([str(item) for item in pdf_files])
    logger.info("PDF 处理完成! 成功: %s, 失败: %s", success_count, failed_count)

    processed_pdf_files = list(Path(download_folder).glob("*.pdf"))
    if not processed_pdf_files:
        print("未找到处理后的 PDF 文件")
        logger.warning("未找到处理后的 PDF 文件")
        return False

    print(f"\n找到 {len(processed_pdf_files)} 个处理后的 PDF 文件")
    logger.info("找到 %s 个处理后的 PDF 文件", len(processed_pdf_files))
    run_comparison(processed_pdf_files)
    return True


def run_download_and_slice_only(session_id: str):
    """仅运行下载和切片。"""
    downloader = AnnouncementDownloader()
    start_date, end_date = downloader.get_date_choice()
    if start_date is None:
        return False

    logger.info("用户选择的日期范围: %s ~ %s", start_date, end_date)
    print(f"\n选择的日期范围: {start_date} ~ {end_date}")

    announcements = downloader.query_announcements(start_date, end_date)
    if not announcements:
        print("未查询到符合条件的公告")
        logger.info("未查询到符合条件的公告")
        return False

    print(f"\n查询到 {len(announcements)} 条公告")
    logger.info("查询到 %s 条公告", len(announcements))

    download_folder = downloader.create_download_folder(start_date, end_date, session_id)
    print(f"下载文件夹: {download_folder}")
    logger.info("下载文件夹: %s", download_folder)

    print("\n开始下载公告...")
    downloaded_count, failed_count, failed_files = downloader.download_batch(
        announcements, download_folder
    )
    print(f"\n下载完成! 成功: {downloaded_count}, 失败: {failed_count}")
    logger.info("下载完成! 成功: %s, 失败: %s", downloaded_count, failed_count)

    if failed_files:
        print(f"失败的文件: {failed_files[:5]}...")
        logger.warning("失败的文件: %s", failed_files)

    pdf_files = list(Path(download_folder).glob("*.pdf"))
    if not pdf_files:
        print("未找到下载的 PDF 文件")
        logger.warning("未找到下载的 PDF 文件")
        return False

    print(f"\n找到 {len(pdf_files)} 个 PDF 文件")
    logger.info("找到 %s 个 PDF 文件", len(pdf_files))

    pdf_processor = PDFProcessor()
    print("\n开始 PDF 切片处理...")
    success_count, failed_count = pdf_processor.process_batch([str(item) for item in pdf_files])
    logger.info("PDF 处理完成! 成功: %s, 失败: %s", success_count, failed_count)
    return True


def run_comparison_only():
    """仅运行比对。"""
    while True:
        source_choice = get_comparison_source_choice()
        if source_choice == "0":
            return False

        folder_path = get_processed_folder() if source_choice == "1" else get_custom_path()
        if folder_path is None:
            continue

        pdf_files = list(Path(folder_path).glob("*.pdf"))
        if not pdf_files:
            print(f"未找到 PDF 文件: {folder_path}")
            continue

        print(f"\n找到 {len(pdf_files)} 个 PDF 文件")
        logger.info("找到 %s 个 PDF 文件", len(pdf_files))
        run_comparison(pdf_files)
        return True


def run_comparison(pdf_files):
    """执行比对流程。"""
    comparison_processor = ComparisonProcessor()
    print("\n开始 AI 比对处理...")
    logger.info("开始 AI 比对处理...")

    start_time = datetime.now()
    results = comparison_processor.process_all_files(pdf_files)
    end_time = datetime.now()

    duration = end_time - start_time
    print(f"\n比对处理完成! 共处理 {len(results)}/{len(pdf_files)} 个文件，耗时: {duration}")
    logger.info("比对处理完成! 共处理 %s/%s 个文件，耗时: %s", len(results), len(pdf_files), duration)

    if not results:
        print("\n没有成功处理的文件，无法生成报告")
        logger.warning("没有成功处理的文件，无法生成报告")
        return

    print("\n生成比对报告...")
    report_file = comparison_processor.generate_report(results)
    if report_file:
        logger.info("报告已生成: %s", report_file)
    else:
        logger.error("报告生成失败")


def main():
    """程序入口。"""
    print("=" * 60)
    print("主要股东背景介绍一季报 AI 比对系统")
    print("=" * 60)

    setup_logging()
    session_id = get_session_id()
    logger.info("程序启动，会话 ID: %s", session_id)

    try:
        while True:
            try:
                choice = get_main_menu_choice()
                if choice == "0":
                    print("\n感谢使用，再见")
                    return
                if choice == "1":
                    logger.info("用户选择: 完整流程")
                    run_full_process(session_id)
                elif choice == "2":
                    logger.info("用户选择: 仅下载和切片")
                    run_download_and_slice_only(session_id)
                elif choice == "3":
                    logger.info("用户选择: 仅运行比对")
                    run_comparison_only()
            except EOFError:
                logger.info("输入结束，程序退出")
                return
            except Exception as exc:
                logger.error("处理过程中发生错误: %s", exc, exc_info=True)
                print(f"\n处理出错: {exc}")
                print("请重新选择\n")
    except KeyboardInterrupt:
        print("\n用户中断操作")
        logger.info("用户中断操作")


if __name__ == "__main__":
    main()
