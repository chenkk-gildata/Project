"""
申报稿领导人AI处理系统 - 主程序
整合下载、切片处理、AI提取、Excel输出、比对功能
"""
import os
from datetime import datetime
from pathlib import Path

from logger_config import setup_logging, get_logger, get_session_id
from downloader import AnnouncementDownloader
from pdf_processor import PDFProcessor
from data_processor import LeaderDataProcessor
from leader_comparison_processor import LeaderComparisonProcessor
from path_utils import get_base_dir

logger = get_logger(__name__)

BACK_OPTION = "0"


def get_main_menu_choice():
    """获取主菜单选项"""
    print("\n请选择运行模式:")
    print("1. 运行新增数据公告处理")
    print("2. 运行已有数据公告处理")
    print("3. 仅提取-新增")
    print("4. 仅提取比对-已有")
    print("0. 退出程序")

    while True:
        choice = input("\n请输入选项 (0-4): ").strip()
        if choice in ["0", "1", "2", "3", "4"]:
            return choice
        print("无效选项，请重新选择")


def get_query_mode_choice():
    """获取查询方式选择"""
    print("\n请选择查询方式:")
    print("1. 日期查询")
    print("2. MD5查询")
    print("0. 返回上一级")

    while True:
        choice = input("\n请输入选项 (0-2): ").strip()
        if choice in ["0", "1", "2"]:
            return choice
        print("无效选项，请重新选择")


def get_pdf_directory():
    """获取用户指定的PDF目录路径"""
    print("\n请输入包含PDF文件的目录路径:")
    print("输入 0 返回上一级")

    while True:
        dir_path = input("\n目录路径: ").strip()

        if dir_path == BACK_OPTION:
            return None

        dir_path = dir_path.strip('"').strip("'")

        if not os.path.isdir(dir_path):
            print(f"目录不存在: {dir_path}，请重新输入")
            continue

        pdf_files = list(Path(dir_path).glob("*.pdf"))
        if not pdf_files:
            print(f"目录中未找到PDF文件: {dir_path}，请重新输入")
            continue

        print(f"找到 {len(pdf_files)} 个PDF文件")
        return dir_path


def run_download_and_slice(downloader, start_date, end_date, session_id: str, sql_file: str):
    """执行下载和切片的公共流程，返回下载文件夹路径，失败返回None"""
    logger.info(f"用户选择的日期范围: {start_date} ~ {end_date}")
    print(f"\n选择的日期范围: {start_date} ~ {end_date}")

    announcements = downloader.query_announcements(start_date, end_date, sql_file)

    if not announcements:
        print("未查询到符合条件的公告")
        logger.info("未查询到符合条件的公告")
        return None

    print(f"\n查询到 {len(announcements)} 条公告")
    logger.info(f"查询到 {len(announcements)} 条公告")

    download_folder = downloader.create_download_folder(start_date, end_date, session_id)
    print(f"下载文件夹: {download_folder}")
    logger.info(f"下载文件夹: {download_folder}")

    print("\n开始下载公告...")
    downloaded_count, failed_count, failed_files = downloader.download_batch(
        announcements, download_folder
    )

    print(f"\n下载完成! 成功: {downloaded_count}, 失败: {failed_count}")
    logger.info(f"下载完成! 成功: {downloaded_count}, 失败: {failed_count}")

    if failed_files:
        print(f"失败的文件: {failed_files[:5]}...")
        logger.warning(f"失败的文件: {failed_files}")

    pdf_files = list(Path(download_folder).glob("*.pdf"))

    if not pdf_files:
        print("未找到下载的PDF文件")
        logger.warning("未找到下载的PDF文件")
        return None

    print(f"\n找到 {len(pdf_files)} 个PDF文件")
    logger.info(f"找到 {len(pdf_files)} 个PDF文件")

    pdf_processor = PDFProcessor()

    print("\n开始PDF切片处理...")
    success_count, failed_count = pdf_processor.process_batch([str(f) for f in pdf_files])

    logger.info(f"PDF处理完成! 成功: {success_count}, 失败: {failed_count}")

    processed_pdf_files = list(Path(download_folder).glob("*.pdf"))

    if not processed_pdf_files:
        print("未找到处理后的PDF文件")
        logger.warning("未找到处理后的PDF文件")
        return None

    print(f"\n找到 {len(processed_pdf_files)} 个处理后的PDF文件")
    return download_folder


def run_download_and_slice_by_md5(downloader, md5_list, session_id: str, sql_file: str):
    """执行MD5查询的下载和切片流程"""
    logger.info(f"MD5列表: {md5_list}")
    print(f"\n解析到 {len(md5_list)} 个MD5")

    announcements = downloader.query_announcements_by_md5(md5_list, sql_file)

    if not announcements:
        print("未查询到符合条件的公告")
        logger.info("未查询到符合条件的公告")
        return None

    print(f"\n查询到 {len(announcements)} 条公告")
    logger.info(f"查询到 {len(announcements)} 条公告")

    download_folder = downloader.create_download_folder_by_md5(session_id)
    print(f"下载文件夹: {download_folder}")
    logger.info(f"下载文件夹: {download_folder}")

    print("\n开始下载公告...")
    downloaded_count, failed_count, failed_files = downloader.download_batch(
        announcements, download_folder
    )

    print(f"\n下载完成! 成功: {downloaded_count}, 失败: {failed_count}")
    logger.info(f"下载完成! 成功: {downloaded_count}, 失败: {failed_count}")

    if failed_files:
        print(f"失败的文件: {failed_files[:5]}...")
        logger.warning(f"失败的文件: {failed_files}")

    pdf_files = list(Path(download_folder).glob("*.pdf"))

    if not pdf_files:
        print("未找到下载的PDF文件")
        logger.warning("未找到下载的PDF文件")
        return None

    print(f"\n找到 {len(pdf_files)} 个PDF文件")
    logger.info(f"找到 {len(pdf_files)} 个PDF文件")

    pdf_processor = PDFProcessor()

    print("\n开始PDF切片处理...")
    success_count, failed_count = pdf_processor.process_batch([str(f) for f in pdf_files])

    logger.info(f"PDF处理完成! 成功: {success_count}, 失败: {failed_count}")

    processed_pdf_files = list(Path(download_folder).glob("*.pdf"))

    if not processed_pdf_files:
        print("未找到处理后的PDF文件")
        logger.warning("未找到处理后的PDF文件")
        return None

    print(f"\n找到 {len(processed_pdf_files)} 个处理后的PDF文件")
    return download_folder


def run_ai_extraction(download_folder: str, mode: str = "新增", skip_excel_output: bool = False):
    """执行AI提取和Excel输出

    Args:
        download_folder: 下载文件夹路径
        mode: 运行模式，"新增" 或 "比对"
        skip_excel_output: 是否跳过Excel输出（比对模式下只提取不单独输出Excel）
    """
    pdf_files = list(Path(download_folder).glob("*.pdf"))

    if not pdf_files:
        print("未找到PDF文件，无法进行AI提取")
        return []

    data_processor = LeaderDataProcessor()

    print("\n开始AI提取领导人数据...")
    results = data_processor.process_all_files(pdf_files, download_folder, mode, skip_excel_output)

    if results and not skip_excel_output:
        base_dir = get_base_dir()
        output_dir = os.path.join(base_dir, "output", mode)
        print(f"\nExcel文件已输出到: {output_dir}")
        logger.info(f"Excel文件已输出到: {output_dir}")
    elif not results:
        print("\n没有成功提取的文件")
        logger.warning("没有成功提取的文件")

    return results


def _run_comparison(results, session_id: str):
    """执行比对并输出比对结果Excel，供模式2和4复用"""
    if not results:
        print("\n没有成功提取的文件，跳过比对")
        logger.warning("没有成功提取的文件，跳过比对")
        return False

    print("\n开始数据比对...")
    logger.info("开始数据比对...")

    comparison_processor = LeaderComparisonProcessor()
    all_comparison_results = []

    for result in results:
        metadata = result.get('metadata', {})
        extracted_data = result.get('extracted_data', [])
        gpdm = metadata.get('GPDM', '')
        xxfbrq = metadata.get('XXFBRQ', '')

        if not extracted_data:
            continue

        comparison_results = comparison_processor.compare_with_database(
            extracted_data, gpdm, xxfbrq
        )
        all_comparison_results.extend(comparison_results)

    if all_comparison_results:
        base_dir = get_base_dir()
        output_dir = os.path.join(base_dir, "output", "比对")
        comparison_processor.generate_comparison_report(
            all_comparison_results, output_dir, session_id
        )
    else:
        print("\n数据比对完成，所有数据一致！")
        logger.info("数据比对完成，所有数据一致")

    return True


def run_new_data_process(session_id: str):
    """选项1：运行新增数据公告处理

    流程：选择查询方式 → 日期/MD5查询 → 下载 → 切片 → AI提取 → Excel输出
    """
    downloader = AnnouncementDownloader()

    query_mode = get_query_mode_choice()
    if query_mode == "0":
        return False

    if query_mode == "1":
        start_date, end_date = downloader.get_date_range()
        if start_date is None:
            return False
        download_folder = run_download_and_slice(
            downloader, start_date, end_date, session_id, 'daily.sql'
        )
    else:
        md5_list = downloader.get_md5_list()
        if md5_list is None:
            return False
        download_folder = run_download_and_slice_by_md5(
            downloader, md5_list, session_id, 'daily.sql'
        )

    if download_folder is None:
        return False

    run_ai_extraction(download_folder, "新增")

    return True


def run_existing_data_process(session_id: str):
    """选项2：运行已有数据公告处理

    流程：选择查询方式 → 日期/MD5查询 → 下载 → 切片 → AI提取 → 比对 → 比对结果Excel
    """
    downloader = AnnouncementDownloader()

    query_mode = get_query_mode_choice()
    if query_mode == "0":
        return False

    if query_mode == "1":
        start_date, end_date = downloader.get_date_range()
        if start_date is None:
            return False
        download_folder = run_download_and_slice(
            downloader, start_date, end_date, session_id, 'compare.sql'
        )
    else:
        md5_list = downloader.get_md5_list()
        if md5_list is None:
            return False
        download_folder = run_download_and_slice_by_md5(
            downloader, md5_list, session_id, 'compare.sql'
        )

    if download_folder is None:
        return False

    results = run_ai_extraction(download_folder, "比对", skip_excel_output=True)

    _run_comparison(results, session_id)

    return True


def run_extract_only_new(session_id: str):
    """选项3：仅提取-新增

    流程：询问目录 → AI提取 → Excel输出（新增）
    """
    pdf_dir = get_pdf_directory()
    if pdf_dir is None:
        return False

    run_ai_extraction(pdf_dir, "新增")

    return True


def run_extract_only_compare(session_id: str):
    """选项4：仅提取比对-已有

    流程：询问目录 → AI提取 → 比对 → 比对结果Excel
    """
    pdf_dir = get_pdf_directory()
    if pdf_dir is None:
        return False

    results = run_ai_extraction(pdf_dir, "比对", skip_excel_output=True)

    _run_comparison(results, session_id)

    return True


def main():
    """主函数"""
    print("=" * 60)
    print("申报稿领导人AI处理系统")
    print("=" * 60)

    setup_logging()

    session_id = get_session_id()
    logger.info(f"程序启动，会话ID: {session_id}")

    try:
        while True:
            try:
                choice = get_main_menu_choice()

                if choice == "0":
                    print("\n感谢使用，再见!")
                    return
                elif choice == "1":
                    logger.info("用户选择: 运行新增数据公告处理")
                    run_new_data_process(session_id)
                elif choice == "2":
                    logger.info("用户选择: 运行已有数据公告处理")
                    run_existing_data_process(session_id)
                elif choice == "3":
                    logger.info("用户选择: 仅提取-新增")
                    run_extract_only_new(session_id)
                elif choice == "4":
                    logger.info("用户选择: 仅提取比对-已有")
                    run_extract_only_compare(session_id)

            except Exception as e:
                logger.error(f"处理过程中发生错误: {e}", exc_info=True)
                print(f"\n处理出错: {e}")
                print("请重新选择\n")
                continue

    except KeyboardInterrupt:
        print("\n用户中断操作")
        logger.info("用户中断操作")


if __name__ == "__main__":
    main()
