"""
主要指标一季报AI比对系统 - 主程序
整合下载、切片处理、比对输出功能
"""
import os
import sys
from datetime import datetime
from pathlib import Path

from logger_config import setup_logging, get_logger, get_session_id
from downloader import AnnouncementDownloader
from pdf_processor import PDFProcessor
from comparison_processor import ComparisonProcessor

logger = get_logger(__name__)


def main():
    """主函数"""
    print("=" * 60)
    print("主要指标一季报AI比对系统")
    print("=" * 60)

    setup_logging()
    
    session_id = get_session_id()
    logger.info(f"程序启动，会话ID: {session_id}")

    try:
        while True:
            try:
                downloader = AnnouncementDownloader()
                
                start_date, end_date = downloader.get_date_choice()
                
                logger.info(f"用户选择的日期范围: {start_date} ~ {end_date}")
                print(f"\n选择的日期范围: {start_date} ~ {end_date}")
                
                announcements = downloader.query_announcements(start_date, end_date)
                
                if not announcements:
                    print("未查询到符合条件的公告，请重新选择日期")
                    logger.info("未查询到符合条件的公告")
                    continue
                
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
                    print("未找到下载的PDF文件，请重新选择日期")
                    logger.warning("未找到下载的PDF文件")
                    continue
                
                print(f"\n找到 {len(pdf_files)} 个PDF文件")
                logger.info(f"找到 {len(pdf_files)} 个PDF文件")
                
                pdf_processor = PDFProcessor()
                
                print("\n开始PDF切片处理...")
                success_count, failed_count = pdf_processor.process_batch([str(f) for f in pdf_files])
                
                print(f"\nPDF处理完成! 成功: {success_count}, 失败: {failed_count}")
                logger.info(f"PDF处理完成! 成功: {success_count}, 失败: {failed_count}")
                
                processed_pdf_files = list(Path(download_folder).glob("*.pdf"))
                
                if not processed_pdf_files:
                    print("未找到处理后的PDF文件，请重新选择日期")
                    logger.warning("未找到处理后的PDF文件")
                    continue
                
                print(f"\n找到 {len(processed_pdf_files)} 个处理后的PDF文件")
                logger.info(f"找到 {len(processed_pdf_files)} 个处理后的PDF文件")
                
                comparison_processor = ComparisonProcessor()
                
                print("\n开始AI比对处理...")
                logger.info("开始AI比对处理...")
                
                program_start_time = datetime.now()
                results = comparison_processor.process_all_files(processed_pdf_files)
                program_end_time = datetime.now()
                
                duration = program_end_time - program_start_time
                
                print(f"\n比对处理完成! 共处理 {len(results)}/{len(processed_pdf_files)} 个文件，耗时: {duration}")
                logger.info(f"比对处理完成! 共处理 {len(results)}/{len(processed_pdf_files)} 个文件，耗时: {duration}")
                
                if results:
                    print("\n生成比对报告...")
                    report_file = comparison_processor.generate_report(results)
                    
                    if report_file:
                        logger.info(f"报告已生成: {report_file}")
                    else:
                        logger.error("报告生成失败")
                else:
                    print("\n没有成功处理的文件，无法生成报告")
                    logger.warning("没有成功处理的文件，无法生成报告")
                
                print("\n" + "=" * 60)
                print("程序执行完成!")
                print("=" * 60)
                logger.info("程序执行完成")
                break
                
            except Exception as e:
                logger.error(f"处理过程中发生错误: {e}", exc_info=True)
                print(f"\n处理出错: {e}")
                print("请重新选择日期\n")
                continue

    except KeyboardInterrupt:
        print("\n用户中断操作")
        logger.info("用户中断操作")


if __name__ == "__main__":
    main()
