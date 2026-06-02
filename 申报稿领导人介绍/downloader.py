"""
公告下载模块 - 支持批量并发下载和日期处理
"""
import json
import os
import re
import requests
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
import logging
from pathlib import Path

from config import download_config
from database_manager import db_manager
from path_utils import get_files_dir, get_base_dir

logger = logging.getLogger(__name__)


class AnnouncementDownloader:
    """公告下载器"""

    def __init__(self):
        self.is_downloading = False
        self.lock = threading.Lock()
        self.downloaded_count = 0
        self.failed_count = 0
        self.failed_files = []
        self.file_metadata = {}

    def get_date_range(self) -> Tuple[Any, Any]:
        """获取用户输入的日期范围，返回None表示返回上一级"""
        print("\n请输入查询日期范围:")
        print("格式: YYYY-MM-DD~YYYY-MM-DD 或 YYYY-MM-DD（单日）")
        print("输入 0 返回上一级")

        while True:
            date_input = input("\n请输入日期: ").strip()

            if date_input == "0":
                return None, None

            if '~' in date_input:
                try:
                    start_str, end_str = date_input.split('~')
                    start_date = datetime.strptime(start_str.strip(), '%Y-%m-%d').date()
                    end_date = datetime.strptime(end_str.strip(), '%Y-%m-%d').date()

                    if start_date > end_date:
                        print("错误: 开始日期不能大于结束日期，请重新输入")
                        continue

                    return start_date, end_date
                except ValueError:
                    print("错误: 日期格式不正确，请使用 YYYY-MM-DD 格式")
                    continue
            else:
                try:
                    single_date = datetime.strptime(date_input, '%Y-%m-%d').date()
                    return single_date, single_date
                except ValueError:
                    print("错误: 日期格式不正确，请使用 YYYY-MM-DD 格式")
                    continue

    def get_md5_list(self) -> List[str]:
        """获取用户输入的MD5列表"""
        print("\n请输入MD5列表（支持逗号、空格、换行分隔）:")
        print("输入完成后按回车，再输入空行结束")
        print("输入 0 返回上一级")

        while True:
            print("\nMD5列表: ")
            lines = []
            while True:
                line = input()
                if line.strip() == "0" and not lines:
                    return None
                if line.strip() == "":
                    break
                lines.append(line)
            
            md5_input = "\n".join(lines)
            
            md5_list = re.split(r'[,\s\n]+', md5_input)
            md5_list = [md5.strip() for md5 in md5_list if md5.strip()]

            if not md5_list:
                print("未输入有效的MD5，请重新输入")
                continue

            md5_list = list(dict.fromkeys(md5_list))

            print(f"\n解析到 {len(md5_list)} 个MD5")
            confirm = input("确认查询? (Y/n): ").strip().lower()
            if confirm in ['', 'y', 'yes']:
                return md5_list

    def load_sql_from_file(self, sql_file: str) -> str:
        """从外部文件加载SQL语句"""
        base_dir = get_base_dir()
        sql_path = os.path.join(base_dir, sql_file)
        encodings = ['utf-8', 'gbk', 'gb2312', 'utf-16']
        for encoding in encodings:
            try:
                with open(sql_path, 'r', encoding=encoding) as f:
                    return f.read()
            except UnicodeDecodeError:
                continue
        raise UnicodeDecodeError(f"无法解码SQL文件: {sql_path}，尝试的编码: {encodings}")

    def _inject_date_condition(self, sql_content: str, start_date, end_date) -> str:
        """动态替换SQL中的日期条件"""
        pattern = r"AND\s+A\.XXFBRQ\s*>\s*CONVERT\s*\(\s*DATE\s*,\s*GETDATE\s*\(\s*\)\s*-\s*30\s*\)"

        start_str = start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else str(start_date)
        end_str = end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else str(end_date)

        if start_str == end_str:
            replacement = f"AND A.XXFBRQ = '{start_str}'"
        else:
            replacement = f"AND A.XXFBRQ BETWEEN '{start_str}' AND '{end_str}'"

        result = re.sub(pattern, replacement, sql_content, flags=re.IGNORECASE)
        logger.info(f"SQL日期条件: {replacement}")
        return result

    def _inject_md5_condition(self, sql_content: str, md5_list: List[str]) -> str:
        """MD5查询：移除日期限制和EXISTS条件，添加MD5条件"""
        date_pattern = r"AND\s+A\.XXFBRQ\s*>\s*CONVERT\s*\(\s*DATE\s*,\s*GETDATE\s*\(\s*\)\s*-\s*30\s*\)"
        sql_content = re.sub(date_pattern, "", sql_content, flags=re.IGNORECASE)

        exists_pattern = r"AND\s+(NOT\s+)?EXISTS\s*\(\s*SELECT\s+1\s+FROM\s+\[10\.101\.0\.212\]\.JYPRIME\.dbo\.usrGSZYLDRJS\s+X\s+WHERE\s+X\.IGSDM\s*=\s*A\.IGSDM\s*\)"
        sql_content = re.sub(exists_pattern, "", sql_content, flags=re.IGNORECASE)

        md5_values = ", ".join(f"'{md5}'" for md5 in md5_list)
        md5_condition = f"AND HASHCODE IN ({md5_values})"

        sql_content = sql_content.rstrip()
        if sql_content.endswith(';'):
            sql_content = sql_content[:-1]
        sql_content = sql_content + "\n" + md5_condition

        logger.info(f"SQL MD5条件: HASHCODE IN ({len(md5_list)} values)")
        return sql_content

    def query_announcements(self, start_date, end_date, sql_file: str = 'daily.sql') -> List[Dict[str, Any]]:
        """查询需要下载的公告"""
        sql_content = self.load_sql_from_file(sql_file)
        sql_content = self._inject_date_condition(sql_content, start_date, end_date)

        try:
            results = db_manager.execute_query(sql_content)
            logger.info(f"查询到 {len(results)} 条公告记录 (SQL: {sql_file})")
            return results
        except Exception as e:
            logger.error(f"查询公告失败: {e}")
            raise

    def query_announcements_by_md5(self, md5_list: List[str], sql_file: str = 'daily.sql') -> List[Dict[str, Any]]:
        """按MD5查询公告"""
        sql_content = self.load_sql_from_file(sql_file)
        sql_content = self._inject_md5_condition(sql_content, md5_list)

        try:
            results = db_manager.execute_query(sql_content)
            logger.info(f"MD5查询到 {len(results)} 条公告记录 (SQL: {sql_file})")
            return results
        except Exception as e:
            logger.error(f"MD5查询公告失败: {e}")
            raise

    def create_download_folder(self, start_date, end_date, session_id: str = None) -> str:
        """创建下载文件夹"""
        files_dir = get_files_dir()

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if start_date == end_date:
            folder_name = f"{start_date.strftime('%Y%m%d')}_{timestamp}"
        else:
            folder_name = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}_{timestamp}"

        download_folder = os.path.join(files_dir, folder_name)
        if not os.path.exists(download_folder):
            os.makedirs(download_folder)

        logger.info(f"创建下载文件夹: {download_folder}")
        return download_folder

    def create_download_folder_by_md5(self, session_id: str = None) -> str:
        """为MD5查询创建下载文件夹"""
        files_dir = get_files_dir()

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        folder_name = f"md5_{timestamp}"
        download_folder = os.path.join(files_dir, folder_name)

        if not os.path.exists(download_folder):
            os.makedirs(download_folder)

        logger.info(f"创建MD5下载文件夹: {download_folder}")
        return download_folder

    def calculate_dynamic_workers(self, task_count: int) -> int:
        """根据任务量动态计算并发线程数"""
        if task_count <= 5:
            return min(3, task_count)
        elif task_count <= 20:
            return 5
        elif task_count <= 50:
            return 8
        else:
            return min(download_config.max_download_workers, 10)

    def download_single_file(self, announcement: Dict[str, Any], save_path: str) -> bool:
        """下载单个公告文件"""
        if not self.is_downloading:
            return False

        try:
            hashcode = announcement.get('HASHCODE', '')

            sql_query = '''
            SELECT C.GPDM, CONVERT(DATE, A.XXFBRQ) XXFBRQ, A.XXBT, B.MS, A.HASHCODE, CONVERT(VARCHAR,C.ZQSC)+CONVERT(VARCHAR,C.SSBZ) ZQBZ
            FROM [10.101.0.212].JYPRIME.dbo.usrGSGGYWFWB A
            JOIN [10.101.0.212].JYPRIME.dbo.usrXTCLB B
                ON A.WJGS = B.DM AND B.LB = '1309'
            JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB C
                ON C.INBBM = A.INBBM AND C.ZQSC IN (83, 90, 18) AND C.ZQLB IN (1, 2, 41)
            WHERE A.HASHCODE = ?
            '''

            file_info = db_manager.execute_query(sql_query, (hashcode,))

            if not file_info:
                logger.warning(f"未找到HASHCODE {hashcode} 对应的文件信息")
                return False

            file_data = file_info[0]
            download_url = download_config.download_url_template.format(appId=file_data['HASHCODE'])

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            response = requests.get(
                download_url,
                headers=headers,
                timeout=download_config.download_timeout,
                stream=True
            )

            hz = str(file_data.get('MS', 'pdf'))
            rq = str(file_data.get('XXFBRQ', ''))
            bt = file_data.get('XXBT', '')
            gpdm = file_data.get('GPDM', '')
            zqbz = file_data.get('ZQBZ', '')

            filename = f"{gpdm}-{rq}-{bt}-{zqbz}.{hz}"
            filename = re.sub(r'[\\/*?:"<>|]', '', filename)
            file_path = os.path.join(save_path, filename)

            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=download_config.chunk_size):
                    if not self.is_downloading:
                        return False
                    if chunk:
                        f.write(chunk)

            if self.is_downloading:
                with self.lock:
                    self.downloaded_count += 1
                    self.file_metadata[filename] = {
                        'ZQJC': announcement.get('ZQJC', ''),
                        'GPDM': gpdm,
                        'XXFBRQ': rq,
                        'XXBT': bt,
                        'HASHCODE': hashcode
                    }
                logger.info(f"下载成功: {filename}")
                return True

        except Exception as e:
            if self.is_downloading:
                with self.lock:
                    self.failed_count += 1
                    self.failed_files.append(announcement.get('HASHCODE', '未知'))
                logger.error(f"下载失败: {announcement.get('HASHCODE', '未知')} - {str(e)}")
            return False

    def download_batch(self, announcements: List[Dict[str, Any]], save_path: str) -> Tuple[int, int, List[str]]:
        """批量下载公告"""
        if not announcements:
            return 0, 0, []

        self.is_downloading = True
        self.downloaded_count = 0
        self.failed_count = 0
        self.failed_files = []
        self.file_metadata = {}

        total_count = len(announcements)
        workers = self.calculate_dynamic_workers(total_count)

        logger.info(f"开始批量下载 {total_count} 个公告，使用 {workers} 个并发线程")
        print(f"\n开始下载 {total_count} 个公告，使用 {workers} 个并发线程...")

        executor = ThreadPoolExecutor(max_workers=workers)

        try:
            futures = {
                executor.submit(self.download_single_file, announcement, save_path): announcement
                for announcement in announcements
            }

            completed = 0
            for future in as_completed(futures):
                if not self.is_downloading:
                    break

                completed += 1

        except Exception as e:
            logger.error(f"批量下载过程中发生错误: {e}")
        finally:
            executor.shutdown(wait=True)
            self.is_downloading = False

        try:
            metadata_path = os.path.join(save_path, 'metadata.json')
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(self.file_metadata, f, ensure_ascii=False, indent=2)
            logger.info(f"元数据已保存: {metadata_path}")
        except Exception as e:
            logger.error(f"保存元数据失败: {e}")

        return self.downloaded_count, self.failed_count, self.failed_files

    def stop_download(self):
        """停止下载"""
        self.is_downloading = False
        logger.info("用户请求停止下载")
