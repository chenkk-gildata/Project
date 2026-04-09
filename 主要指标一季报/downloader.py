"""
公告下载模块 - 支持批量并发下载和日期处理
"""
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
from path_utils import get_files_dir

logger = logging.getLogger(__name__)


class AnnouncementDownloader:
    """公告下载器"""

    def __init__(self):
        self.is_downloading = False
        self.lock = threading.Lock()
        self.downloaded_count = 0
        self.failed_count = 0
        self.failed_files = []

    def get_date_choice(self) -> Tuple[datetime, datetime]:
        """获取用户选择的日期范围，返回None表示返回上一级"""
        print("\n请选择下载方式:")
        print("1. 下载当天的公告")
        print("2. 自定义下载日期范围")
        print("0. 返回上一级")

        while True:
            choice = input("\n请输入选项 (0-2): ").strip()
            
            if choice == "0":
                return None, None
            elif choice == "1":
                today = datetime.now().date()
                return today, today
            elif choice == "2":
                while True:
                    date_input = input("请输入日期范围 (格式: YYYY-MM-DD~YYYY-MM-DD 或 YYYY-MM-DD，输入0返回): ").strip()
                    
                    if date_input == "0":
                        break
                    
                    if '~' in date_input:
                        try:
                            start_str, end_str = date_input.split('~')
                            start_date = datetime.strptime(start_str.strip(), '%Y-%m-%d').date()
                            end_date = datetime.strptime(end_str.strip(), '%Y-%m-%d').date()
                            
                            if start_date > end_date:
                                print("错误: 开始日期不能大于结束日期")
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
            else:
                print("无效选项，请重新选择")

    def build_date_query_conditions(self, start_date, end_date) -> Tuple[str, str]:
        """构建日期查询条件，处理XXLY!=410007600时XXFBRQ+1天的逻辑"""
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        start_plus_one = (start_date + timedelta(days=1)).strftime('%Y-%m-%d')
        end_plus_one = (end_date + timedelta(days=1)).strftime('%Y-%m-%d')
        
        condition_xxly_410007600 = f"A.XXLY = 410007600 AND A.XXFBRQ BETWEEN '{start_str}' AND '{end_str}'"
        condition_xxly_other = f"A.XXLY != 410007600 AND A.XXFBRQ BETWEEN '{start_plus_one}' AND '{end_plus_one}'"
        
        date_condition = f"({condition_xxly_410007600} OR {condition_xxly_other})"
        
        return date_condition, start_str, end_str

    def query_announcements(self, start_date, end_date) -> List[Dict[str, Any]]:
        """查询需要下载的公告"""
        date_condition, start_str, end_str = self.build_date_query_conditions(start_date, end_date)
        
        sql_query = f'''
        SELECT HASHCODE
             , B.GPDM
             , B.ZQJC
             , CONVERT(DATE, A.XXFBRQ) AS XXFBRQ
             , A.XXBT
             , A.FBSJ
        FROM [10.101.0.212].JYPRIME.dbo.usrGSGGYWFWB A
             JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B 
                ON A.INBBM = B.INBBM 
                AND B.ZQSC IN (83, 90, 18) 
                AND B.ZQLB IN (1, 2, 41)
        WHERE A.XXLB = 20 
          AND A.NRLB = 17 
          AND A.XXLY IN (69, 70, 410007600) 
          AND A.MTCC IN ('上海证券交易所','深圳证券交易所','北京证券交易所')
          AND {date_condition}
          AND A.XXBT NOT LIKE '%英文%' AND A.XXBT NOT LIKE '%网页已%' 
          AND A.XXBT NOT LIKE '%延期%' AND A.XXBT NOT LIKE '%披露%' 
          AND A.XXBT NOT LIKE '%附件%' AND A.XXBT NOT LIKE '%审计%'
          AND A.XXBT NOT LIKE '%办法%' AND A.XXBT NOT LIKE '%H%'
          AND A.XXBT NOT LIKE '%更正%' AND A.XXBT NOT LIKE '%修订%'
          AND A.XXBT NOT LIKE '%更新%' AND A.XXBT NOT LIKE '%修正%'
          AND B.GPDM NOT LIKE '%X%'
        '''
        
        try:
            results = db_manager.execute_query(sql_query)
            logger.info(f"查询到 {len(results)} 条公告记录")
            return results
        except Exception as e:
            logger.error(f"查询公告失败: {e}")
            raise

    def create_download_folder(self, start_date, end_date, session_id: str = None) -> str:
        """创建下载文件夹"""
        files_dir = get_files_dir()
        
        if session_id is None:
            timestamp = datetime.now().strftime('%H%M%S')
        else:
            timestamp = session_id
        
        if start_date == end_date:
            folder_name = f"{start_date.strftime('%Y%m%d')}_{timestamp}"
        else:
            folder_name = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}_{timestamp}"
        
        download_folder = os.path.join(files_dir, folder_name)
        if not os.path.exists(download_folder):
            os.makedirs(download_folder)
        
        logger.info(f"创建下载文件夹: {download_folder}")
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
            SELECT C.GPDM, CONVERT(DATE, A.XXFBRQ) XXFBRQ, A.XXBT, B.MS, A.HASHCODE
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
            
            filename = f"{gpdm}-{rq}-{bt}.{hz}"
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
                if completed % 10 == 0 or completed == total_count:
                    print(f"下载进度: {completed}/{total_count} (成功: {self.downloaded_count}, 失败: {self.failed_count})")

        except Exception as e:
            logger.error(f"批量下载过程中发生错误: {e}")
        finally:
            executor.shutdown(wait=True)
            self.is_downloading = False

        return self.downloaded_count, self.failed_count, self.failed_files

    def stop_download(self):
        """停止下载"""
        self.is_downloading = False
        logger.info("用户请求停止下载")
