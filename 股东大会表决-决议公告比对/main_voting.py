"""
股东大会决议公告AI比对系统 - 优化版本V2
解决速率限制、JSON解析错误和性能问题
"""
import concurrent.futures
import json
import os
import re
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd

# 导入优化后的模块
from config import processing_config, validate_config
from database_manager import db_manager
from ai_service_enhanced import enhanced_ai_service
from logger_config import setup_logging, setup_file_logging, get_logger, get_session_id
from mappings_voting import BASIC_MAPPING, PROPOSAL_VOTING_MAPPING
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

import cninfo_Crawling_usrGDDHBJ as cj

# SQL查询语句
SQL_QUERY_BJ = '''
SELECT A.ID,B.GPDM,A.SCXXFBRQ,A.XXFBRQ,
       A.GDDHLB,A.JC,A.DBTXH,A.XBTXH,CASE WHEN A.SFTG=1 THEN '是' ELSE '否' END AS SFTG,
       A.QBAGTYGS,A.QBAGFDGS,A.QBAGQQGS,
       A.QBHGTYGS,A.QBHGFDGS,A.QBHGQQGS,
       A.QBQTGDTYGS,A.QBQTGDFDGS,A.QBQTGDQQGS,
       A.ZXGDTYGS,A.ZXGDFDGS,A.ZXGDQQGS,
       A.QBTYGS,A.QBFDGS,A.QBQQGS
FROM [10.101.0.212].JYPRIME.dbo.usrGDDHBJ A
    JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B
        ON A.INBBM=B.INBBM AND B.ZQSC IN (18,83,90) AND B.ZQLB IN (1,2,41)
WHERE A.DBTXH<>100 AND
      B.GPDM = ? AND A.XXFBRQ = ? AND A.GDDHLB = ? AND A.JC = ?
ORDER BY B.GPDM, DBTXH, XBTXH
'''

SQL_QUERY_CX_HB = '''
SELECT A.ID,C.GPDM,A.GDDHGGR,A.GDDHLB,A.JC,A.ZCR,F.MS ZW,A.LSSWS,A.JBLS,D.GDMC,E.DM SJYADBTXH,D.SJYAXBTXH
FROM [10.101.0.212].JYPRIME.dbo.usrGDDHCX A
    FULL JOIN [10.101.0.212].JYPRIME.dbo.usrGDDHCX_SL B ON A.ID=B.ID
    LEFT JOIN [10.101.0.212].JYPRIME.dbo.usrXTCLB F ON B.DM=F.DM AND F.LB=1182
    JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB C ON A.INBBM=C.INBBM AND C.ZQSC IN (18,83,90) AND C.ZQLB IN (1,2,41)
    FULL JOIN [10.101.0.212].JYPRIME.dbo.usrGDDHHBBJQKB D ON A.INBBM=D.INBBM AND A.SCXXFBRQ=D.SCXXFBRQ AND A.GDDHLB=D.GDDHLB AND ISNULL(A.JC,'')=ISNULL(D.JC,'')
    FULL JOIN [10.101.0.212].JYPRIME.dbo.usrGDDHHBBJQKB_SL E ON D.ID=E.ID
WHERE A.GDCXLB=1 AND A.SFYX=1
  AND C.GPDM = ? AND A.GDDHGGR = ? AND A.GDDHLB = ? AND A.JC = ?
'''

# 设置日志
log_file = setup_logging()
logger = get_logger(__name__)


class EnhancedDataProcessor:
    """股东大会决议公告数据处理类 - 优化版本V2"""

    def __init__(self):
        self.data_cache = {}
        self.processed_count = 0
        self.total_files = 0
        self.lock = threading.Lock()
        self.file_status = {}
        self.basic_mapping = BASIC_MAPPING
        self.proposal_voting_mapping = PROPOSAL_VOTING_MAPPING
        self.uploaded_file_ids = {}
        # 降低上传并发数，减少速率限制
        self.upload_semaphore = threading.Semaphore(1)  # 从3降低到1

        # 验证配置
        try:
            validate_config()
        except ValueError as e:
            print(f"配置验证失败: {e}")
            raise

    def process_all_files(self, max_workers: int = None, custom_dir: str = None) -> List[Dict[str, Any]]:
        """批量处理所有PDF文件 - 使用优化的流水线模式"""
        max_workers = max_workers or processing_config.default_workers

        latest_dir = self._get_target_directory(custom_dir)
        if not latest_dir:
            print("未找到目标目录")
            return []

        pdf_files = list(Path(latest_dir).glob("*.pdf"))
        if not pdf_files:
            print("未找到PDF文件")
            return []

        self._initialize_processing_state(len(pdf_files))

        print(f"开始处理 {self.total_files} 个文件，使用 {max_workers} 个工作线程")

        try:
            # 使用优化的流水线模式
            all_results = self._optimized_pipeline(pdf_files, max_workers)

            print(f"\n处理完成! 共处理 {len(all_results)}/{self.total_files} 个文件")
            success_rate = (len(all_results) / self.total_files * 100) if self.total_files > 0 else 0
            print(f"成功率: {success_rate:.2f}%")

            return all_results

        except Exception as e:
            return []

    def _optimized_pipeline(self, pdf_files: List[Path], max_workers: int) -> List[Dict[str, Any]]:
        """优化的流水线处理 - 上传和处理并行进行"""
        upload_workers = 1
        process_workers = max_workers  # 处理线程数
        batch_size = max_workers  # 批量大小，直接使用处理线程数

        print(f"第一阶段：上传 {len(pdf_files)} 个文件，使用 {upload_workers} 个上传线程")
        print(f"第二阶段：处理文件，使用 {process_workers} 个处理线程")

        # 使用流水线模式并行上传和处理
        return self._pipeline_upload_and_process(pdf_files, process_workers, batch_size)

    def _pipeline_upload_and_process(self, pdf_files: List[Path], process_workers: int, batch_size: int) -> List[
        Dict[str, Any]]:
        """流水线模式：上传和处理并行进行"""
        all_results = []
        upload_queue = []  # 待上传文件队列
        processing_queue = {}  # 待处理文件字典 {文件路径: file_id}

        # 创建处理线程池
        with concurrent.futures.ThreadPoolExecutor(max_workers=process_workers) as executor:
            future_to_file = {}
            upload_count = 0
            processing_count = 0
            completed_count = 0

            # 初始化上传队列
            upload_queue = pdf_files.copy()

            print("开始上传公告...")

            # 当还有文件需要上传或处理时继续循环
            while upload_queue or processing_queue or future_to_file:
                # 1. 上传文件到处理队列（直到达到批量大小或没有更多文件）
                while len(processing_queue) < batch_size and upload_queue:
                    pdf_file = upload_queue.pop(0)
                    try:
                        file_id = self._upload_single_file_with_timeout(pdf_file)
                        if file_id:
                            processing_queue[pdf_file] = file_id
                            upload_count += 1
                            print(f"✓ 上传成功 ({upload_count}/{len(pdf_files)}): {pdf_file.name}")

                            # 立即提交处理任务
                            future = executor.submit(
                                self._process_and_cleanup_single_file,
                                pdf_file, file_id, pdf_file.name
                            )
                            future_to_file[future] = pdf_file
                            processing_count += 1
                        else:
                            print(f"✗ 上传失败 ({upload_count + 1}/{len(pdf_files)}): {pdf_file.name}")
                    except Exception as e:
                        print(f"✗ 上传异常 ({upload_count + 1}/{len(pdf_files)}) {pdf_file.name}: {e}")

                # 2. 检查已完成的处理任务
                if future_to_file:
                    # 使用非阻塞方式检查已完成的任务
                    done_futures = []
                    for future in future_to_file:
                        if future.done():
                            done_futures.append(future)

                    # 处理已完成的任务
                    for future in done_futures:
                        pdf_file = future_to_file.pop(future)
                        processing_queue.pop(pdf_file, None)  # 从处理队列中移除

                        try:
                            result = future.result()
                            if result:
                                all_results.append(result)
                                completed_count += 1
                                status = "成功"
                            else:
                                completed_count += 1
                                status = "失败"
                            print(
                                f"{'✓' if result else '✗'} 处理{status}({completed_count}/{len(pdf_files)}): {pdf_file.name}")
                        except Exception as e:
                            completed_count += 1
                            print(f"✗ 处理异常({completed_count}/{len(pdf_files)}): {pdf_file.name} - {e}")

                # 3. 短暂休眠避免CPU占用过高
                import time
                time.sleep(0.1)

        return all_results

    def _process_and_cleanup_single_file(self, pdf_file: Path, file_id: str, filename: str) -> Optional[Dict[str, Any]]:
        """处理单个文件并清理上传的文件"""

        # 提取基本信息用于数据库查询
        basic_data = self._extract_basic_info(filename)
        if not basic_data:
            logger.warning(f"无法提取基本信息: {filename}")
            return None

        try:
            # 处理文件
            bjqk_result = self.process_file_with_uploaded_id_bjqk(file_id, filename, basic_data)
            cxqk_result = self.process_file_with_uploaded_id_hbbj(file_id, filename, basic_data)
            # 处理完成后立即清理上传的文件
            self._cleanup_single_file(file_id)

            # 返回包含两个比对结果的字典
            return {
                "bjqk_result": bjqk_result,
                "cxqk_result": cxqk_result
            }
        except Exception as e:
            # 确保异常时也清理上传的文件
            self._cleanup_single_file(file_id)
            raise

    def _cleanup_single_file(self, file_id: str):
        """清理单个上传的文件"""
        try:
            enhanced_ai_service.delete_file(file_id)
        except Exception as e:
            pass

    @retry(
        stop=stop_after_attempt(processing_config.retry_attempts),
        wait=wait_exponential(
            multiplier=2,  # 增加重试间隔倍数
            min=5,  # 最小等待5秒
            max=30  # 最大等待30秒
        ),
        retry=retry_if_exception_type(Exception)
    )
    def _upload_single_file_with_retry(self, pdf_file: Path) -> str:
        """带重试机制的单个文件上传"""
        try:
            # 检查文件是否存在和可读
            if not pdf_file.exists():
                return ""

            if pdf_file.stat().st_size == 0:
                return ""

            return enhanced_ai_service.upload_file(str(pdf_file))

        except Exception as e:
            if "429" in str(e) or "rate_limit" in str(e).lower():
                raise e
            return ""

    def _upload_single_file_with_timeout(self, pdf_file: Path) -> str:
        """带超时控制的单个文件上传"""
        try:
            # 使用信号量控制并发，但设置超时
            acquired = self.upload_semaphore.acquire(timeout=processing_config.upload_timeout)
            if not acquired:
                return ""

            try:
                return self._upload_single_file_with_retry(pdf_file)
            finally:
                self.upload_semaphore.release()

        except Exception as e:
            # 确保信号量被释放
            try:
                self.upload_semaphore.release()
            except:
                pass
            return ""

    def process_file_with_uploaded_id_bjqk(self, file_id: str, filename: str, basic_data) -> Optional[Dict[str, Any]]:
        """处理单个PDF文件-股东大会投票表决情况"""
        try:
            # 从AI服务提取数据
            extracted_data = enhanced_ai_service.extract_data_from_file(file_id, self.load_prompt_from_md_bjqk())
            if not extracted_data:
                logger.warning(f"AI提取数据为空: {filename}")
                return None
            
            # 将AI提取的JSON数据保存到日志文件
            try:
                log_dir = os.path.join("logs")
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                session_id = get_session_id()
                with open(os.path.join(log_dir, f"ai_extraction_data_{session_id}.log"), "a", encoding="utf-8") as f:
                    f.write(f"\n=== 表决情况：{filename} ===\n")
                    f.write(f"{json.dumps(extracted_data, ensure_ascii=False, indent=2)}")
                    f.write(f"\n===========================================\n")
            except Exception as e:
                logger.warning(f"保存AI提取数据到日志文件失败: {filename} - {e}")

            # AI返回的数据结构是 {"届次": "1", "extracted_data": [表决数据数组]}
            # 从AI结果中提取届次，结合会议类型判断届次是否有效
            ai_jc = extracted_data.get("届次", "")
            meeting_type = basic_data.get("meeting_type", "")
            
            if ai_jc and str(ai_jc).strip():
                # AI届次有值，直接使用
                basic_data["meeting_session"] = str(ai_jc).strip()
            elif meeting_type == "1":
                # 年度股东大会，届次为空是正常的
                basic_data["meeting_session"] = ""
            else:
                # 临时股东大会但AI届次为空，说明AI提取异常，使用文件名解析值备用
                logger.warning(f"临时股东大会AI届次为空，使用文件名解析值: {basic_data.get('meeting_session')}")
                # 保持basic_data中的meeting_session（文件名解析值）
            
            # 需要获取实际的表决数据数组
            proposal_voting_data = []
            if "extracted_data" in extracted_data and isinstance(extracted_data["extracted_data"], list):
                proposal_voting_data = extracted_data["extracted_data"]
            elif isinstance(extracted_data, list):
                # 兼容直接返回数组的情况
                proposal_voting_data = extracted_data

            if not proposal_voting_data:
                logger.warning(f"没有提取到AI内的数组数据: {filename}")
                return None

            # 从数据库获取数据
            sql_data = self.get_data_from_db_bj(**basic_data)
            if not sql_data:
                logger.warning(f"数据库中未找到对应数据: {basic_data}")

            # 比对数据
            comparison_results = self.compare_data_bj({"proposal_voting_data": proposal_voting_data}, sql_data, filename, **basic_data)

            return {
                "stock_code": basic_data["stock_code"],
                "info_date": basic_data["info_date"],
                "meeting_type": basic_data["meeting_type"],
                "meeting_session": basic_data["meeting_session"],
                "extracted_data": proposal_voting_data,  # 确保这里是数组
                "sql_data": sql_data,
                "comparison_results": comparison_results,
                "processing_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

        except Exception as e:
            logger.error(f"处理文件时出错 {filename}: {e}")
            import traceback
            traceback.print_exc()
            return None

    def process_file_with_uploaded_id_hbbj(self, file_id: str, filename: str, basic_data) -> Optional[Dict[str, Any]]:
        """处理单个PDF文件-股东大会回避表决情况"""
        try:
            # 从AI服务提取数据
            hbbj_data = enhanced_ai_service.extract_data_from_file(file_id, self.load_prompt_from_md_hbbj())
            if not hbbj_data:
                logger.warning(f"AI提取数据为空: {filename}")
                return None

            # 将AI提取的JSON数据保存到日志文件
            try:
                log_dir = os.path.join("logs")
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                session_id = get_session_id()
                with open(os.path.join(log_dir, f"ai_extraction_data_{session_id}.log"), "a", encoding="utf-8") as f:
                    f.write(f"\n=== 出席回避：{filename} ===\n")
                    f.write(f"{json.dumps(hbbj_data, ensure_ascii=False, indent=2)}")
                    f.write(f"\n===========================================\n")
            except Exception as e:
                logger.warning(f"保存AI提取数据到日志文件失败: {filename} - {e}")

            # AI返回的数据结构是 {"extracted_data": {"届次": "1", "basic_data": {...}, "avoid_data": [...]}}
            # 需要获取实际的表决数据数组
            if "extracted_data" in hbbj_data:
                extracted_data = hbbj_data["extracted_data"]
                
                # 从AI结果中提取届次，结合会议类型判断届次是否有效
                if isinstance(extracted_data, dict):
                    ai_jc = extracted_data.get("届次", "")
                    meeting_type = basic_data.get("meeting_type", "")
                    
                    if ai_jc and str(ai_jc).strip():
                        # AI届次有值，直接使用
                        basic_data["meeting_session"] = str(ai_jc).strip()
                    elif meeting_type == "1":
                        # 年度股东大会，届次为空是正常的
                        basic_data["meeting_session"] = ""
                    else:
                        # 临时股东大会但AI届次为空，说明AI提取异常，使用文件名解析值备用
                        logger.warning(f"临时股东大会AI届次为空，使用文件名解析值: {basic_data.get('meeting_session')}")
                        # 保持basic_data中的meeting_session（文件名解析值）
                
                if isinstance(extracted_data, list):
                    # 直接是数组的情况
                    proposal_voting_data = extracted_data
                elif isinstance(extracted_data, dict):
                    # 字典格式，完整提取所有数据作为一条记录
                    proposal_voting_data = [extracted_data]
                else:
                    # 其他类型的数据，包装成列表
                    proposal_voting_data = [extracted_data] if extracted_data else []
            elif isinstance(hbbj_data, list):
                # 兼容直接返回数组的情况
                proposal_voting_data = hbbj_data
            else:
                logger.warning(f"无法识别的数据格式: {filename}")
                return None

            if not proposal_voting_data:
                logger.warning(f"没有提取到AI内的数组数据: {filename}")
                return None

            # 从数据库获取数据
            sql_data = self.get_data_from_db_hb(**basic_data)
            if not sql_data:
                logger.warning(f"数据库中未找到对应数据: {basic_data}")

            # 比对数据
            comparison_results = self.compare_data_hb({"proposal_voting_data": proposal_voting_data}, sql_data, filename, **basic_data)

            return {
                "stock_code": basic_data["stock_code"],
                "info_date": basic_data["info_date"],
                "meeting_type": basic_data["meeting_type"],
                "meeting_session": basic_data["meeting_session"],
                "extracted_data": proposal_voting_data,  # 确保这里是数组
                "sql_data": sql_data,
                "comparison_results": comparison_results,
                "processing_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

        except Exception as e:
            logger.error(f"处理文件时出错 {filename}: {e}")
            import traceback
            traceback.print_exc()
            return None

    def get_data_from_db_bj(self, stock_code: str, info_date: str, meeting_type: str, meeting_session: str) -> Optional[
        List[Dict[str, Any]]]:
        """从数据库获取数据，动态处理届次查询条件"""
        try:
            if meeting_session and str(meeting_session).strip():
                sql = SQL_QUERY_BJ
                params = (stock_code, info_date, meeting_type, meeting_session)
            else:
                sql = SQL_QUERY_BJ.replace("AND A.JC = ?", "AND A.JC IS NULL")
                params = (stock_code, info_date, meeting_type)
            return db_manager.execute_query(sql, params)
        except Exception as e:
            return None

    def get_data_from_db_hb(self, stock_code: str, info_date: str, meeting_type: str, meeting_session: str) -> Optional[
        List[Dict[str, Any]]]:
        """从数据库获取数据，动态处理届次查询条件"""
        try:
            if meeting_session and str(meeting_session).strip():
                sql = SQL_QUERY_CX_HB
                params = (stock_code, info_date, meeting_type, meeting_session)
            else:
                sql = SQL_QUERY_CX_HB.replace("AND A.JC = ?", "AND A.JC IS NULL")
                params = (stock_code, info_date, meeting_type)
            raw_data = db_manager.execute_query(sql, params)
            return self._preprocess_sql_data_hb(raw_data)
        except Exception as e:
            return None

    def _preprocess_sql_data_hb(self, raw_data: Optional[List[Dict[str, Any]]]) -> Optional[List[Dict[str, Any]]]:
        """预处理数据库返回的数据，包括去重、合并ZW字段、合并SJYADBTXH字段、合并SJYADBTXH和SJYAXBTXH字段"""
        if not raw_data:
            return raw_data

        processed_data = []
        
        # 第一步：根据除ZW外的字段去重合并ZW字段
        zw_unique_records = {}
        for record in raw_data:
            # 创建除ZW外的字段组合键
            key_fields = {k: v for k, v in record.items() if k != 'ZW'}
            key = str(sorted(key_fields.items()))
            
            # 如果已存在相同除ZW外的记录，则合并ZW字段
            if key in zw_unique_records:
                existing_record = zw_unique_records[key]
                existing_zw = existing_record.get('ZW', '')
                current_zw = record.get('ZW', '')
                
                # 合并ZW字段，用顿号分隔
                if existing_zw and current_zw:
                    # 去重并合并
                    zw_list = list(set(existing_zw.split('、') + current_zw.split('、')))
                    zw_list = [zw for zw in zw_list if zw]  # 去除空字符串
                    zw_unique_records[key]['ZW'] = '、'.join(sorted(zw_list))
                elif current_zw:
                    zw_unique_records[key]['ZW'] = current_zw
            else:
                # 新记录，直接添加
                zw_unique_records[key] = record.copy()
        
        # 第二步：根据除SJYADBTXH外的字段去重合并SJYADBTXH字段
        sjyad_unique_records = {}
        for record in zw_unique_records.values():
            # 创建除SJYADBTXH外的字段组合键
            key_fields = {k: v for k, v in record.items() if k != 'SJYADBTXH'}
            key = str(sorted(key_fields.items()))
            
            if key in sjyad_unique_records:
                existing_record = sjyad_unique_records[key]
                existing_sjyad = existing_record.get('SJYADBTXH', '')
                current_sjyad = record.get('SJYADBTXH', '')
                
                # 合并SJYADBTXH字段，用逗号分隔
                if existing_sjyad and current_sjyad:
                    # 分割、去重
                    sjyad_list = []
                    for num_str in [existing_sjyad, current_sjyad]:
                        if num_str:
                            nums = [n.strip() for n in str(num_str).split(',') if n.strip()]
                            sjyad_list.extend(nums)
                    
                    # 去重并排序
                    unique_sjyad = list(set(sjyad_list))
                    try:
                        unique_sjyad.sort(key=lambda x: int(x) if x.isdigit() else x)
                    except:
                        unique_sjyad.sort()
                    
                    sjyad_unique_records[key]['SJYADBTXH'] = ','.join(unique_sjyad)
                elif current_sjyad:
                    sjyad_unique_records[key]['SJYADBTXH'] = current_sjyad
            else:
                sjyad_unique_records[key] = record.copy()
        
        # 第三步：合并SJYADBTXH和SJYAXBTXH两个字段，并按数字大小顺序排列
        for record in sjyad_unique_records.values():
            sjyad_btxh = record.get('SJYADBTXH', '')
            sjyax_btxh = record.get('SJYAXBTXH', '')
            
            # 合并两个字段
            if sjyad_btxh and sjyax_btxh:
                # 分割、去重、排序
                all_numbers = []
                for num_str in [sjyad_btxh, sjyax_btxh]:
                    if num_str:
                        # 处理可能的逗号分隔情况
                        nums = [n.strip() for n in str(num_str).split(',') if n.strip()]
                        all_numbers.extend(nums)
                
                # 去重并按数字升序排序
                unique_numbers = list(set(all_numbers))
                try:
                    # 尝试按数字排序
                    unique_numbers.sort(key=lambda x: int(x) if x.isdigit() else x)
                except:
                    # 如果不是纯数字，按字符串排序
                    unique_numbers.sort()
                
                # 合并为逗号分隔的字符串
                record['SJYAXH'] = ','.join(unique_numbers)
            elif sjyad_btxh:
                record['SJYAXH'] = sjyad_btxh
            elif sjyax_btxh:
                record['SJYAXH'] = sjyax_btxh
            else:
                record['SJYAXH'] = ''
            
            # 移除原始字段
            if 'SJYADBTXH' in record:
                del record['SJYADBTXH']
            if 'SJYAXBTXH' in record:
                del record['SJYAXBTXH']
            
            processed_data.append(record)
        return processed_data

    def compare_data_bj(self, current_data: Dict[str, Any], sql_data_list: Optional[List[Dict[str, Any]]], filename, stock_code: str, info_date: str, meeting_type: str, meeting_session: str) -> List[
        Dict[str, Any]]:
        """比对AI数据与SQL数据"""

        comparison_results = []

        # 提取AI数据中的序号数据
        ai_voting_data = current_data.get("proposal_voting_data", [])

        if not ai_voting_data:
            logger.warning("AI提取的表决数据为空")
            return [{
                "ID": "",
                "GPDM": sql_data_list[0].get("GPDM", "") if sql_data_list else "",
                "XXFBRQ": sql_data_list[0].get("XXFBRQ", "").strftime('%Y-%m-%d') if sql_data_list and sql_data_list[
                    0].get("XXFBRQ") else "",
                "GDDHLB": sql_data_list[0].get("GDDHLB", "") if sql_data_list else "",
                "JC": sql_data_list[0].get("JC", "") if sql_data_list else "",
                "DBTXH": "",
                "XBTXH": "",
                "错误描述": "AI提取的表决数据为空"
            }]

        for ai_item in ai_voting_data:

            if not sql_data_list:
                logger.warning("数据库未查询到对应表决数据，请检查！")
                return [{
                    "ID": filename,
                    "GPDM": stock_code,
                    "XXFBRQ": info_date,
                    "GDDHLB": meeting_type,
                    "JC": meeting_session,
                    "DBTXH": ai_item.get("大议案序号", ""),
                    "XBTXH": ai_item.get("小议案序号", ""),
                    "错误描述": "数据库未查询到对应表决数据，请检查！"
                }]

            # 根据关键字段匹配数据库记录
            matched_sql_item = self._find_matching_sql_record(ai_item, sql_data_list)

            if not matched_sql_item:
                comparison_results.append({
                    "ID": "",
                    "GPDM": sql_data_list[0].get("GPDM", "") if sql_data_list else "",
                    "XXFBRQ": sql_data_list[0].get("XXFBRQ", "").strftime('%Y-%m-%d') if sql_data_list and
                                                                                         sql_data_list[0].get(
                                                                                             "XXFBRQ") else "",
                    "GDDHLB": sql_data_list[0].get("GDDHLB", "") if sql_data_list else "",
                    "JC": sql_data_list[0].get("JC", "") if sql_data_list else "",
                    "DBTXH": ai_item.get("大议案序号", ""),
                    "XBTXH": ai_item.get("小议案序号", ""),
                    "错误描述": "数据库无对应序号匹配的数据"
                })
                continue

            # 进行字段比对
            error_messages = self._compare_fields(ai_item, matched_sql_item)

            # 无论是否有错误，都记录比对结果
            comparison_results.append({
                "ID": str(matched_sql_item.get("ID", "")),
                "GPDM": str(matched_sql_item.get("GPDM", "")),
                "XXFBRQ": matched_sql_item.get("XXFBRQ", "").strftime('%Y-%m-%d') if matched_sql_item.get(
                    "XXFBRQ") else "",
                "GDDHLB": matched_sql_item.get("GDDHLB", ""),
                "JC": matched_sql_item.get("JC", ""),
                "DBTXH": matched_sql_item.get("DBTXH", ""),
                "XBTXH": matched_sql_item.get("XBTXH", ""),
                "错误描述": "；".join(error_messages) if error_messages else ""
            })

        return comparison_results

    def compare_data_hb(self, current_data: Dict[str, Any], sql_data_list: Optional[List[Dict[str, Any]]], filename, stock_code: str, info_date: str, meeting_type: str, meeting_session: str) -> List[
        Dict[str, Any]]:
        """比对回避AI数据与SQL数据"""
        comparison_results = []

        ai_voting_data = current_data.get("proposal_voting_data", [])

        if not ai_voting_data:
            logger.warning("AI提取的表决数据为空")
            return [{
                "ID": "",
                "GPDM": sql_data_list[0].get("GPDM", "") if sql_data_list else "",
                "GDDHGGR": sql_data_list[0].get("GDDHGGR", "").strftime('%Y-%m-%d') if sql_data_list and sql_data_list[
                    0].get("GDDHGGR") else "",
                "GDDHLB": sql_data_list[0].get("GDDHLB", "") if sql_data_list else "",
                "JC": sql_data_list[0].get("JC", "") if sql_data_list else "",
                "错误描述": "AI提取的表决数据为空"
            }]

        if not sql_data_list:
            logger.warning("数据库未查询到对应出席回避数据，请检查！")
            return [{
                "ID": filename,
                "GPDM": stock_code,
                "GDDHGGR": info_date,
                "GDDHLB": meeting_type,
                "JC": meeting_session,
                "错误描述": "数据库未查询到对应出席回避数据，请检查！"
            }]

        ai_item_basic = ai_voting_data[0]['basic_data']
        ai_item_avoid = ai_voting_data[0]['avoid_data']

        error_messages = self._compare_basic_fields_attendinfo(ai_item_basic, sql_data_list[0])

        # 进行回避数据比对
        error_messages = self._compare_avoid_data(error_messages, ai_item_avoid, sql_data_list)

        # 无论是否有错误，都记录比对结果
        comparison_results.append({
            "ID": str(sql_data_list[0].get("ID", "")),
            "GPDM": str(sql_data_list[0].get("GPDM", "")),
            "GDDHGGR": sql_data_list[0].get("GDDHGGR", "").strftime('%Y-%m-%d') if sql_data_list[0].get("GDDHGGR") else "",
            "GDDHLB": sql_data_list[0].get("GDDHLB", ""),
            "JC": sql_data_list[0].get("JC", ""),
            "错误描述": "；".join(error_messages) if error_messages else ""
        })

        return comparison_results

    def _compare_basic_fields_attendinfo(self, ai_item, sql_item) -> List[str]:
        """比对出席表表决数据字段"""
        error_messages = []

        # 使用映射配置进行字段比对
        for ai_field, sql_field in BASIC_MAPPING.items():
            ai_value = ai_item.get(ai_field, "").replace('（', '(').replace('）', ')')
            sql_value = sql_item.get(sql_field, "")

            if ai_field in ("主持人", "见证律师事务所", "经办律师"):
                # 字符串比较
                if str(ai_value).strip() != str(sql_value).strip() and ai_value:
                    error_messages.append(f"{ai_field}【正式库：{sql_value}，AI：{ai_value}】")
            if ai_field == "主持人职位":
                if str(ai_value).strip() != str(sql_value).strip() and ai_value:
                    error_messages.append(f"{ai_field}【正式库：{sql_value}，AI：{ai_value}】")

        return error_messages

    def _find_matching_sql_record(self, ai_item: Dict[str, Any], sql_data_list: List[Dict[str, Any]]) -> Optional[
        Dict[str, Any]]:
        """根据关键字段在SQL数据中查找匹配的记录"""
        for sql_item in sql_data_list:
            # 预处理议案序号：如果值为0则置空
            if ai_item.get("小议案序号", "") == 0:
                ai_item["小议案序号"] = None

            if (str(sql_item.get("DBTXH", "")).strip() == str(ai_item.get("大议案序号", "")).strip() and
                    str(sql_item.get("XBTXH", "")).strip() == str(ai_item.get("小议案序号", "")).strip()):
                return sql_item
        return None

    def _compare_fields(self, ai_item: Dict[str, Any], sql_item: Dict[str, Any]) -> List[str]:
        """比对单个记录的所有字段"""
        error_messages = []

        # 基础字段比对
        basic_errors = self._compare_basic_fields(ai_item, sql_item)
        error_messages.extend(basic_errors)

        # 投票数据字段比对
        voting_errors = self._compare_voting_fields(ai_item, sql_item)
        error_messages.extend(voting_errors)

        return error_messages

    def _compare_basic_fields(self, ai_item: Dict[str, Any], sql_item: Dict[str, Any]) -> List[str]:
        """比对表决表基础字段"""
        error_messages = []

        # 是否通过字段比对
        sftg_ai = str(ai_item.get("是否通过", "")).strip().upper()
        sftg_sql = str(sql_item.get("SFTG", "")).strip().upper()

        if sftg_ai and sftg_sql and sftg_ai != sftg_sql:
            error_messages.append(f"是否通过【正式库：{sftg_sql}，AI：{sftg_ai}】")

        return error_messages

    def _compare_voting_fields(self, ai_item: Dict[str, Any], sql_item: Dict[str, Any]) -> List[str]:
        """比对表决表表决数据字段"""
        error_messages = []

        # 使用映射配置进行字段比对
        for ai_field, sql_field in PROPOSAL_VOTING_MAPPING.items():
            if ai_field in ["是否通过"]:  # 基础字段已在其他方法中处理
                continue

            ai_value = ai_item.get(ai_field, "")
            sql_value = sql_item.get(sql_field, "")

            # 处理数值类型的比较（考虑浮点数精度）
            if self._is_numeric_field(ai_field):
                if not self._compare_numeric_values(ai_value, sql_value):
                    error_messages.append(f"{ai_field}【正式库：{sql_value}，AI：{ai_value}】")
            else:
                # 字符串比较
                if str(ai_value).strip() != str(sql_value).strip():
                    error_messages.append(f"{ai_field}【正式库：{sql_value}，AI：{ai_value}】")

        return error_messages

    def _compare_avoid_data(self, error_messages, ai_avoid_list: List[Dict[str, Any]], sql_data_list: List[Dict[str, Any]]) -> List[str]:
        """比对回避股东数据"""

        if not ai_avoid_list:
            # 检查AI是否有回避数据但数据库没有
            if sql_data_list:
                for sql_item in sql_data_list:
                    if sql_item.get('GDMC'):
                        error_messages.append(f"AI提取数据为空【股东：{sql_item.get('GDMC')}】")
            return error_messages

        # 提取AI中的回避股东列表和议案信息
        def preprocess_proposal(proposal_str):
            """预处理议案编号，去除多余的0"""
            if not proposal_str:
                return proposal_str
            try:
                for i in range(3):
                    if '.0' in proposal_str:
                        proposal_str = proposal_str.replace('.0', '.')
                    else:
                        return proposal_str

            except (ValueError, TypeError):
                # 如果转换失败，返回原始字符串
                return proposal_str
        
        ai_avoid_shareholders = {}
        for ai_item in ai_avoid_list:
            shareholder = str(ai_item.get('回避股东', '')).strip().replace('（','(').replace('）',')')
            proposal = preprocess_proposal(str(ai_item.get('回避议案', '')).strip())
            if shareholder:
                ai_avoid_shareholders[shareholder] = proposal

        # 提取SQL中的回避股东列表和议案信息
        sql_avoid_shareholders = {}
        for sql_item in sql_data_list:
            shareholder = str(sql_item.get('GDMC', '')).strip()
            if shareholder:
                # 从数据库记录中提取议案信息，SJYAXH字段表示议案编号
                proposal_code = sql_item.get('SJYAXH', '')
                if proposal_code:
                    # 直接使用议案编号（阿拉伯数字）
                    proposal = str(proposal_code)
                else:
                    proposal = ''
                    
                sql_avoid_shareholders[shareholder] = {
                    'proposal': proposal,
                    'sql_item': sql_item
                }

        # 比对股东名称是否一致
        ai_shareholders = set(ai_avoid_shareholders.keys())
        sql_shareholders = set(sql_avoid_shareholders.keys())

        # 检查是否有AI有但数据库没有的股东（漏处理）
        ai_only = ai_shareholders - sql_shareholders
        for shareholder in ai_only:
            error_messages.append(f"漏处理【股东：{shareholder}，回避议案：{ai_avoid_shareholders[shareholder]}】")

        # 检查是否有数据库有但AI没有的股东（多处理）
        sql_only = sql_shareholders - ai_shareholders
        for shareholder in sql_only:
            sql_item = sql_avoid_shareholders[shareholder]['sql_item']
            error_messages.append(f"正式库多处理【股东：{shareholder}，回避议案{sql_item['SJYAXH']}】")

        # 检查共同股东的比对议案是否一致
        common_shareholders = ai_shareholders & sql_shareholders
        for shareholder in common_shareholders:
            ai_proposal = ai_avoid_shareholders[shareholder]
            sql_proposal = sql_avoid_shareholders[shareholder]['proposal']
            
            # 比对回避议案是否一致
            if str(ai_proposal).strip() != str(sql_proposal).strip():
                error_messages.append(f"回避议案不一致【股东：{shareholder}，AI议案：{ai_proposal}，正式库议案：{sql_proposal}】")

        return error_messages

    def _is_numeric_field(self, field_name: str) -> bool:
        """判断字段是否为数值类型"""
        numeric_indicators = ["股数", "GS"]
        return any(indicator in field_name for indicator in numeric_indicators)

    def _compare_numeric_values(self, value1: Any, value2: Any, tolerance: float = 0.0001) -> bool:
        """比较数值字段，考虑精度容差、千分位格式和科学计数法"""
        try:
            # 处理空值
            if value1 is None and value2 is None:
                return True
            if value1 is None or value2 is None:
                return False

            # 统一格式化数值
            def normalize_number(value):
                if isinstance(value, str):
                    # 移除逗号千分位分隔符
                    value = value.replace(',', '')
                    # 处理科学计数法
                    if 'E' in value.upper() or 'E-' in value.upper():
                        try:
                            return str(float(value))
                        except ValueError:
                            return value
                return value

            normalized_value1 = normalize_number(value1)
            normalized_value2 = normalize_number(value2)

            # 转换为浮点数比较
            num1 = float(normalized_value1) if normalized_value1 else 0
            num2 = float(normalized_value2) if normalized_value2 else 0

            # 处理零值的特殊情况
            if abs(num1) < tolerance and abs(num2) < tolerance:
                return True

            # 允许一定的精度误差
            return abs(num1 - num2) <= tolerance

        except (ValueError, TypeError):
            # 如果转换失败，进行字符串比较（统一格式化后比较）
            def clean_for_string_compare(value):
                if not value:
                    return ""
                # 移除逗号，处理科学计数法
                cleaned = str(value).replace(',', '').strip()
                # 尝试转换为数字再转回字符串，统一格式
                try:
                    return str(float(cleaned))
                except (ValueError, TypeError):
                    return cleaned

            cleaned1 = clean_for_string_compare(value1)
            cleaned2 = clean_for_string_compare(value2)
            return cleaned1 == cleaned2

    def _get_target_directory(self, custom_dir: str = None):
        """获取目标目录路径"""
        if custom_dir:
            if not os.path.exists(custom_dir):
                print(f"指定目录不存在: {custom_dir}")
                return None
            return custom_dir
        return self.get_latest_download_dir()

    def _initialize_processing_state(self, file_count: int):
        """初始化处理状态"""
        self.total_files = file_count
        self.processed_count = 0
        self.file_status = {}

    def _extract_basic_info(self, filename: str) -> Dict[str, str]:
        """提取基本信息用于数据库查询"""
        try:
            parts = filename.split('-')
            if len(parts) < 6:
                return {}

            stock_code = parts[0]
            info_date = '-'.join(parts[2:5])  # 格式: 2025-09-29

            # 提取会议类型和届次
            meeting_info = '-'.join(parts[5:])

            # 确定会议类型
            meeting_type = "3"  # 默认为临时股东大会
            if "年度" in meeting_info and "临时" not in meeting_info:
                meeting_type = "1"
            elif "出资人组" in meeting_info:
                meeting_type = "5"

            # 提取届次
            session_match = re.search(r'第([一二三四五六七八九十\d]+)次', meeting_info)
            if session_match:
                jc_text = session_match.group(1)
                meeting_session = self.convert_chinese_number(jc_text)
            else:
                meeting_session = None

            return {
                "stock_code": stock_code,
                "info_date": info_date,
                "meeting_type": meeting_type,
                "meeting_session": meeting_session
            }
        except Exception as e:
            return {}

    def convert_chinese_number(self, chinese_num: str) -> str:
        """将汉字数字转换为阿拉伯数字"""
        chinese_to_digit = {
            '一': '1', '二': '2', '三': '3', '四': '4', '五': '5',
            '六': '6', '七': '7', '八': '8', '九': '9', '十': '10',
            '十一': '11', '十二': '12', '十三': '13', '十四': '14', '十五': '15'
        }

        if chinese_num.isdigit():
            return chinese_num

        return chinese_to_digit.get(chinese_num, chinese_num)

    def get_latest_download_dir(self, base_dir: str = "./files") -> Optional[str]:
        """获取最新下载的目录"""
        try:
            directories = [d for d in Path(base_dir).iterdir() if d.is_dir()]
            return str(max(directories, key=os.path.getmtime)) if directories else None
        except Exception as e:
            return None

    def get_resource_path(self, relative_path):
        """获取资源的绝对路径，适用于开发和打包后的环境"""
        try:
            # PyInstaller创建的临时文件夹路径
            base_path = sys._MEIPASS
        except Exception:
            # 开发环境下的路径
            base_path = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(base_path, relative_path)

    def load_prompt_from_md_bjqk(self, md_file_path: str = "prompt_GDDHBJ.md") -> str:
        """从MD文件加载提示词（优先从exe目录读取，便于用户修改）"""
        try:
            # 1. 优先从exe所在目录读取（打包后用户可修改）
            try:
                exe_dir = os.path.dirname(sys.executable)
                exe_path = os.path.join(exe_dir, md_file_path)
                if os.path.exists(exe_path):
                    with open(exe_path, 'r', encoding='utf-8') as f:
                        return f.read()
            except:
                pass
            
            # 2. 尝试当前工作目录
            if os.path.exists(md_file_path):
                with open(md_file_path, 'r', encoding='utf-8') as f:
                    return f.read()
            
            # 3. 尝试脚本所在目录（开发环境）
            script_dir = os.path.dirname(os.path.abspath(__file__))
            script_path = os.path.join(script_dir, md_file_path)
            if os.path.exists(script_path):
                with open(script_path, 'r', encoding='utf-8') as f:
                    return f.read()
            
            print(f"警告: 无法找到提示词文件: {md_file_path}")
            return ""
        except Exception as e:
            print(f"加载提示词文件失败 {md_file_path}: {e}")
            return ""

    def load_prompt_from_md_hbbj(self, md_file_path: str = "prompt_GDDHCX_HBBJ.md") -> str:
        """从MD文件加载提示词（优先从exe目录读取，便于用户修改）"""
        try:
            # 1. 优先从exe所在目录读取（打包后用户可修改）
            try:
                exe_dir = os.path.dirname(sys.executable)
                exe_path = os.path.join(exe_dir, md_file_path)
                if os.path.exists(exe_path):
                    with open(exe_path, 'r', encoding='utf-8') as f:
                        return f.read()
            except:
                pass
            
            # 2. 尝试当前工作目录
            if os.path.exists(md_file_path):
                with open(md_file_path, 'r', encoding='utf-8') as f:
                    return f.read()
            
            # 3. 尝试脚本所在目录（开发环境）
            script_dir = os.path.dirname(os.path.abspath(__file__))
            script_path = os.path.join(script_dir, md_file_path)
            if os.path.exists(script_path):
                with open(script_path, 'r', encoding='utf-8') as f:
                    return f.read()
            
            print(f"警告: 无法找到提示词文件: {md_file_path}")
            return ""
        except Exception as e:
            print(f"加载提示词文件失败 {md_file_path}: {e}")
            return ""

    def generate_report(self, results: List[Dict[str, Any]]) -> Optional[str]:
        """生成Excel报告，包含两个sheet：表决情况和出席回避"""
        if not results:
            print("警告: 没有可用的处理结果")
            return None

        try:
            # 准备两个sheet的数据
            bjqk_report_data = []  # 表决情况数据
            cxqk_report_data = []  # 出席回避数据

            for result in results:
                # 处理表决情况数据
                if "bjqk_result" in result and result["bjqk_result"] and "comparison_results" in result["bjqk_result"]:
                    bjqk_report_data.extend(result["bjqk_result"]["comparison_results"])

                # 处理出席回避数据
                if "cxqk_result" in result and result["cxqk_result"] and "comparison_results" in result["cxqk_result"]:
                    cxqk_report_data.extend(result["cxqk_result"]["comparison_results"])

            # 创建Excel写入器
            os.makedirs("report", exist_ok=True)
            session_id = get_session_id()
            report_path = f"report/report_{session_id}.xlsx"
            
            with pd.ExcelWriter(report_path, engine='openpyxl') as writer:
                # 写入表决情况sheet
                if bjqk_report_data:
                    bjqk_df = pd.DataFrame(bjqk_report_data)
                    bjqk_df.to_excel(writer, sheet_name="表决情况", index=False)
                    print(f"表决情况数据已写入，共 {len(bjqk_report_data)} 条记录")
                else:
                    print("警告: 没有表决情况数据可用于生成报告")
                    # 创建空sheet
                    pd.DataFrame().to_excel(writer, sheet_name="表决情况", index=False)

                # 写入出席回避sheet
                if cxqk_report_data:
                    cxqk_df = pd.DataFrame(cxqk_report_data)
                    cxqk_df.to_excel(writer, sheet_name="出席回避", index=False)
                    print(f"出席回避数据已写入，共 {len(cxqk_report_data)} 条记录")
                else:
                    print("警告: 没有出席回避数据可用于生成报告")
                    # 创建空sheet
                    pd.DataFrame().to_excel(writer, sheet_name="出席回避", index=False)

            # 输出统计信息
            bjqk_total = len(bjqk_report_data)
            bjqk_correct = len([r for r in bjqk_report_data if r.get("错误描述") == ""]) if bjqk_report_data else 0
            bjqk_errors = bjqk_total - bjqk_correct

            cxqk_total = len(cxqk_report_data)
            cxqk_correct = len([r for r in cxqk_report_data if r.get("错误描述") == ""]) if cxqk_report_data else 0
            cxqk_errors = cxqk_total - cxqk_correct

            print(f"\n报告已生成: {report_path}")
            print(f"表决情况统计: 总记录数={bjqk_total}, 正确记录={bjqk_correct}, 错误记录={bjqk_errors}")
            print(f"出席回避统计: 总记录数={cxqk_total}, 正确记录={cxqk_correct}, 错误记录={cxqk_errors}")

            return report_path

        except Exception as e:
            print(f"生成报告时出错: {e}")
            import traceback
            traceback.print_exc()
            return None


def run_crawler() -> bool:
    """运行采集程序"""
    try:
        cj.main()
        return True
    except Exception as e:
        return False


def main():
    """主函数"""
    try:
        processor = EnhancedDataProcessor()

        print("=" * 50)
        print("股东大会决议公告AI比对系统")
        print("=" * 50)

        while True:
            print("\n请选择操作:")
            print("1. 运行采集程序并处理数据")
            print("2. 仅处理已下载的文件")
            print("3. 退出")

            choice = input("请输入选择 (1-3): ").strip()

            if choice == "1":
                # 初始化文件日志
                log_file = setup_file_logging()
                
                if run_crawler():
                    print("正在处理文件……")
                    results = processor.process_all_files(max_workers=processing_config.default_workers)
                    if results:
                        report_path = processor.generate_report(results)
                        if report_path:
                            print(f"\n✓ 处理完成！报告已生成: {report_path}")
                        else:
                            print("\n✗ 报告生成失败")
                    else:
                        print("\n✗ 没有成功处理的文件")

            elif choice == "2":
                # 初始化文件日志
                log_file = setup_file_logging()
                
                print("\n请选择处理方式:")
                print("1. 处理最新下载的文件")
                print("2. 指定目录处理文件")

                process_choice = input("请输入选择 (1-2): ").strip()
                custom_dir = None

                if process_choice == "2":
                    custom_dir = input("请输入要处理的目录路径: ").strip()
                    if not custom_dir:
                        print("目录路径不能为空")
                        continue

                results = processor.process_all_files(max_workers=processing_config.default_workers,
                                                      custom_dir=custom_dir)
                if results:
                    report_path = processor.generate_report(results)
                    if report_path:
                        print(f"\n✓ 处理完成！报告已生成: {report_path}")
                    else:
                        print("\n✗ 报告生成失败")
                else:
                    print("\n✗ 没有成功处理的文件")

            elif choice == "3":
                print("程序退出")
                # 清理资源
                db_manager.close_pool()
                break

            else:
                print("无效选择，请重新输入")

    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"程序运行异常: {e}")
    finally:
        # 确保资源清理
        try:
            db_manager.close_pool()
        except:
            pass


if __name__ == "__main__":
    main()
