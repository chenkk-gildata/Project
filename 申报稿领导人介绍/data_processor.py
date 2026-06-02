"""
领导人数据提取模块 - AI提取、数据后处理、Excel输出
"""
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from ai_service_enhanced import enhanced_ai_service
from config import validate_config
from logger_config import get_logger, get_file_only_logger, get_session_id
from path_utils import get_prompt_path, get_logs_dir, get_reports_dir, get_base_dir

logger = get_logger(__name__)
file_only_logger = get_file_only_logger(__name__)

LEADER_FIELDS = [
    "领导人姓名", "性别", "出生日期", "学历", "国籍",
    "职位描述", "职称", "任期上限", "兼职状况", "背景介绍"
]

TITLE_CODE_MAP = {
    "工程师": "1",
    "高级工程师": "2",
    "助理工程师": "3",
    "经济师": "4",
    "高级经济师": "5",
    "助理经济师": "6",
    "研究员": "7",
    "副研究员": "8",
    "教授": "9",
    "副教授": "10",
    "政工师": "11",
    "高级政工师": "12",
    "会计师": "13",
    "注册会计师": "14",
    "院士": "15",
    "助理政工师": "16",
    "助理研究员": "20",
    "高级会计师": "21",
    "审计师": "22",
    "高级审计师": "23",
    "注册审计师": "24",
    "注册评估师": "25",
    "注册税务师": "26",
    "执业药师": "27",
    "建筑师": "28",
    "分析师": "29",
    "测量师": "30",
    "律师": "31",
}

OUTPUT_FIELDS = [
    "证券代码", "信息发布日期", "届次",
    "领导人姓名", "性别", "出生日期", "学历", "国籍",
    "职位描述", "序号", "职称", "任期上限", "兼职状况", "背景介绍"
]

POSITION_CODE_MAP = {
    1: ["董事长", "执行董事长", "代理董事长", "联席董事长", "名誉董事长"],
    2: ["总经理", "总裁", "代理总经理", "代理总裁", "首席执行官", "首席执行官(CEO)",
        "行长", "执行总经理", "执行总裁", "执行委员会委员", "联席总经理"],
    3: ["副董事长", "常务副董事长"],
    4: ["董事", "常务董事", "非执行董事", "执行董事", "职工董事"],
    5: ["独立董事", "独立非执行董事"],
    6: ["监事会主席", "监事会副主席", "代理监事会主席"],
    7: ["监事", "职工监事", "员工监事", "股东监事"],
    8: ["独立监事", "外部监事"],
    9: ["常务副总", "常务副总裁", "常务副行长"],
    10: ["副总经理", "副总裁", "副行长", "执行副总经理", "执行副总裁"],
    11: ["总经理助理", "总裁助理", "行长助理"],
    12: ["董事会秘书", "代理董事会秘书", "信息披露负责人", "信息披露负责人（发债人）", "公司秘书"],
    14: ["证券事务代表", "代理证券事务代表"],
    15: ["财务总监", "首席财务官", "首席财务官(CFO)", "总会计师", "财务负责人",
         "审计部负责人", "内部审计负责人", "代理总会计师", "代理财务总监", "代理首席财务官"],
    16: ["财务副总监", "副总会计师"],
    17: ["市场总监", "技术总监", "首席技术官", "首席技术官(CTO)",
         "人力资源总监", "首席人事官", "首席人事官(CHO)",
         "首席信息官", "首席信息官(CIO)", "首席风险官", "首席风险官(CRO)",
         "首席营运官", "首席营运官(COO)", "首席审计官",
         "首席市场官", "首席市场官(CMO)", "合规负责人", "合规总监", "业务总监"],
    18: ["总工程师", "总经济师"],
    19: ["副总经济师", "副总工程师"],
    20: ["工会主席", "工会副主席"],
    21: ["审计委员会", "薪酬与考核委员会", "提名委员会", "战略委员会",
         "风险管理委员会", "关联交易控制委员会", "其他委员会"],
    22: ["核心技术人员"],
    23: ["其他高级管理人员", "总法律顾问"],
    24: ["其他职位"],
}

_POSITION_LOOKUP = {}
for _code, _positions in POSITION_CODE_MAP.items():
    for _pos in _positions:
        _POSITION_LOOKUP[_pos] = _code


class LeaderDataProcessor:
    """领导人数据提取处理器"""

    def __init__(self):
        self.lock = threading.Lock()
        self.success_count = 0
        self.failed_count = 0
        self.prompt_cache = None

        try:
            validate_config()
        except ValueError as e:
            print(f"配置验证失败: {e}")
            raise

    def _load_prompt(self) -> str:
        """加载提示词"""
        if self.prompt_cache is not None:
            return self.prompt_cache

        prompt_path = get_prompt_path("prompt_step2_rules.md")
        if os.path.exists(prompt_path):
            with open(prompt_path, 'r', encoding='utf-8') as f:
                self.prompt_cache = f.read()
            logger.info(f"已加载提示词文件: {prompt_path}")
            return self.prompt_cache
        else:
            logger.error(f"提示词文件不存在: {prompt_path}")
            raise FileNotFoundError(f"提示词文件不存在: {prompt_path}")

    def _load_metadata(self, download_folder: str) -> Dict[str, Dict]:
        """加载元数据"""
        metadata_path = os.path.join(download_folder, 'metadata.json')
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    @staticmethod
    def _get_position_code(position_desc: str) -> Union[int, str]:
        """根据职位描述获取职位代码（序号），多职位取最小值，任一未匹配则返回99"""
        if not position_desc or not isinstance(position_desc, str):
            return ""

        positions = [p.strip() for p in position_desc.replace("、", ",").split(",") if p.strip()]

        matched_codes = []
        for pos in positions:
            if pos in _POSITION_LOOKUP:
                matched_codes.append(_POSITION_LOOKUP[pos])
                continue
            found = False
            for key, code in _POSITION_LOOKUP.items():
                if key in pos or pos in key:
                    matched_codes.append(code)
                    found = True
                    break
            if not found:
                return 99

        if matched_codes:
            return min(matched_codes)
        return 99

    @staticmethod
    def _extract_metadata_from_filename(filename: str) -> Dict[str, str]:
        """从文件名中提取GPDM和XXFBRQ，格式：GPDM-XXFBRQ-XXBT.pdf"""
        base_name = os.path.splitext(filename)[0]
        parts = base_name.split('-')
        if len(parts) >= 4:
            gpdm = parts[0]
            xxfbrq = '-'.join(parts[1:4])
            return {"GPDM": gpdm, "XXFBRQ": xxfbrq}
        return {}

    def process_all_files(self, pdf_files: List[Path], download_folder: str,
                          mode: str = "新增", skip_excel_output: bool = False) -> List[Dict[str, Any]]:
        """处理所有PDF文件：上传 → AI提取 → 后处理 → 输出Excel
        
        Args:
            pdf_files: PDF文件列表
            download_folder: 下载文件夹路径
            mode: 运行模式，"新增" 或 "比对"
            skip_excel_output: 是否跳过Excel输出（比对模式下只提取不单独输出Excel）
        """
        if not pdf_files:
            return []

        metadata = self._load_metadata(download_folder)

        if not metadata:
            for pdf_file in pdf_files:
                filename = pdf_file.name
                file_meta = self._extract_metadata_from_filename(filename)
                if file_meta:
                    metadata[filename] = file_meta

        session_id = get_session_id()

        base_dir = get_base_dir()
        base_output_dir = os.path.join(base_dir, "output", mode)

        prompt = self._load_prompt()

        self.success_count = 0
        self.failed_count = 0

        total_count = len(pdf_files)
        workers = min(12, total_count)

        logger.info(f"开始处理 {total_count} 个PDF文件，使用 {workers} 个并发线程")

        results = []

        executor = ThreadPoolExecutor(max_workers=workers)

        try:
            futures = {}
            for pdf_file in pdf_files:
                filename = pdf_file.name
                meta = metadata.get(filename, {})
                future = executor.submit(
                    self._process_single_file,
                    str(pdf_file), filename, meta, prompt, base_output_dir, skip_excel_output, session_id, mode
                )
                futures[future] = filename

            completed = 0
            for future in as_completed(futures):
                filename = futures[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                        with self.lock:
                            self.success_count += 1
                        print(f"  ✓ 处理成功({self.success_count + self.failed_count}/{total_count}): {filename}")
                    else:
                        with self.lock:
                            self.failed_count += 1
                        print(f"  ✗ 处理失败({self.success_count + self.failed_count}/{total_count}): {filename}")
                except Exception as e:
                    with self.lock:
                        self.failed_count += 1
                    logger.error(f"处理文件异常 {filename}: {e}")
                    print(f"  ✗ 处理异常({self.success_count + self.failed_count}/{total_count}): {filename}")

        except Exception as e:
            logger.error(f"批量处理过程中发生错误: {e}")
        finally:
            executor.shutdown(wait=True)

        print(f"\nAI提取完成! 成功: {self.success_count}, 失败: {self.failed_count}")
        logger.info(f"AI提取完成! 成功: {self.success_count}, 失败: {self.failed_count}")

        return results

    def _process_single_file(self, pdf_path: str, filename: str,
                             metadata: Dict, prompt: str,
                             base_output_dir: str, skip_excel_output: bool = False,
                             session_id: str = "", mode: str = "新增") -> Optional[Dict[str, Any]]:
        """处理单个文件：上传 → AI提取 → 校验 → 重试 → 后处理 → 输出Excel"""
        file_id = None
        max_retries = 3
        try:
            file_id = enhanced_ai_service.upload_file(pdf_path)
            if not file_id:
                logger.error(f"文件上传失败: {filename}")
                return None

            logger.info(f"文件上传成功: {filename} (ID: {file_id})")

            processed_data = None

            for attempt in range(max_retries):
                ai_result = enhanced_ai_service.extract_data_two_step(file_id)

                if not ai_result:
                    logger.error(f"AI提取返回空结果: {filename} (第{attempt+1}次)")
                    continue

                step1_output = ai_result.get('step1_output', '')
                result_data = ai_result.get('extracted_data', {})
                
                if isinstance(result_data, dict):
                    extracted_data = result_data.get('extracted_data', [])
                else:
                    extracted_data = result_data
                
                if not extracted_data:
                    logger.error(f"AI提取返回空数据: {filename} (第{attempt+1}次)")
                    continue
                
                self._save_extraction_log(filename, step1_output, extracted_data)

                zqlb = self._extract_zqlb_from_filename(filename)
                processed_data = self._post_process_data(extracted_data, zqlb, mode)
                break

            if processed_data is None:
                logger.error(f"AI提取失败（已重试{max_retries}次）: {filename}")
                return None

            excel_path = None
            if not skip_excel_output:
                excel_path = self._output_excel(processed_data, metadata, base_output_dir, session_id)

            return {
                "filename": filename,
                "file_path": pdf_path,
                "metadata": metadata,
                "extracted_data": processed_data,
                "excel_path": excel_path
            }

        except Exception as e:
            logger.error(f"处理文件异常 {filename}: {e}")
            return None
        finally:
            if file_id:
                try:
                    enhanced_ai_service.delete_file(file_id)
                except Exception:
                    pass

    def _save_extraction_log(self, filename: str, step1_output: str, extracted_data: Any):
        """保存AI提取结果到日志"""
        try:
            log_dir = get_logs_dir()
            session_id = get_session_id()
            
            step1_log_path = os.path.join(log_dir, f"step1_tables_{session_id}.log")
            with open(step1_log_path, "a", encoding="utf-8") as f:
                f.write(f"\n{'='*60}\n")
                f.write(f"=== {filename} ===\n")
                f.write(f"{'='*60}\n")
                f.write(step1_output)
                f.write("\n")
            
            step2_log_path = os.path.join(log_dir, f"step2_extracted_{session_id}.log")
            with open(step2_log_path, "a", encoding="utf-8") as f:
                f.write(f"\n{'='*60}\n")
                f.write(f"=== {filename} ===\n")
                f.write(f"{'='*60}\n")
                f.write(f"{json.dumps(extracted_data, ensure_ascii=False, indent=2)}\n")
        except Exception:
            pass

    def _validate_positions(self, processed_data: List[Dict[str, Any]]) -> List[str]:
        """校验提取结果，返回问题列表。空列表表示通过。"""
        return []

    def _extract_zqlb_from_filename(self, filename: str) -> str:
        """从文件名中提取证券类别
        
        文件名格式：GPDM-日期-标题-证券类别.后缀
        例如：300123-2026-05-13-公告标题-837.pdf
        """
        try:
            name_without_ext = Path(filename).stem
            parts = name_without_ext.split('-')
            if len(parts) >= 2:
                return parts[-1]
        except Exception:
            pass
        return ""

    def _post_process_data(self, extracted_data: Union[Dict, List], zqlb: str = "", mode: str = "新增") -> List[Dict[str, Any]]:
        """后处理AI提取的数据，标准化字段
        
        Args:
            extracted_data: AI提取的原始数据
            zqlb: 证券类别，用于判断是否移除核心技术人员
            mode: 运行模式，"新增" 或 "比对"
        """
        if isinstance(extracted_data, dict):
            data_list = [extracted_data]
        elif isinstance(extracted_data, list):
            data_list = extracted_data
        else:
            return []

        remove_core_tech = (zqlb not in ("837", "188"))

        processed = []
        for item in data_list:
            if not isinstance(item, dict):
                continue

            record = {}
            for field in LEADER_FIELDS:
                value = item.get(field, "")
                if value is None:
                    value = ""
                if isinstance(value, str):
                    value = value.strip()
                record[field] = value

                record["届次"] = ""
            pos = record.get("职位描述", "")
            if not pos:
                continue
            
            positions = [p.strip() for p in pos.split("、") if p.strip() != "高级管理人员"]
            
            if remove_core_tech:
                positions = [p for p in positions if p != "核心技术人员"]
            
            if not positions:
                continue
            
            seen = set()
            unique = []
            for p in positions:
                if p not in seen:
                    seen.add(p)
                    unique.append(p)
            record["职位描述"] = "、".join(unique)
            record["序号"] = self._get_position_code(record.get("职位描述", ""))

            title = record.get("职称", "")
            if title:
                title_parts = [t.strip() for t in title.split("、") if t.strip()]
                codes = []
                all_found = True
                for t in title_parts:
                    if t in TITLE_CODE_MAP:
                        codes.append(TITLE_CODE_MAP[t])
                    else:
                        all_found = False
                        break
                
                if all_found and codes:
                    record["职称"] = ",".join(codes)

            bg_intro = record.get("背景介绍", "")
            if bg_intro and mode == "新增" and not bg_intro.startswith("　"):
                record["背景介绍"] = "　　" + bg_intro

            processed.append(record)

        return processed

    def _output_excel(self, data: List[Dict[str, Any]],
                      metadata: Dict, base_output_dir: str,
                      session_id: str = "") -> Optional[str]:
        if not data:
            logger.warning("无数据可输出Excel")
            return None

        zqjc = metadata.get('ZQJC', '')
        gpdm = metadata.get('GPDM', '未知')
        xxfbrq = metadata.get('XXFBRQ', '未知')

        folder_name = f"{gpdm}-{xxfbrq}"
        folder_name = folder_name.replace('/', '-').replace('\\', '-').replace(':', '-')

        per_file_output_dir = os.path.join(base_output_dir, folder_name)
        os.makedirs(per_file_output_dir, exist_ok=True)

        if session_id:
            excel_filename = f"{gpdm}_{session_id}.xlsx"
        else:
            excel_filename = f"{gpdm}.xlsx"
        excel_path = os.path.join(per_file_output_dir, excel_filename)

        try:
            for row in data:
                row["证券代码"] = gpdm
                row["信息发布日期"] = xxfbrq

            df = pd.DataFrame(data, columns=OUTPUT_FIELDS)
            df.to_excel(excel_path, index=False, engine='openpyxl')

            from openpyxl import load_workbook
            wb = load_workbook(excel_path)
            ws = wb.active

            column_widths = {
                'A': 8, 'B': 12, 'C': 6, 'D': 12,
                'E': 6, 'F': 12, 'G': 8, 'H': 8, 'I': 30,
                'J': 6, 'K': 15, 'L': 12, 'M': 15, 'N': 60
            }
            for col_letter, width in column_widths.items():
                ws.column_dimensions[col_letter].width = width

            wb.save(excel_path)

            logger.info(f"Excel已输出: {excel_path}")
            return excel_path

        except Exception as e:
            logger.error(f"输出Excel失败: {e}")
            return None
