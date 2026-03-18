"""
职工构成处理器
"""
import os
import re
import fitz
from typing import Dict, Optional

from processors.base_processor import BaseProcessor
from utils.pdf_utils import (
    is_bse_pdf, crop_page_before_keyword, crop_page_after_keyword,
    crop_same_page, process_standard_pdf
)
from logger import logger


class ZggcProcessor(BaseProcessor):
    """职工构成处理器"""
    
    MODULE_NAME = "职工构成"
    
    def find_keywords(self, pdf_path: str) -> Dict:
        """查找职工构成关键词"""
        is_bse = is_bse_pdf(pdf_path)
        
        if is_bse:
            # 北交所关键词
            start_pattern = re.compile(r'在职[职|员]工.*基本情况', re.IGNORECASE | re.MULTILINE)
            end_pattern = re.compile(r'员工薪酬政策.*培训计划.*离退休职工人数', re.IGNORECASE | re.MULTILINE)
            start_keyword = ['在职职工', '基本情况']
            end_keyword = ['薪酬政策']
        else:
            # 沪深关键词
            start_pattern = re.compile(r'(?<!高级管理人员[和及])(?:公司|本集团(的)?)?员工情况\s*$|员工及其薪金\s*$', re.IGNORECASE | re.MULTILINE)
            end_pattern = re.compile(r'[（(]?[一二三四五六七八九\d][）)]?[、.．]?\s*(?:员工)?薪酬政策', re.IGNORECASE | re.MULTILINE)
            start_keyword = ["员工情况"]
            end_keyword = ['薪酬政策']
        
        doc = fitz.open(pdf_path)
        total_pages = len(doc)
        
        start_info = None
        end_info = None
        
        # 搜索范围
        search_ranges = [(30, 60), (60, 90), (20, 30), (1, 20), (90, total_pages)]
        
        def get_search_rect(page, inst, keyword_type):
            """根据关键词类型和页面旋转角度智能调整搜索区域"""
            page_width = page.rect.width
            page_rotation = page.rotation

            if keyword_type in ['在职职工', '基本情况', '薪酬政策', '员工情况']:
                # 处理旋转页面
                if page_rotation == 90:
                    return fitz.Rect(inst.x0 - 50, 0, inst.x1 + 50, page_width)
                else:
                    return fitz.Rect(0, inst.y0 - 50, page_width, inst.y1 + 50)
            else:
                # 默认搜索区域
                return fitz.Rect(0, inst.y0 - 50, inst.x1 + 400, inst.y1 + 50)

        found = False
        for start_range, end_range in search_ranges:
            actual_start = max(1, start_range)
            actual_end = min(end_range, total_pages)

            for page_num in range(actual_start - 1, actual_end):
                page = doc.load_page(page_num)
                page_rect = page.rect

                # 搜索开始关键词
                if not end_info:
                    for keyword in start_keyword:
                        instances = page.search_for(keyword)
                        for inst in instances:
                            rect = get_search_rect(page, inst, keyword)
                            text = page.get_text("text", clip=rect)
                            if start_pattern.search(text):
                                start_info = {
                                    'page_number': page_num + 1,
                                    'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                    'page_dimensions': (page_rect.width, page_rect.height)
                                }
                                break
                        if start_info:
                            break

                # 搜索结束关键词
                if start_info and not end_info:
                    for keyword in end_keyword:
                        instances = page.search_for(keyword)
                        for inst in instances:
                            rect = get_search_rect(page, inst, keyword)
                            text = page.get_text("text", clip=rect)
                            if end_pattern.search(text):
                                end_info = {
                                    'page_number': page_num + 1,
                                    'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                    'page_dimensions': (page_rect.width, page_rect.height)
                                }
                                break
                        if end_info:
                            break

                if start_info and end_info:
                    found = True
                    break

            if found:
                break
        
        doc.close()
        
        return {
            'start': start_info,
            'end': end_info,
            'is_bse': is_bse
        }
    
    def process_pdf(self, pdf_path: str, keywords: Dict) -> Optional[str]:
        """处理职工构成PDF"""
        start_info = keywords.get('start')
        end_info = keywords.get('end')
        
        # 什么都没找到
        if not start_info and not end_info:
            logger.debug(f"职工构成未找到任何关键词,跳过处理: {os.path.basename(pdf_path)}")
            return None
        
        # 只找到结束关键词
        if not start_info and end_info:
            logger.debug(f"职工构成只找到结束关键词: {os.path.basename(pdf_path)}")
            return crop_page_before_keyword(pdf_path, end_info, self.output_dir, pre_pages=1)
        
        # 只找到开始关键词
        if start_info and not end_info:
            logger.debug(f"职工构成只找到开始关键词: {os.path.basename(pdf_path)}")
            return crop_page_after_keyword(pdf_path, start_info, self.output_dir, post_pages=1)
        
        # 找到开始和结束关键词
        if start_info and end_info:
            # 开始页大于结束页
            if start_info['page_number'] > end_info['page_number']:
                return crop_page_before_keyword(pdf_path, end_info, self.output_dir, pre_pages=1)
            
            # 同一页
            if start_info['page_number'] == end_info['page_number']:
                return crop_same_page(pdf_path, start_info, end_info, self.output_dir)
            
            # 正常处理
            return process_standard_pdf(pdf_path, start_info, end_info, self.output_dir)
        
        return None
