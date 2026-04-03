"""
股东背景介绍处理器
"""
import os
import re
import fitz
from typing import Dict, Optional

from processors.base_processor import BaseProcessor
from utils.pdf_utils import (
    get_exchange_code, crop_page_before_keyword, crop_page_after_keyword,
    crop_same_page, process_standard_pdf
)
from logger import logger
from PyPDF2 import PdfReader, PdfWriter


class BjjsProcessor(BaseProcessor):
    """股东背景介绍处理器"""
    
    MODULE_NAME = "股东背景介绍"
    
    def find_keywords(self, pdf_path: str) -> Dict:
        """查找股东背景介绍关键词"""
        exchange_code = get_exchange_code(pdf_path)
        
        if exchange_code == "bjs":
            start_pattern = re.compile(
                r'^[（(]?[\d一二三四五六七八九]*.*[）)]?[、.．]?\s*持股5%以上的股东或前十名股东情况\s*$',
                re.IGNORECASE | re.MULTILINE
            )
            end_pattern = re.compile(
                r'是否存在实际控制人',
                re.IGNORECASE | re.MULTILINE
            )
        else:
            start_pattern = re.compile(
                r'^[（(]?[\d一二三四五六七八九]*.*[）)]?[、.．]?\s*(?:控股)?股东[和及]实际控制人情况(?:介绍)?\s*$',
                re.IGNORECASE | re.MULTILINE
            )
            end_pattern = re.compile(
                r'^[（(]?[\d一二三四五六七八九]*.*[）)]?[、.．]?\s*[公司|本行]控股股东或第一大股东及其一致行动人累计质押股份数量占',
                re.IGNORECASE | re.MULTILINE
            )
        
        def get_search_rect(page, inst, keyword_type):
            page_width = page.rect.width
            page_rotation = page.rotation
            
            if keyword_type in ['实际控制人情况', '股东情况', '累计质押', '存在实际']:
                if page_rotation == 90:
                    return fitz.Rect(inst.x0 - 30, 0, inst.x1 + 30, page_width)
                else:
                    return fitz.Rect(0, inst.y0 - 30, page_width, inst.y1 + 30)
            else:
                return fitz.Rect(0, inst.y0 - 30, page_width, inst.y1 + 30)
        
        def collect_and_sort_instances(page, keywords, pattern, get_search_rect):
            """
            收集所有keyword实例，按位置从上到下排序后匹配pattern，返回第一个匹配结果
            
            PyMuPDF坐标系：原点在左上角，Y轴向下递增
            因此 y0 值越小，位置越靠上
            """
            candidates = []
            
            for keyword in keywords:
                instances = page.search_for(keyword)
                for inst in instances:
                    candidates.append({
                        'keyword': keyword,
                        'inst': inst,
                        'y0': inst.y0,
                        'x0': inst.x0
                    })
            
            if not candidates:
                return None
            
            page_rotation = page.rotation
            if page_rotation == 90:
                candidates.sort(key=lambda c: c['x0'])
            else:
                candidates.sort(key=lambda c: c['y0'])
            
            for candidate in candidates:
                keyword = candidate['keyword']
                inst = candidate['inst']
                rect = get_search_rect(page, inst, keyword)
                text = page.get_text("text", clip=rect)
                if pattern.search(text):
                    return {
                        'inst': inst,
                        'keyword': keyword
                    }
            
            return None
        
        doc = fitz.open(pdf_path)
        total_pages = len(doc)
        
        start_info = None
        end_info = None
        
        if exchange_code == "bjs":
            start_keyword = ["股东情况"]
            end_keyword = ["存在实际"]
        else:
            start_keyword = ["实际控制人情况"]
            end_keyword = ["累计质押"]
        
        search_ranges = [
            (30, 75),
            (70, 105),
            (100, 200)
        ]
        
        found = False
        for start_range, end_range in search_ranges:
            actual_start = max(1, start_range)
            actual_end = min(end_range, total_pages)
            
            for page_num in range(actual_start - 1, actual_end):
                page = doc.load_page(page_num)
                page_rect = page.rect
                
                if not start_info:
                    result = collect_and_sort_instances(page, start_keyword, start_pattern, get_search_rect)
                    if result:
                        inst = result['inst']
                        start_info = {
                            'page_number': page_num + 1,
                            'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                            'page_dimensions': (page_rect.width, page_rect.height)
                        }
                        logger.debug(f"股东背景介绍找到开始关键词在第 {page_num + 1} 页")
                
                if not end_info:
                    result = collect_and_sort_instances(page, end_keyword, end_pattern, get_search_rect)
                    if result:
                        inst = result['inst']
                        end_info = {
                            'page_number': page_num + 1,
                            'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                            'page_dimensions': (page_rect.width, page_rect.height)
                        }
                        logger.debug(f"股东背景介绍找到结束关键词在第 {page_num + 1} 页")
                
                if start_info and end_info:
                    found = True
                    break
            
            if found:
                break
        
        doc.close()
        
        return {
            'start': start_info,
            'end': end_info
        }
    
    def process_pdf(self, pdf_path: str, keywords: Dict) -> Optional[str]:
        """处理股东背景介绍PDF切片"""
        exchange_code = get_exchange_code(pdf_path)
        
        if exchange_code == "kcb":
            logger.debug(f"科创板股票，跳过处理: {os.path.basename(pdf_path)}")
            return None
        
        start_info = keywords.get('start')
        end_info = keywords.get('end')
        
        if not start_info and not end_info:
            logger.debug(f"股东背景介绍未找到任何关键词: {os.path.basename(pdf_path)}")
            return None
        
        if not start_info and end_info:
            logger.debug(f"股东背景介绍只找到结束关键词: {os.path.basename(pdf_path)}")
            return crop_page_before_keyword(pdf_path, end_info, self.output_dir, pre_pages=5)
        
        if start_info and not end_info:
            logger.debug(f"股东背景介绍只找到开始关键词: {os.path.basename(pdf_path)}")
            return crop_page_after_keyword(pdf_path, start_info, self.output_dir, post_pages=4)
        
        if start_info and end_info:
            if start_info['page_number'] > end_info['page_number']:
                logger.debug(f"开始关键词页码大于结束关键词页码，按只找到结束处理: {os.path.basename(pdf_path)}")
                return crop_page_before_keyword(pdf_path, end_info, self.output_dir, pre_pages=5)
            
            if start_info['page_number'] == end_info['page_number']:
                logger.debug(f"股东背景介绍同一页: {os.path.basename(pdf_path)}")
                return crop_same_page(pdf_path, start_info, end_info, self.output_dir)
            
            logger.debug(f"股东背景介绍标准处理: {os.path.basename(pdf_path)}")
            return process_standard_pdf(pdf_path, start_info, end_info, self.output_dir)
        
        return None
