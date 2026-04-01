"""
领导人介绍处理器（含联系人模块）
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


class LdrjsProcessor(BaseProcessor):
    """领导人介绍处理器（含联系人模块）"""
    
    MODULE_NAME = "领导人介绍"
    
    def find_lxr_keywords(self, pdf_path: str) -> Dict:
        """查找联系人关键词（仅上交所和深交所）"""
        exchange_code = get_exchange_code(pdf_path)
        
        if exchange_code == "bjs":
            return {'start': None, 'end': None}
        
        start_pattern = re.compile(
            r'[\d一二三四]?[、.．]?\s*联系人和联系方式',
            re.IGNORECASE | re.MULTILINE
        )
        
        end_pattern = re.compile(
            r'[\d二三四五]?[、.．]?\s*(?:基本情况简介|信息披露及备置地点)',
            re.IGNORECASE | re.MULTILINE
        )
        
        doc = fitz.open(pdf_path)
        total_pages = len(doc)
        
        start_info = None
        end_info = None
        
        search_ranges = [(3, 20)]
        
        def get_search_rect(page, inst, keyword_type):
            page_width = page.rect.width
            page_rotation = page.rotation
            
            if keyword_type in ['联系人', '基本情况', '备置地点']:
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

        found = False
        skip_directory_check = False
        
        for start_range, end_range in search_ranges:
            actual_start = max(1, start_range)
            actual_end = min(end_range, total_pages)
            
            for page_num in range(actual_start - 1, actual_end):
                page = doc.load_page(page_num)
                page_rect = page.rect
                
                if not skip_directory_check:
                    directory_instances = page.search_for("目录")
                    if directory_instances:
                        skip_directory_check = True
                        continue
                
                if not end_info:
                    result = collect_and_sort_instances(page, ["联系人"], start_pattern, get_search_rect)
                    if result:
                        inst = result['inst']
                        start_info = {
                            'page_number': page_num + 1,
                            'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                            'page_dimensions': (page_rect.width, page_rect.height)
                        }

                if start_info and not end_info:
                    result = collect_and_sort_instances(page, ["基本情况", "备置地点"], end_pattern, get_search_rect)
                    if result:
                        inst = result['inst']
                        end_info = {
                            'page_number': page_num + 1,
                            'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                            'page_dimensions': (page_rect.width, page_rect.height)
                        }
                
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
    
    def find_keywords(self, pdf_path: str) -> Dict:
        """查找所有关键词（联系人+领导人介绍）- 兼容基类调用"""
        return self.find_all_keywords(pdf_path)
    
    def find_ldrjs_keywords(self, pdf_path: str) -> Dict:
        """查找领导人介绍关键词"""
        exchange_code = get_exchange_code(pdf_path)
        
        start_pattern = re.compile(
            r'^[（(]?[\d一二三四五六七八九]*.*[）)]?[、.．]?\s*(?:本行|公司|现任)?董事(?:、监事)?[及和、]高级管理人员(?:的|基本)?情况',
            re.IGNORECASE | re.MULTILINE
        )
        
        if exchange_code == "bjs":
            end_pattern = re.compile(
                r'董事(?:、监事)?[和、]高级管理人员.*决策程序.*报酬.*支付情况',
                re.IGNORECASE | re.MULTILINE
            )
        else:
            end_pattern = re.compile(
                r'在(?:本公司)?(?:股东|其他)(?:及关联)?单位.*任职的?情况',
                re.IGNORECASE | re.MULTILINE
            )
        
        doc = fitz.open(pdf_path)
        total_pages = len(doc)
        
        start_info = None
        end_info = None
        
        search_ranges = [(15, 50), (45, 70), (65, 130)]
        
        def get_search_rect(page, inst, keyword_type):
            page_width = page.rect.width
            page_rotation = page.rotation

            if keyword_type in ['高级管理人员', '股东单位', '任职情况', '决策程序', '报酬确定', '支付情况']:
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

        found = False
        for start_range, end_range in search_ranges:
            actual_start = max(1, start_range)
            actual_end = min(end_range, total_pages)

            for page_num in range(actual_start - 1, actual_end):
                page = doc.load_page(page_num)
                page_rect = page.rect

                if not end_info:
                    result = collect_and_sort_instances(page, ["高级管理人员"], start_pattern, get_search_rect)
                    if result:
                        inst = result['inst']
                        start_info = {
                            'page_number': page_num + 1,
                            'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                            'page_dimensions': (page_rect.width, page_rect.height)
                        }

                if start_info and not end_info:
                    end_keywords = ["股东单位", "任职情况"] if exchange_code != "bjs" else ["决策程序", "报酬确定", "支付情况"]
                    result = collect_and_sort_instances(page, end_keywords, end_pattern, get_search_rect)
                    if result:
                        inst = result['inst']
                        end_info = {
                            'page_number': page_num + 1,
                            'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                            'page_dimensions': (page_rect.width, page_rect.height)
                        }

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
    
    def find_all_keywords(self, pdf_path: str) -> Dict:
        """查找所有关键词（联系人+领导人介绍）"""
        exchange_code = get_exchange_code(pdf_path)
        
        if exchange_code == "bjs":
            return {
                'lxr': {'start': None, 'end': None},
                'ldrjs': self.find_ldrjs_keywords(pdf_path)
            }
        else:
            return {
                'lxr': self.find_lxr_keywords(pdf_path),
                'ldrjs': self.find_ldrjs_keywords(pdf_path)
            }
    
    def _crop_lxr_start_only(self, pdf_path: str, start_info: Dict) -> Optional[str]:
        """只找到开始关键词：当前页裁剪 + 下一页完整"""
        os.makedirs(self.output_dir, exist_ok=True)
        
        reader = PdfReader(pdf_path)
        writer = PdfWriter()
        
        page_num = start_info['page_number']
        page = reader.pages[page_num - 1]
        
        page_width, page_height = start_info['page_dimensions']
        min_x, min_y, max_x, max_y = start_info['keyword_box']
        page_rotation = page.rotation
        
        pydf2_max_y = page_height - min_y
        
        cropped_page = page
        if page_rotation == 90:
            cropped_page.cropbox.lower_left = (min_x, 0)
            cropped_page.cropbox.upper_right = (page_height, page_width)
        else:
            cropped_page.cropbox.lower_left = (0, 0)
            cropped_page.cropbox.upper_right = (page_width, pydf2_max_y)
        
        writer.add_page(cropped_page)
        
        if page_num < len(reader.pages):
            writer.add_page(reader.pages[page_num])
        
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        output_path = os.path.join(self.output_dir, f"{base_name}.pdf")
        
        with open(output_path, "wb") as output_file:
            writer.write(output_file)
        
        return output_path
    
    def _crop_lxr_end_only(self, pdf_path: str, end_info: Dict) -> Optional[str]:
        """只找到结束关键词：上一页完整 + 当前页裁剪"""
        os.makedirs(self.output_dir, exist_ok=True)
        
        reader = PdfReader(pdf_path)
        writer = PdfWriter()
        
        page_num = end_info['page_number']
        
        if page_num > 1:
            writer.add_page(reader.pages[page_num - 2])
        
        page = reader.pages[page_num - 1]
        page_width, page_height = end_info['page_dimensions']
        min_x, min_y, max_x, max_y = end_info['keyword_box']
        page_rotation = page.rotation
        
        if page_rotation == 90:
            pydf2_min_y = page_height - max_x
        else:
            pydf2_min_y = page_height - max_y
        
        cropped_page = page
        if page_rotation == 90:
            cropped_page.cropbox.lower_left = (0, 0)
            cropped_page.cropbox.upper_right = (max_x, page_width)
        else:
            cropped_page.cropbox.lower_left = (0, pydf2_min_y)
            cropped_page.cropbox.upper_right = (page_width, page_height)
        
        writer.add_page(cropped_page)
        
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        output_path = os.path.join(self.output_dir, f"{base_name}.pdf")
        
        with open(output_path, "wb") as output_file:
            writer.write(output_file)
        
        return output_path
    
    def _crop_lxr_two_pages(self, pdf_path: str, start_info: Dict, end_info: Dict) -> Optional[str]:
        """联系人两页裁剪：开始页裁剪 + 结束页裁剪"""
        os.makedirs(self.output_dir, exist_ok=True)
        
        reader = PdfReader(pdf_path)
        writer = PdfWriter()
        
        start_page = reader.pages[start_info['page_number'] - 1]
        page_width, page_height = start_info['page_dimensions']
        min_x, min_y, max_x, max_y = start_info['keyword_box']
        page_rotation = start_page.rotation
        
        pydf2_max_y = page_height - min_y
        
        cropped_start = start_page
        if page_rotation == 90:
            cropped_start.cropbox.lower_left = (min_x, 0)
            cropped_start.cropbox.upper_right = (page_height, page_width)
        else:
            cropped_start.cropbox.lower_left = (0, 0)
            cropped_start.cropbox.upper_right = (page_width, pydf2_max_y)
        
        writer.add_page(cropped_start)
        
        end_page = reader.pages[end_info['page_number'] - 1]
        page_width, page_height = end_info['page_dimensions']
        min_x, min_y, max_x, max_y = end_info['keyword_box']
        page_rotation = end_page.rotation
        
        if page_rotation == 90:
            pydf2_min_y = page_height - max_x
        else:
            pydf2_min_y = page_height - max_y
        
        cropped_end = end_page
        if page_rotation == 90:
            cropped_end.cropbox.lower_left = (0, 0)
            cropped_end.cropbox.upper_right = (max_x, page_width)
        else:
            cropped_end.cropbox.lower_left = (0, pydf2_min_y)
            cropped_end.cropbox.upper_right = (page_width, page_height)
        
        writer.add_page(cropped_end)
        
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        output_path = os.path.join(self.output_dir, f"{base_name}.pdf")
        
        with open(output_path, "wb") as output_file:
            writer.write(output_file)
        
        return output_path
    
    def _process_lxr_only(self, pdf_path: str, lxr_info: Dict) -> Optional[str]:
        """单独处理联系人切片"""
        start_info = lxr_info.get('start')
        end_info = lxr_info.get('end')
        
        if not start_info and not end_info:
            return None
        
        if not start_info and end_info:
            logger.debug(f"联系人只找到结束关键词: {os.path.basename(pdf_path)}")
            return self._crop_lxr_end_only(pdf_path, end_info)
        
        if start_info and not end_info:
            logger.debug(f"联系人只找到开始关键词: {os.path.basename(pdf_path)}")
            return self._crop_lxr_start_only(pdf_path, start_info)
        
        if start_info and end_info:
            if start_info['page_number'] > end_info['page_number']:
                return self._crop_lxr_end_only(pdf_path, end_info)
            
            if start_info['page_number'] == end_info['page_number']:
                logger.debug(f"联系人同一页: {os.path.basename(pdf_path)}")
                return crop_same_page(pdf_path, start_info, end_info, self.output_dir)
            
            if start_info['page_number'] + 1 == end_info['page_number']:
                logger.debug(f"联系人两页: {os.path.basename(pdf_path)}")
                return self._crop_lxr_two_pages(pdf_path, start_info, end_info)
            
            logger.debug(f"联系人超过两页，按只找到结束处理: {os.path.basename(pdf_path)}")
            return self._crop_lxr_end_only(pdf_path, end_info)
        
        return None
    
    def _process_ldrjs_only(self, pdf_path: str, ldrjs_info: Dict) -> Optional[str]:
        """单独处理领导人介绍切片"""
        start_info = ldrjs_info.get('start')
        end_info = ldrjs_info.get('end')
        
        if not start_info and not end_info:
            logger.debug(f"领导人介绍未找到任何关键词,跳过处理: {os.path.basename(pdf_path)}")
            return None
        
        if not start_info and end_info:
            logger.debug(f"领导人介绍只找到结束关键词: {os.path.basename(pdf_path)}")
            return crop_page_before_keyword(pdf_path, end_info, self.output_dir, pre_pages=5)
        
        if start_info and not end_info:
            logger.debug(f"领导人介绍只找到开始关键词: {os.path.basename(pdf_path)}")
            return crop_page_after_keyword(pdf_path, start_info, self.output_dir, post_pages=4)
        
        if start_info and end_info:
            if start_info['page_number'] > end_info['page_number']:
                return crop_page_before_keyword(pdf_path, end_info, self.output_dir, pre_pages=5)
            
            if start_info['page_number'] == end_info['page_number']:
                return crop_same_page(pdf_path, start_info, end_info, self.output_dir)
            
            return process_standard_pdf(pdf_path, start_info, end_info, self.output_dir)
        
        return None
    
    def _add_lxr_slice_to_writer(self, reader: PdfReader, writer: PdfWriter, lxr_info: Dict):
        """添加联系人切片到writer"""
        start_info = lxr_info.get('start')
        end_info = lxr_info.get('end')
        
        if not start_info and not end_info:
            return
        
        if not start_info and end_info:
            page_num = end_info['page_number']
            if page_num > 1:
                writer.add_page(reader.pages[page_num - 2])
            
            page = reader.pages[page_num - 1]
            page_width, page_height = end_info['page_dimensions']
            min_x, min_y, max_x, max_y = end_info['keyword_box']
            page_rotation = page.rotation
            
            if page_rotation == 90:
                pydf2_min_y = page_height - max_x
            else:
                pydf2_min_y = page_height - max_y
            
            cropped_page = page
            if page_rotation == 90:
                cropped_page.cropbox.lower_left = (0, 0)
                cropped_page.cropbox.upper_right = (max_x, page_width)
            else:
                cropped_page.cropbox.lower_left = (0, pydf2_min_y)
                cropped_page.cropbox.upper_right = (page_width, page_height)
            
            writer.add_page(cropped_page)
            return
        
        if start_info and not end_info:
            page_num = start_info['page_number']
            page = reader.pages[page_num - 1]
            
            page_width, page_height = start_info['page_dimensions']
            min_x, min_y, max_x, max_y = start_info['keyword_box']
            page_rotation = page.rotation
            
            pydf2_max_y = page_height - min_y
            
            cropped_page = page
            if page_rotation == 90:
                cropped_page.cropbox.lower_left = (min_x, 0)
                cropped_page.cropbox.upper_right = (page_height, page_width)
            else:
                cropped_page.cropbox.lower_left = (0, 0)
                cropped_page.cropbox.upper_right = (page_width, pydf2_max_y)
            
            writer.add_page(cropped_page)
            
            if page_num < len(reader.pages):
                writer.add_page(reader.pages[page_num])
            return
        
        if start_info and end_info:
            if start_info['page_number'] > end_info['page_number']:
                self._add_lxr_slice_to_writer(reader, writer, {'start': None, 'end': end_info})
                return
            
            if start_info['page_number'] == end_info['page_number']:
                page = reader.pages[start_info['page_number'] - 1]
                page_width, page_height = start_info['page_dimensions']
                _, start_min_y, _, _ = start_info['keyword_box']
                _, _, _, end_max_y = end_info['keyword_box']
                page_rotation = page.rotation
                
                start_pydf2_max_y = page_height - start_min_y
                end_pydf2_min_y = page_height - end_max_y
                
                cropped_page = page
                if page_rotation == 90:
                    pass
                else:
                    cropped_page.cropbox.lower_left = (0, end_pydf2_min_y)
                    cropped_page.cropbox.upper_right = (page_width, start_pydf2_max_y)
                
                writer.add_page(cropped_page)
                return
            
            if start_info['page_number'] + 1 == end_info['page_number']:
                start_page = reader.pages[start_info['page_number'] - 1]
                page_width, page_height = start_info['page_dimensions']
                min_x, min_y, max_x, max_y = start_info['keyword_box']
                page_rotation = start_page.rotation
                
                pydf2_max_y = page_height - min_y
                
                cropped_start = start_page
                if page_rotation == 90:
                    cropped_start.cropbox.lower_left = (min_x, 0)
                    cropped_start.cropbox.upper_right = (page_height, page_width)
                else:
                    cropped_start.cropbox.lower_left = (0, 0)
                    cropped_start.cropbox.upper_right = (page_width, pydf2_max_y)
                
                writer.add_page(cropped_start)
                
                end_page = reader.pages[end_info['page_number'] - 1]
                page_width, page_height = end_info['page_dimensions']
                min_x, min_y, max_x, max_y = end_info['keyword_box']
                page_rotation = end_page.rotation
                
                if page_rotation == 90:
                    pydf2_min_y = page_height - max_x
                else:
                    pydf2_min_y = page_height - max_y
                
                cropped_end = end_page
                if page_rotation == 90:
                    cropped_end.cropbox.lower_left = (0, 0)
                    cropped_end.cropbox.upper_right = (max_x, page_width)
                else:
                    cropped_end.cropbox.lower_left = (0, pydf2_min_y)
                    cropped_end.cropbox.upper_right = (page_width, page_height)
                
                writer.add_page(cropped_end)
                return
            
            self._add_lxr_slice_to_writer(reader, writer, {'start': None, 'end': end_info})
            return
    
    def _add_ldrjs_slice_to_writer(self, reader: PdfReader, writer: PdfWriter, ldrjs_info: Dict):
        """添加领导人介绍切片到writer"""
        start_info = ldrjs_info.get('start')
        end_info = ldrjs_info.get('end')
        
        if not start_info and not end_info:
            return
        
        if not start_info and end_info:
            page_num = end_info['page_number']
            start_page = max(0, page_num - 6)
            for i in range(start_page, page_num - 1):
                writer.add_page(reader.pages[i])
            
            page = reader.pages[page_num - 1]
            page_width, page_height = end_info['page_dimensions']
            min_x, min_y, max_x, max_y = end_info['keyword_box']
            page_rotation = page.rotation
            
            if page_rotation == 90:
                pydf2_min_y = page_height - max_x
            else:
                pydf2_min_y = page_height - max_y
            
            cropped_page = page
            if page_rotation == 90:
                cropped_page.cropbox.lower_left = (0, 0)
                cropped_page.cropbox.upper_right = (max_x, page_width)
            else:
                cropped_page.cropbox.lower_left = (0, pydf2_min_y)
                cropped_page.cropbox.upper_right = (page_width, page_height)
            
            writer.add_page(cropped_page)
            return
        
        if start_info and not end_info:
            page_num = start_info['page_number']
            page = reader.pages[page_num - 1]
            
            page_width, page_height = start_info['page_dimensions']
            min_x, min_y, max_x, max_y = start_info['keyword_box']
            page_rotation = page.rotation
            
            pydf2_max_y = page_height - min_y
            
            cropped_page = page
            if page_rotation == 90:
                cropped_page.cropbox.lower_left = (min_x, 0)
                cropped_page.cropbox.upper_right = (page_height, page_width)
            else:
                cropped_page.cropbox.lower_left = (0, 0)
                cropped_page.cropbox.upper_right = (page_width, pydf2_max_y)
            
            writer.add_page(cropped_page)
            
            total_pages = len(reader.pages)
            end_page = min(page_num + 4, total_pages)
            for i in range(page_num, end_page):
                writer.add_page(reader.pages[i])
            return
        
        if start_info and end_info:
            if start_info['page_number'] > end_info['page_number']:
                self._add_ldrjs_slice_to_writer(reader, writer, {'start': None, 'end': end_info})
                return
            
            if start_info['page_number'] == end_info['page_number']:
                page = reader.pages[start_info['page_number'] - 1]
                page_width, page_height = start_info['page_dimensions']
                _, start_min_y, _, _ = start_info['keyword_box']
                _, _, _, end_max_y = end_info['keyword_box']
                page_rotation = page.rotation
                
                start_pydf2_max_y = page_height - start_min_y
                end_pydf2_min_y = page_height - end_max_y
                
                cropped_page = page
                if page_rotation == 90:
                    pass
                else:
                    cropped_page.cropbox.lower_left = (0, end_pydf2_min_y)
                    cropped_page.cropbox.upper_right = (page_width, start_pydf2_max_y)
                
                writer.add_page(cropped_page)
                return
            
            start_page = reader.pages[start_info['page_number'] - 1]
            page_width, page_height = start_info['page_dimensions']
            min_x, min_y, max_x, max_y = start_info['keyword_box']
            page_rotation = start_page.rotation
            
            pydf2_max_y = page_height - min_y
            
            cropped_start = start_page
            if page_rotation == 90:
                cropped_start.cropbox.lower_left = (min_x, 0)
                cropped_start.cropbox.upper_right = (page_height, page_width)
            else:
                cropped_start.cropbox.lower_left = (0, 0)
                cropped_start.cropbox.upper_right = (page_width, pydf2_max_y)
            
            writer.add_page(cropped_start)
            
            for i in range(start_info['page_number'], end_info['page_number'] - 1):
                writer.add_page(reader.pages[i])
            
            end_page = reader.pages[end_info['page_number'] - 1]
            page_width, page_height = end_info['page_dimensions']
            min_x, min_y, max_x, max_y = end_info['keyword_box']
            page_rotation = end_page.rotation
            
            if page_rotation == 90:
                pydf2_min_y = page_height - max_x
            else:
                pydf2_min_y = page_height - max_y
            
            cropped_end = end_page
            if page_rotation == 90:
                cropped_end.cropbox.lower_left = (0, 0)
                cropped_end.cropbox.upper_right = (max_x, page_width)
            else:
                cropped_end.cropbox.lower_left = (0, pydf2_min_y)
                cropped_end.cropbox.upper_right = (page_width, page_height)
            
            writer.add_page(cropped_end)
            return
    
    def _merge_slices(self, pdf_path: str, lxr_info: Dict, ldrjs_info: Dict) -> Optional[str]:
        """合并联系人和领导人介绍切片"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        reader = PdfReader(pdf_path)
        writer = PdfWriter()
        
        self._add_lxr_slice_to_writer(reader, writer, lxr_info)
        
        self._add_ldrjs_slice_to_writer(reader, writer, ldrjs_info)
        
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        output_path = os.path.join(self.output_dir, f"{base_name}.pdf")
        
        with open(output_path, "wb") as output_file:
            writer.write(output_file)
        
        return output_path
    
    def _process_combined(self, pdf_path: str, lxr_info: Dict, ldrjs_info: Dict) -> Optional[str]:
        """合并处理联系人和领导人介绍"""
        lxr_start = lxr_info.get('start')
        ldrjs_start = ldrjs_info.get('start')
        
        if not lxr_start and not ldrjs_start:
            logger.debug(f"联系人和领导人介绍都未找到关键词: {os.path.basename(pdf_path)}")
            return None
        
        if lxr_start and not ldrjs_start:
            logger.debug(f"只找到联系人关键词: {os.path.basename(pdf_path)}")
            return self._process_lxr_only(pdf_path, lxr_info)
        
        if not lxr_start and ldrjs_start:
            logger.debug(f"只找到领导人介绍关键词: {os.path.basename(pdf_path)}")
            return self._process_ldrjs_only(pdf_path, ldrjs_info)
        
        logger.debug(f"找到联系人和领导人介绍关键词，合并输出: {os.path.basename(pdf_path)}")
        return self._merge_slices(pdf_path, lxr_info, ldrjs_info)
    
    def process_pdf(self, pdf_path: str, keywords: Dict) -> Optional[str]:
        """处理PDF（联系人+领导人介绍合并输出）"""
        exchange_code = get_exchange_code(pdf_path)
        
        if 'lxr' in keywords:
            lxr_info = keywords['lxr']
            ldrjs_info = keywords['ldrjs']
        else:
            lxr_info = {'start': None, 'end': None}
            ldrjs_info = keywords
        
        if exchange_code == "bjs":
            return self._process_ldrjs_only(pdf_path, ldrjs_info)
        
        return self._process_combined(pdf_path, lxr_info, ldrjs_info)
