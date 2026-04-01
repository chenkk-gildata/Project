"""
领导人持股处理器
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


class LdrcgProcessor(BaseProcessor):
    """领导人持股处理器"""
    
    MODULE_NAME = "领导人持股"
    
    def find_keywords(self, pdf_path: str) -> Dict:
        """查找领导人持股关键词"""
        exchange_code = get_exchange_code(pdf_path)
        
        # 开始关键词(上深北一致)
        start_pattern = re.compile(
            r'^[（(]?[\d一二三四五六七八九]?[）)]?[、.．]?\s*(?:本行|公司)?董事(?:[和及、]监事)?[和及、]高级管理人员(?:的|基本)?情况\s*$',
            re.IGNORECASE | re.MULTILINE
        )
        
        # 根据交易所定义不同的结束和移除关键词
        if exchange_code == "bjs":
            end_pattern = re.compile(r'[（(]?[二三四五六123456][）)]?[、.．]?\s*股权激励情况\s*$', re.IGNORECASE | re.MULTILINE)
            remove_start_pattern = re.compile(r'[（(]?[一二三四五12345][）)]?[、.．]?\s*变动情况\s*$', re.IGNORECASE | re.MULTILINE)
            remove_end_pattern = re.compile(r'董事(?:[和及、]监事)?[和及、]高级管理人员.*决策程序.*确定依据以及实际支付情况', re.IGNORECASE | re.MULTILINE)
            end_keyword = ["股权激励情况"]
            remove_start_keyword = ["变动情况"]
            remove_end_keyword = ["实际支付"]
        elif exchange_code in ["shs", "kcb"]:
            end_pattern = re.compile(
                r'^[（(]?[\d一二三四五六七八九]?[）)]?[、.．]?\s*(公司)?董事(?:、监事)?[和及、]高级管理人员(?:和核心技术人员)?变动情况\s*$',
                re.IGNORECASE | re.MULTILINE
            )
            remove_start_pattern = re.compile(r'主要工作经历', re.IGNORECASE | re.MULTILINE)
            remove_end_pattern = re.compile(r'董事(?:、监事)?[和及、]高级管理人员(?:和核心技术人员)?(?:报酬|薪酬)情况\s*$', re.IGNORECASE | re.MULTILINE)
            end_keyword = ["变动情况"]
            remove_start_keyword = ["主要工作经历"]
            remove_end_keyword = ["报酬情况", "薪酬情况"]
        else:  # 深交所
            end_pattern = re.compile(
                r'^[（(]?[\d二三四五六七八九][）)]?[、.．]?\s*(报告期内)?.*董事.*(履行职责的|履职|有关)情况\s*$',
                re.IGNORECASE | re.MULTILINE
            )
            # 24年报移除开始关键词
            #remove_start_pattern = re.compile(r'报告期.*任期内董事(?:、监事离任)?和高级管理人员解聘的情况', re.IGNORECASE)
            # 25年报移除开始关键词
            remove_start_pattern = re.compile(r'报告期是否存在任期内董事和高级管理人员离任的情况', re.IGNORECASE | re.MULTILINE)
            remove_end_pattern = re.compile(r'董事(?:、监事)?[和及、]高级管理人员(?:和核心技术人员)?(?:的|年度)?(?:报酬|薪酬)情况\s*$', re.IGNORECASE | re.MULTILINE)
            end_keyword = ["履行职责", "履职", "有关情况"]
            remove_start_keyword = ["高级管理人员离任"]
            remove_end_keyword = ["报酬情况", "薪酬情况"]
        
        doc = fitz.open(pdf_path)
        total_pages = len(doc)
        
        start_info = None
        end_info = None
        remove_start_info = None
        remove_end_info = None
        
        search_ranges = [(15, 50), (45, 70), (65, 120)]

        def get_search_rect(page, inst, keyword_type):
            """根据关键词类型和页面旋转角度智能调整搜索区域"""
            page_width = page.rect.width
            page_rotation = page.rotation

            if keyword_type in ['高级管理人员', '履行职责', '履职', '主要工作经历', '变动情况', '有关情况', '高级管理人员离任', '报酬情况', '薪酬情况', '股权激励情况', '实际支付']:
                # 处理旋转页面
                if page_rotation == 90:
                    return fitz.Rect(inst.x0 - 30, 0, inst.x1 + 50, page_width)
                else:
                    return fitz.Rect(0, inst.y0 - 30, page_width, inst.y1 + 50)
            else:
                return fitz.Rect(0, inst.y0 - 20, page_width, inst.y1 + 50)

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

                if not start_info:
                    result = collect_and_sort_instances(page, ["高级管理人员"], start_pattern, get_search_rect)
                    if result:
                        inst = result['inst']
                        start_info = {
                            'page_number': page_num + 1,
                            'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                            'page_dimensions': (page_rect.width, page_rect.height)
                        }

                if not end_info and start_info:
                    result = collect_and_sort_instances(page, end_keyword, end_pattern, get_search_rect)
                    if result:
                        inst = result['inst']
                        end_info = {
                            'page_number': page_num + 1,
                            'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                            'page_dimensions': (page_rect.width, page_rect.height)
                        }

                if start_info and not remove_start_info:
                    result = collect_and_sort_instances(page, remove_start_keyword, remove_start_pattern, get_search_rect)
                    if result:
                        inst = result['inst']
                        remove_start_info = {
                            'page_number': page_num + 1,
                            'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                            'page_dimensions': (page_rect.width, page_rect.height)
                        }

                if remove_start_info and not remove_end_info:
                    result = collect_and_sort_instances(page, remove_end_keyword, remove_end_pattern, get_search_rect)
                    if result:
                        inst = result['inst']
                        remove_end_info = {
                            'page_number': page_num + 1,
                            'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                            'page_dimensions': (page_rect.width, page_rect.height)
                        }
                
                if start_info and end_info and remove_start_info and remove_end_info:
                    found = True
                    break
            
            if found:
                break
        
        doc.close()
        
        result = {
            'start': start_info,
            'end': end_info,
            'remove_start': remove_start_info,
            'remove_end': remove_end_info
        }
        
        return result
    
    def process_pdf(self, pdf_path: str, keywords: Dict) -> Optional[str]:
        """处理领导人持股PDF"""
        from PyPDF2 import PdfReader, PdfWriter
        
        start_info = keywords.get('start')
        end_info = keywords.get('end')
        remove_start_info = keywords.get('remove_start')
        remove_end_info = keywords.get('remove_end')
        
        if not start_info and not end_info:
            logger.debug(f"领导人持股未找到任何关键词,跳过处理: {os.path.basename(pdf_path)}")
            return None
        
        if not start_info and end_info:
            logger.debug(f"领导人持股只找到结束关键词: {os.path.basename(pdf_path)}")
            return crop_page_before_keyword(pdf_path, end_info, self.output_dir, pre_pages=5)
        
        if start_info and not end_info:
            logger.debug(f"领导人持股只找到开始关键词: {os.path.basename(pdf_path)}")
            return crop_page_after_keyword(pdf_path, start_info, self.output_dir, post_pages=4)
        
        # 找到开始和结束关键词
        if start_info and end_info:
            # 开始页大于结束页
            if start_info['page_number'] > end_info['page_number']:
                return crop_page_before_keyword(pdf_path, end_info, self.output_dir, pre_pages=5)
            
            # 同一页
            if start_info['page_number'] == end_info['page_number']:
                return crop_same_page(pdf_path, start_info, end_info, self.output_dir)
            
            try:
                reader = PdfReader(pdf_path)
                writer = PdfWriter()
                
                if remove_start_info and remove_end_info:
                    for page_num in range(start_info['page_number'] - 1, end_info['page_number']):
                        page = reader.pages[page_num]

                        page_width, page_height = start_info['page_dimensions']
                        min_x, min_y, max_x, max_y = start_info['keyword_box']
                        if page_num == start_info['page_number'] - 1 and page_num != remove_start_info['page_number'] - 1:
                            pydf2_max_y = page_height - min_y

                            cropped_start_page = page
                            cropped_start_page.cropbox.lower_left = (0, 0)
                            cropped_start_page.cropbox.upper_right = (page_width, pydf2_max_y)

                            writer.add_page(cropped_start_page)
                            continue

                        if page_num + 1 == start_info['page_number'] == remove_start_info['page_number']:
                            rs_min_x, rs_min_y, rs_max_x, rs_max_y = remove_start_info['keyword_box']

                            pydf2_max_y = page_height - min_y
                            pydf2_min_y = page_height - rs_max_y

                            cropped_start_page = page
                            cropped_start_page.cropbox.lower_left = (0, pydf2_min_y)
                            cropped_start_page.cropbox.upper_right = (page_width, pydf2_max_y)

                            writer.add_page(cropped_start_page)
                            continue

                        if (remove_start_info and remove_end_info and
                                remove_start_info['page_number'] <= page_num + 1 <= remove_end_info['page_number']):
                            
                            if page_num + 1 == remove_start_info['page_number'] and page_num + 1 != start_info['page_number']:
                                page_width, page_height = remove_start_info['page_dimensions']
                                rs_min_x, rs_min_y, rs_max_x, rs_max_y = remove_start_info['keyword_box']
                                re_min_x, re_min_y, re_max_x, re_max_y = remove_end_info['keyword_box']

                                pydf2_min_y = page_height - rs_max_y
                                pydf2_max_y = page_height - re_min_y
                                
                                cropped_page = page

                                if page_num + 1 == remove_end_info['page_number']:
                                    cropped_page.cropbox.lower_left = (0, pydf2_min_y)
                                    cropped_page.cropbox.upper_right = (page_width, page_height)
                                    writer.add_page(cropped_page)
                                    
                                    cropped_page2 = page
                                    cropped_page2.cropbox.lower_left = (0, 0)
                                    cropped_page2.cropbox.upper_right = (page_width, pydf2_max_y)
                                    writer.add_page(cropped_page2)
                                    continue
                                
                                else:
                                    cropped_page.cropbox.lower_left = (0, pydf2_min_y)
                                    cropped_page.cropbox.upper_right = (page_width, page_height)
                                    
                                    writer.add_page(cropped_page)
                                
                            elif page_num + 1 == remove_end_info['page_number']:
                                page_width, page_height = remove_end_info['page_dimensions']
                                remove_min_x, remove_min_y, remove_max_x, remove_max_y = remove_end_info['keyword_box']
                                min_x, min_y, max_x, max_y = end_info['keyword_box']

                                pydf2_max_y = page_height - max_y
                                pydf2_min_y = page_height - remove_min_y

                                cropped_page = page

                                if page_num + 1 == end_info['page_number']:
                                    cropped_page.cropbox.lower_left = (0, pydf2_max_y)
                                    cropped_page.cropbox.upper_right = (page_width, pydf2_min_y)
                                    writer.add_page(cropped_page)
                                    break
                                else:
                                    cropped_page.cropbox.lower_left = (0, 0)
                                    cropped_page.cropbox.upper_right = (page_width, pydf2_min_y)
                                    writer.add_page(cropped_page)
                            
                            else:
                                continue

                        elif page_num + 1 == end_info['page_number']:
                            page_width, page_height = end_info['page_dimensions']
                            min_x, min_y, max_x, max_y = end_info['keyword_box']

                            pydf2_max_y = page_height - max_y

                            cropped_page = page
                            cropped_page.cropbox.lower_left = (0, pydf2_max_y)
                            cropped_page.cropbox.upper_right = (page_width, page_height)

                            writer.add_page(cropped_page)

                        else:
                            writer.add_page(page)
                
                base_name = os.path.splitext(os.path.basename(pdf_path))[0]
                os.makedirs(self.output_dir, exist_ok=True)
                output_path = os.path.join(self.output_dir, f"{base_name}.pdf")
                
                with open(output_path, "wb") as output_file:
                    writer.write(output_file)
                
                logger.debug(f"领导人持股处理成功: {os.path.basename(pdf_path)}")
                return output_path
            except Exception as e:
                logger.error(f"处理领导人持股PDF时出错: {e}")
                return None
        
        return None
