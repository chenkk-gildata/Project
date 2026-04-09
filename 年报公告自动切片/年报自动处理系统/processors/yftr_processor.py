"""
研发投入处理器
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


class YftrProcessor(BaseProcessor):
    """研发投入处理器"""
    
    MODULE_NAME = "研发投入"
    
    def find_keywords(self, pdf_path: str) -> Dict:
        """查找研发投入关键词"""
        exchange_code = get_exchange_code(pdf_path)
        
        # 定义不同交易所的关键词模式
        if exchange_code == "szs":
            start_pattern = re.compile(r'公司研发人员情况|^([\d三四五][、.．]?)+\s*研发投入\s*$', re.IGNORECASE | re.MULTILINE)
            end_pattern = re.compile(r'研发人员构成发生重大变化的原因及影响|([\d四五六][）)]?[、.．]?)+\s*现金流', re.IGNORECASE | re.MULTILINE)
            start_keyword = ["研发人员", "研发投入"]
            end_keyword = ["发生重大变化的原因", "现金流"]
        elif exchange_code == "shs":
            start_pattern = re.compile(r'^[\d三四五六][）)]?[、.．]?\s*(?:研发投入|研发支出)\s*$', re.IGNORECASE | re.MULTILINE)
            end_pattern = re.compile(r'[\d四五六七][）)]?[、.．]?\s*现金流', re.IGNORECASE | re.MULTILINE)
            start_keyword = ["研发投入", "研发支出"]
            end_keyword = ["现金流"]
        elif exchange_code == "bjs":
            start_pattern = re.compile(r'[（(]?[一二三四九12349][）)]?[、.．]?\s*研发支出情况', re.IGNORECASE | re.MULTILINE)
            end_pattern = re.compile(r'[\d一二三四五][）)]?[、.．]?\s*专利情况', re.IGNORECASE | re.MULTILINE)
            start_keyword = ["研发支出"]
            end_keyword = ["专利情况"]
        else:  # 科创板
            start_pattern = re.compile(r'\d?[）)]?[、.．]?\s*研发投入情况表', re.IGNORECASE | re.MULTILINE)
            end_pattern = re.compile(r'\d[、.．]?\s*其他说明', re.IGNORECASE | re.MULTILINE)
            remove_start_pattern = re.compile(r'研发.*总额较上年发生重大变化的原因', re.IGNORECASE | re.MULTILINE)
            remove_end_pattern = re.compile(r'\d[、.．]?\s*研发人员情况', re.IGNORECASE | re.MULTILINE)
            start_keyword = ["研发投入"]
            end_keyword = ["其他说明"]
            remove_start_keyword = ["发生重大变化的原因"]
            remove_end_keyword = ["研发人员情况"]
        
        doc = fitz.open(pdf_path)
        total_pages = len(doc)

        start_info = None
        end_info = None
        remove_start_info = None
        remove_end_info = None

        search_ranges = [(10, 40), (35, 100)]

        def get_search_rect(page, inst, keyword_type):
            """根据关键词类型和页面旋转角度智能调整搜索区域"""
            page_width = page.rect.width
            page_rotation = page.rotation

            if keyword_type in ['研发投入', '研发人员', '核心技术', '研发进展', '研发项目', '研发成果', '研发机构']:
                if page_rotation == 90:
                    return fitz.Rect(inst.x0 - 30, 0, inst.x1 + 50, page_width)
                else:
                    return fitz.Rect(0, inst.y0 - 30, page_width, inst.y1 + 50)
            else:
                return fitz.Rect(0, inst.y0 - 20, inst.x1 + 400, inst.y1 + 50)

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
                    result = collect_and_sort_instances(page, start_keyword, start_pattern, get_search_rect)
                    if result:
                        inst = result['inst']
                        start_info = {
                            'page_number': page_num + 1,
                            'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                            'page_dimensions': (page_rect.width, page_rect.height)
                        }

                if start_info and not end_info:
                    result = collect_and_sort_instances(page, end_keyword, end_pattern, get_search_rect)
                    if result:
                        inst = result['inst']
                        end_info = {
                            'page_number': page_num + 1,
                            'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                            'page_dimensions': (page_rect.width, page_rect.height)
                        }

                if exchange_code == 'kcb':
                    if not remove_start_info:
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
                
                # 检查是否完成
                if exchange_code == 'kcb':
                    if start_info and end_info and remove_start_info and remove_end_info:
                        found = True
                        break
                else:
                    if start_info and end_info:
                        found = True
                        break
            
            if found:
                break
        
        doc.close()
        
        result = {
            'start': start_info,
            'end': end_info,
            'is_kcb': exchange_code == 'kcb'
        }
        if exchange_code == 'kcb':
            result['remove_start'] = remove_start_info
            result['remove_end'] = remove_end_info
        
        return result
    
    def process_pdf(self, pdf_path: str, keywords: Dict) -> Optional[str]:
        """处理研发投入PDF"""
        from PyPDF2 import PdfReader, PdfWriter
        
        start_info = keywords.get('start')
        end_info = keywords.get('end')
        remove_start_info = keywords.get('remove_start')
        remove_end_info = keywords.get('remove_end')
        is_kcb = keywords.get('is_kcb', False)
        
        # 什么都没找到
        if not start_info and not end_info:
            logger.debug(f"研发投入未找到任何关键词,跳过处理: {os.path.basename(pdf_path)}")
            return None
        
        # 只找到结束关键词
        if not start_info and end_info:
            logger.debug(f"研发投入只找到结束关键词: {os.path.basename(pdf_path)}")
            return crop_page_before_keyword(pdf_path, end_info, self.output_dir, pre_pages=2)
        
        # 只找到开始关键词
        if start_info and not end_info:
            logger.debug(f"研发投入只找到开始关键词: {os.path.basename(pdf_path)}")
            return crop_page_after_keyword(pdf_path, start_info, self.output_dir, post_pages=2)
        
        # 找到开始和结束关键词
        if start_info and end_info:
            # 开始页大于结束页
            if start_info['page_number'] > end_info['page_number']:
                return crop_page_before_keyword(pdf_path, end_info, self.output_dir, pre_pages=2)
            
            # 同一页
            if start_info['page_number'] == end_info['page_number']:
                return crop_same_page(pdf_path, start_info, end_info, self.output_dir)
            
            # 情况4.3: 正常情况
            try:
                reader = PdfReader(pdf_path)
                writer = PdfWriter()
                
                # 如果是科创板PDF，需要处理中间内容移除
                if is_kcb and remove_start_info and remove_end_info:
                    # 添加从开始关键词页到结束关键词页的所有页面，但需要处理移除区域
                    for page_num in range(start_info['page_number'] - 1, end_info['page_number']):
                        page = reader.pages[page_num]

                        page_width, page_height = start_info['page_dimensions']
                        min_x, min_y, max_x, max_y = start_info['keyword_box']
                        # 处理开始关键词页的裁剪 start_info和remove_start_info不在一页的情况，裁剪保留开始关键词之后的内容
                        if page_num == start_info['page_number'] - 1 and page_num != remove_start_info['page_number'] - 1:
                            # 转换y坐标
                            pydf2_max_y = page_height - min_y

                            # 创建裁剪后的页面
                            cropped_start_page = page
                            cropped_start_page.cropbox.lower_left = (0, 0)
                            cropped_start_page.cropbox.upper_right = (page_width, pydf2_max_y)

                            # 添加裁剪后的开始关键词页
                            writer.add_page(cropped_start_page)
                            continue

                        # start_info和remove_start_info在一页的情况，裁剪保留开始关键词之后、移除开始关键词之前的内容
                        if page_num + 1 == start_info['page_number'] == remove_start_info['page_number']:
                            rs_min_x, rs_min_y, rs_max_x, rs_max_y = remove_start_info['keyword_box']

                            # 转换y坐标
                            pydf2_max_y = page_height - min_y
                            pydf2_min_y = page_height - rs_max_y

                            # 创建裁剪后的页面
                            cropped_start_page = page
                            cropped_start_page.cropbox.lower_left = (0, pydf2_min_y)
                            cropped_start_page.cropbox.upper_right = (page_width, pydf2_max_y)

                            # 添加裁剪后的开始关键词页
                            writer.add_page(cropped_start_page)
                            continue

                        # 如果是移除区域内的页面，需要进行特殊处理
                        if (remove_start_info and remove_end_info and
                                remove_start_info['page_number'] <= page_num + 1 <= remove_end_info['page_number']):
                            
                            # 如果是移除开始页，且跟开始关键词不在一页的情况，只保留移除开始关键词之前的内容
                            if page_num + 1 == remove_start_info['page_number'] and page_num + 1 != start_info['page_number']:
                                page_width, page_height = remove_start_info['page_dimensions']
                                rs_min_x, rs_min_y, rs_max_x, rs_max_y = remove_start_info['keyword_box']
                                re_min_x, re_min_y, re_max_x, re_max_y = remove_end_info['keyword_box']

                                # 转换y坐标
                                pydf2_min_y = page_height - rs_max_y
                                pydf2_max_y = page_height - re_min_y
                                
                                # 创建裁剪后的页面
                                cropped_page = page

                                # 如果移除开始页和移除结束页在同一页，保留移除开始关键词之前和移除结束关键词之后的内容
                                if page_num + 1 == remove_end_info['page_number']:
                                    # 移除开始关键词之前的内容
                                    cropped_page.cropbox.lower_left = (0, pydf2_min_y)
                                    cropped_page.cropbox.upper_right = (page_width, page_height)
                                    writer.add_page(cropped_page)
                                    
                                    # 移除移除结束关键词之后的内容
                                    cropped_page2 = page
                                    cropped_page2.cropbox.lower_left = (0, 0)
                                    cropped_page2.cropbox.upper_right = (page_width, pydf2_max_y)
                                    writer.add_page(cropped_page2)
                                    continue
                                
                                else:
                                    cropped_page.cropbox.lower_left = (0, pydf2_min_y)
                                    cropped_page.cropbox.upper_right = (page_width, page_height)
                                    
                                    writer.add_page(cropped_page)
                                
                            # 如果是移除结束页，判断是否和结束关键词一页
                            elif page_num + 1 == remove_end_info['page_number']:
                                page_width, page_height = remove_end_info['page_dimensions']
                                remove_min_x, remove_min_y, remove_max_x, remove_max_y = remove_end_info['keyword_box']
                                min_x, min_y, max_x, max_y = end_info['keyword_box']

                                # 转换y坐标
                                pydf2_max_y = page_height - max_y
                                pydf2_min_y = page_height - remove_min_y

                                # 创建裁剪后的页面
                                cropped_page = page

                                # 和结束关键词一页，保留移除结束关键词之后和结束关键词之前的内容
                                if page_num + 1 == end_info['page_number']:
                                    cropped_page.cropbox.lower_left = (0, pydf2_max_y)
                                    cropped_page.cropbox.upper_right = (page_width, pydf2_min_y)
                                    writer.add_page(cropped_page)
                                    break
                                # 不和结束关键词是一页，只保留结束关键词之后的内容
                                else:
                                    # 创建裁剪后的页面
                                    cropped_page.cropbox.lower_left = (0, 0)
                                    cropped_page.cropbox.upper_right = (page_width, pydf2_min_y)
                                    writer.add_page(cropped_page)
                            
                            # 如果是移除区域中间的页面，完全跳过
                            else:
                                continue

                        # 结束关键词页，只保留结束关键词之前的内容
                        elif page_num + 1 == end_info['page_number']:
                            page_width, page_height = end_info['page_dimensions']
                            min_x, min_y, max_x, max_y = end_info['keyword_box']

                            # 转换y坐标
                            pydf2_max_y = page_height - max_y

                            # 创建裁剪后的页面
                            cropped_page = page
                            cropped_page.cropbox.lower_left = (0, pydf2_max_y)
                            cropped_page.cropbox.upper_right = (page_width, page_height)

                            # 添加裁剪后的结束关键词页
                            writer.add_page(cropped_page)

                        else:
                            # 非移除区域的页面，完整添加
                            writer.add_page(page)
                else:
                    # 非科创板PDF处理逻辑，不需要处理中间内容移除
                    # 处理开始关键词页的裁剪
                    start_page = reader.pages[start_info['page_number'] - 1]
                    page_width, page_height = start_info['page_dimensions']
                    min_x, min_y, max_x, max_y = start_info['keyword_box']

                    # 转换y坐标
                    pydf2_max_y = page_height - min_y

                    # 创建裁剪后的页面
                    cropped_start_page = start_page
                    cropped_start_page.cropbox.lower_left = (0, 0)
                    cropped_start_page.cropbox.upper_right = (page_width, pydf2_max_y)

                    # 添加裁剪后的开始关键词页
                    writer.add_page(cropped_start_page)

                    # 只添加从开始关键词页到结束关键词前一页的所有完整页面
                    for i in range(start_info['page_number'], end_info['page_number'] - 1):
                        writer.add_page(reader.pages[i])
                    
                    # 处理结束关键词页的裁剪
                    end_page = reader.pages[end_info['page_number'] - 1]
                    page_width, page_height = end_info['page_dimensions']
                    min_x, min_y, max_x, max_y = end_info['keyword_box']
                    
                    # 转换y坐标
                    pydf2_min_y = page_height - max_y
                    
                    # 创建裁剪后的页面
                    cropped_end_page = end_page
                    cropped_end_page.cropbox.lower_left = (0, pydf2_min_y)
                    cropped_end_page.cropbox.upper_right = (page_width, page_height)
                    
                    # 添加裁剪后的结束关键词页
                    writer.add_page(cropped_end_page)
                
                # 保存新PDF
                base_name = os.path.splitext(os.path.basename(pdf_path))[0]
                os.makedirs(self.output_dir, exist_ok=True)
                output_path = os.path.join(self.output_dir, f"{base_name}.pdf")
                
                with open(output_path, "wb") as output_file:
                    writer.write(output_file)
                
                logger.debug(f"研发投入处理成功: {os.path.basename(pdf_path)}")
                return output_path
            except Exception as e:
                logger.error(f"处理研发投入PDF时出错: {e}")
                return None
        
        return None
