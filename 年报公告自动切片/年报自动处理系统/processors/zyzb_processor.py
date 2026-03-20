"""
主要指标处理器
"""
import os
import re
import fitz
from PyPDF2 import PdfReader, PdfWriter
from typing import Dict, Optional

from processors.base_processor import BaseProcessor
from utils.pdf_utils import (
    is_bse_pdf, crop_page_before_keyword, crop_page_after_keyword,
    crop_same_page
)
from logger import logger


class ZyzbProcessor(BaseProcessor):
    """主要指标处理器"""
    
    MODULE_NAME = "主要指标"
    
    def find_keywords(self, pdf_path: str) -> Dict:
        """查找主要指标关键词"""
        is_bse = is_bse_pdf(pdf_path)
        
        if is_bse:
            # 北交所关键词
            start_pattern = re.compile(r'第三节\s*会计数据和财务指标', re.IGNORECASE | re.MULTILINE)
            end_pattern = re.compile(r'补充财务指标', re.IGNORECASE | re.MULTILINE)
            remove_start_pattern = re.compile(r'业绩快报.*财务数据差异', re.IGNORECASE | re.MULTILINE)
            remove_end_pattern = re.compile(r'非经常性损益项目和金额', re.IGNORECASE | re.MULTILINE)
        else:
            # 沪深关键词
            start_pattern = re.compile(r'主要(?:会计|财务|财务会计)数据[及和与]?财务指标?', re.IGNORECASE | re.MULTILINE)
            end_pattern = re.compile(r'第?[一二三四五六七八九十]?[节章]?[、.．]?\s*(?:其他符合非经常性损益定义的损益项目的具体情况|《公开发行证券的公司信息披露解释性公告.*非经常性损益》|董事会报告)', re.IGNORECASE | re.MULTILINE)
            remove_start_pattern = None
            remove_end_pattern = None
        
        doc = fitz.open(pdf_path)
        total_pages = len(doc)
        
        start_info = None
        end_info = None
        remove_start_info = None
        remove_end_info = None

        # 搜索范围
        search_ranges = [(4, 10), (9, 20), (19, 30), (29, 50)]

        def get_search_rect(page, inst, keyword_type):
            """根据关键词类型和页面旋转角度智能调整搜索区域"""
            page_width = page.rect.width
            page_rotation = page.rotation

            if keyword_type in ['会计数据', '财务指标', '具体情况', '解释性公告', '董事会报告', '财务数据差异', '非经常性损益']:
                # 处理旋转页面
                if page_rotation == 90:
                    return fitz.Rect(inst.x0 - 30, 0, inst.x1 + 50, page_width)
                else:
                    return fitz.Rect(0, inst.y0 - 30, page_width, inst.y1 + 50)
            else:
                # 默认搜索区域
                return fitz.Rect(0, inst.y0 - 20, inst.x1 + 400, inst.y1 + 50)

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

                if not start_info:
                    for keyword in ['会计数据', '财务指标']:
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
                    end_keywords = ["具体情况", "解释性公告", "董事会报告"] if not is_bse else ["财务指标"]
                    for keyword in end_keywords:
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

                # 北交所特殊处理
                if is_bse:
                    if not remove_start_info:
                        instances = page.search_for("财务数据差异")
                        for inst in instances:
                            rect = get_search_rect(page, inst, "财务数据差异")
                            text = page.get_text("text", clip=rect)
                            if remove_start_pattern.search(text):
                                remove_start_info = {
                                    'page_number': page_num + 1,
                                    'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                    'page_dimensions': (page_rect.width, page_rect.height)
                                }
                                break

                    if remove_start_info and not remove_end_info:
                        instances = page.search_for("非经常性损益")
                        for inst in instances:
                            rect = get_search_rect(page, inst, "非经常性损益")
                            text = page.get_text("text", clip=rect)
                            if remove_end_pattern.search(text):
                                remove_end_info = {
                                    'page_number': page_num + 1,
                                    'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                    'page_dimensions': (page_rect.width, page_rect.height)
                                }
                                break
                
                # 检查是否找到所有关键词
                if is_bse:
                    if start_info and end_info and remove_start_info and remove_end_info:
                        found = True
                        break
                else:
                    if start_info and end_info:
                        found = True
                        break
            
            if found:
                break
        
        append_start_info = None
        append_end_info = None
        
        append_start_pattern = re.compile(r'净资产收益率[及和]每股收益', re.IGNORECASE | re.MULTILINE)
        append_start_keyword = "每股收益"
        append_end_pattern = re.compile(r'境内外会计准则下会计数据差异\s*$|资产收益率.*计算过程', re.IGNORECASE | re.MULTILINE)
        append_end_keywords = ["计算", "会计准则"]
        
        append_search_start = max(1, total_pages - 15)
        
        for page_num in range(total_pages - 1, append_search_start - 1, -1):
            page = doc.load_page(page_num)
            page_rect = page.rect
            
            if not append_start_info:
                for append_end_keyword in append_end_keywords:
                    instances = page.search_for(append_end_keyword)
                    for inst in instances:
                        rect = get_search_rect(page, inst, append_end_keyword)
                        text = page.get_text("text", clip=rect)
                        if append_end_pattern.search(text):
                            append_end_info = {
                                'page_number': page_num + 1,
                                'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                'page_dimensions': (page_rect.width, page_rect.height)
                            }
                            break
                    if append_end_info:
                        break
            
            if not append_start_info:
                instances = page.search_for(append_start_keyword)
                for inst in instances:
                    rect = get_search_rect(page, inst, append_start_keyword)
                    text = page.get_text("text", clip=rect)
                    if append_start_pattern.search(text):
                        append_start_info = {
                            'page_number': page_num + 1,
                            'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                            'page_dimensions': (page_rect.width, page_rect.height)
                        }
                        break
            
            if append_start_info and append_end_info:
                break
        
        doc.close()
        
        result = {
            'start': start_info,
            'end': end_info,
            'is_bse': is_bse,
            'append_start': append_start_info,
            'append_end': append_end_info
        }
        if is_bse:
            result['remove_start'] = remove_start_info
            result['remove_end'] = remove_end_info
        
        return result
    
    def _extract_append_pages(self, pdf_path: str, append_start_info: Dict, append_end_info: Dict) -> list:
        """
        提取追加内容页面
        
        Args:
            pdf_path: PDF文件路径
            append_start_info: 开始关键词信息
            append_end_info: 结束关键词信息
        
        Returns:
            list: 页面列表
        """
        reader = PdfReader(pdf_path)
        total_pages = len(reader.pages)
        append_pages = []
        
        if not append_start_info and append_end_info:
            end_page = append_end_info['page_number']
            start_page = max(1, end_page - 1)
            for i in range(start_page - 1, end_page):
                page = reader.pages[i]
                if i == end_page - 1:
                    page_width, page_height = append_end_info['page_dimensions']
                    _, _, _, max_y = append_end_info['keyword_box']
                    pydf2_max_y = page_height - max_y
                    page.cropbox.lower_left = (0, pydf2_max_y)
                    page.cropbox.upper_right = (page_width, page_height)
                append_pages.append(page)
        
        elif append_start_info and not append_end_info:
            start_page = append_start_info['page_number']
            end_page = min(total_pages, start_page + 1)
            for i in range(start_page - 1, end_page):
                page = reader.pages[i]
                if i == start_page - 1:
                    page_width, page_height = append_start_info['page_dimensions']
                    _, min_y, _, _ = append_start_info['keyword_box']
                    pydf2_max_y = page_height - min_y
                    page.cropbox.lower_left = (0, 0)
                    page.cropbox.upper_right = (page_width, pydf2_max_y)
                append_pages.append(page)
        
        elif append_start_info and append_end_info:
            start_page = append_start_info['page_number']
            end_page = append_end_info['page_number']
            
            if start_page > end_page:
                target_page = max(1, end_page - 1)
                append_pages.append(reader.pages[target_page - 1])
            
            elif start_page == end_page:
                page = reader.pages[start_page - 1]
                page_width, page_height = append_start_info['page_dimensions']
                _, start_min_y, _, _ = append_start_info['keyword_box']
                _, _, _, end_max_y = append_end_info['keyword_box']
                
                pydf2_start_max_y = page_height - start_min_y
                pydf2_end_max_y = page_height - end_max_y
                
                page.cropbox.lower_left = (0, pydf2_end_max_y)
                page.cropbox.upper_right = (page_width, pydf2_start_max_y)
                append_pages.append(page)
            
            else:
                for i in range(start_page - 1, end_page):
                    page = reader.pages[i]
                    
                    if i == start_page - 1:
                        page_width, page_height = append_start_info['page_dimensions']
                        _, min_y, _, _ = append_start_info['keyword_box']
                        pydf2_max_y = page_height - min_y
                        page.cropbox.lower_left = (0, 0)
                        page.cropbox.upper_right = (page_width, pydf2_max_y)
                    
                    elif i == end_page - 1:
                        page_width, page_height = append_end_info['page_dimensions']
                        _, _, _, max_y = append_end_info['keyword_box']
                        pydf2_max_y = page_height - max_y
                        page.cropbox.lower_left = (0, pydf2_max_y)
                        page.cropbox.upper_right = (page_width, page_height)
                    
                    append_pages.append(page)
        
        return append_pages
    
    def _append_content_to_pdf(self, existing_pdf_path: str, source_pdf_path: str, 
                                append_start_info: Dict, append_end_info: Dict) -> Optional[str]:
        """
        将追加内容合并到已存在的PDF中
        
        Args:
            existing_pdf_path: 已存在的PDF路径
            source_pdf_path: 源PDF路径
            append_start_info: 开始关键词信息
            append_end_info: 结束关键词信息
        
        Returns:
            Optional[str]: 输出文件路径
        """
        try:
            append_pages = self._extract_append_pages(source_pdf_path, append_start_info, append_end_info)
            if not append_pages:
                return existing_pdf_path
            
            existing_reader = PdfReader(existing_pdf_path)
            writer = PdfWriter()
            
            for page in existing_reader.pages:
                writer.add_page(page)
            
            for page in append_pages:
                writer.add_page(page)
            
            base_name = os.path.splitext(os.path.basename(existing_pdf_path))[0]
            output_path = os.path.join(self.output_dir, f"{base_name}.pdf")
            
            with open(output_path, "wb") as output_file:
                writer.write(output_file)
            
            return output_path
        except Exception as e:
            logger.error(f"追加内容到PDF时出错: {e}")
            return existing_pdf_path

    def _save_append_content_pdf(self, pdf_path: str, append_start_info: Dict, append_end_info: Dict) -> Optional[str]:
        """
        将净资产收益率/每股收益模块单独保存为PDF

        Args:
            pdf_path: 源PDF路径
            append_start_info: 开始关键词信息
            append_end_info: 结束关键词信息

        Returns:
            Optional[str]: 输出文件路径，如果无内容则返回None
        """
        try:
            append_pages = self._extract_append_pages(pdf_path, append_start_info, append_end_info)
            if not append_pages:
                logger.debug(f"没有找到净资产收益率/每股收益内容: {os.path.basename(pdf_path)}")
                return None

            mgsy_dir = os.path.join(self.output_dir, "净资产收益率和每股收益")
            if not os.path.exists(mgsy_dir):
                os.makedirs(mgsy_dir)

            base_name = os.path.splitext(os.path.basename(pdf_path))[0]
            output_path = os.path.join(mgsy_dir, f"{base_name}_mgsy.pdf")

            writer = PdfWriter()
            for page in append_pages:
                writer.add_page(page)

            with open(output_path, "wb") as output_file:
                writer.write(output_file)

            logger.debug(f"净资产收益率/每股收益内容已保存到: {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"保存净资产收益率/每股收益内容时出错: {e}")
            return None

    def process_pdf(self, pdf_path: str, keywords: Dict) -> Optional[str]:
        """处理主要指标PDF"""
        from PyPDF2 import PdfReader, PdfWriter
        
        start_info = keywords.get('start')
        end_info = keywords.get('end')
        remove_start_info = keywords.get('remove_start')
        remove_end_info = keywords.get('remove_end')
        is_bse = keywords.get('is_bse', False)
        append_start_info = keywords.get('append_start')
        append_end_info = keywords.get('append_end')
        
        if not start_info and not end_info:
            logger.debug(f"主要指标未找到任何关键词,跳过处理: {os.path.basename(pdf_path)}")
            return None
        
        if not start_info and end_info:
            logger.debug(f"主要指标只找到结束关键词: {os.path.basename(pdf_path)}")
            output_path = crop_page_before_keyword(pdf_path, end_info, self.output_dir, pre_pages=4)
            if output_path and (append_start_info or append_end_info):
                self._save_append_content_pdf(pdf_path, append_start_info, append_end_info)
            return output_path

        if start_info and not end_info:
            logger.debug(f"主要指标只找到开始关键词: {os.path.basename(pdf_path)}")
            output_path = crop_page_after_keyword(pdf_path, start_info, self.output_dir, post_pages=4)
            if output_path and (append_start_info or append_end_info):
                self._save_append_content_pdf(pdf_path, append_start_info, append_end_info)
            return output_path

        if start_info and end_info:
            if start_info['page_number'] > end_info['page_number']:
                output_path = crop_page_before_keyword(pdf_path, end_info, self.output_dir, pre_pages=4)
                if output_path and (append_start_info or append_end_info):
                    self._save_append_content_pdf(pdf_path, append_start_info, append_end_info)
                return output_path

            if start_info['page_number'] == end_info['page_number']:
                output_path = crop_same_page(pdf_path, start_info, end_info, self.output_dir)
                if output_path and (append_start_info or append_end_info):
                    self._save_append_content_pdf(pdf_path, append_start_info, append_end_info)
                return output_path
            
            # 情况4.3: 正常情况
            try:
                reader = PdfReader(pdf_path)
                writer = PdfWriter()
                
                # 如果是北交所PDF，需要处理中间内容移除
                if is_bse and remove_start_info and remove_end_info:
                    # 添加从开始关键词页到结束关键词页的所有页面，但需要处理移除区域
                    for page_num in range(start_info['page_number'] - 1, end_info['page_number']):
                        page = reader.pages[page_num]

                        # 处理开始关键词页的裁剪
                        if page_num == start_info['page_number'] - 1 and page_num != remove_start_info['page_number'] - 1:
                            page_width, page_height = start_info['page_dimensions']
                            min_x, min_y, max_x, max_y = start_info['keyword_box']

                            # 转换y坐标
                            pydf2_max_y = page_height - min_y

                            # 创建裁剪后的页面
                            cropped_start_page = page
                            cropped_start_page.cropbox.lower_left = (0, 0)
                            cropped_start_page.cropbox.upper_right = (page_width, pydf2_max_y)

                            # 添加裁剪后的开始关键词页
                            writer.add_page(cropped_start_page)
                            continue

                        # 如果是移除区域内的页面，需要进行特殊处理
                        if (remove_start_info and remove_end_info and
                                remove_start_info['page_number'] <= page_num + 1 <= remove_end_info['page_number']):
                            
                            # 如果是移除开始页，只保留移除开始关键词之前的内容
                            if page_num + 1 == remove_start_info['page_number']:
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
                    # 沪深PDF处理逻辑，不需要处理中间内容移除
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

                if append_start_info or append_end_info:
                    self._save_append_content_pdf(pdf_path, append_start_info, append_end_info)

                base_name = os.path.splitext(os.path.basename(pdf_path))[0]
                os.makedirs(self.output_dir, exist_ok=True)
                output_path = os.path.join(self.output_dir, f"{base_name}.pdf")
                
                with open(output_path, "wb") as output_file:
                    writer.write(output_file)
                
                logger.debug(f"主要指标处理成功: {os.path.basename(pdf_path)}")
                return output_path
            except Exception as e:
                logger.error(f"处理主要指标PDF时出错: {e}")
                return None
        
        return None
