"""
PDF切片处理模块 - 支持并发处理
"""
import os
import re
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional
import logging
from pathlib import Path

import fitz
from PyPDF2 import PdfReader, PdfWriter

logger = logging.getLogger(__name__)


class PDFProcessor:
    """PDF切片处理器"""

    def __init__(self):
        self.lock = threading.Lock()
        self.processed_count = 0
        self.success_count = 0
        self.failed_count = 0
        self.is_processing = False

    def is_bse_pdf(self, pdf_path: str) -> bool:
        """根据文件名判断是否为北交所PDF公告"""
        filename = os.path.basename(pdf_path)
        if len(filename) >= 6:
            prefix = filename[:6]
            return prefix.startswith("92") or prefix.startswith("8") or prefix.startswith("4")
        return False

    def find_keywords(self, pdf_path: str) -> Dict[str, Any]:
        """使用PyMuPDF查找PDF文件中开始和结束关键词所在的页码和精确位置"""
        is_bse = self.is_bse_pdf(pdf_path)
        
        if is_bse:
            start_pattern = re.compile(r'主要财务数据|主要会计数据[及和]财务指标', re.IGNORECASE | re.MULTILINE)
            end_pattern = re.compile(r'前十名股东情况', re.IGNORECASE | re.MULTILINE)
            remove_start_pattern = re.compile(r'财务数据重大变动原因', re.IGNORECASE | re.MULTILINE)
            remove_end_pattern = re.compile(r'非经常性损益项目和金额', re.IGNORECASE | re.MULTILINE)
        else:
            start_pattern = re.compile(r'[（(]?[一1][）)]?[、.．]?\s*主要财务数据|主要会计数据[及和]财务指标', re.IGNORECASE | re.MULTILINE)
            end_pattern = re.compile(r'[（(]?[三3][）)]?[、.．]?\s*主要会计数据.*变动.*原因', re.IGNORECASE | re.MULTILINE)
            remove_start_pattern = None
            remove_end_pattern = None
        
        def get_search_rect(inst, keyword_type):
            """根据关键词类型智能调整搜索区域"""
            if keyword_type in ['三', '发生变动的情况', '前十名股东情况']:
                return fitz.Rect(inst.x0, inst.y0 - 20, inst.x1 + 400, inst.y1 + 50)
            elif keyword_type in ['一', '主要财务数据', '主要会计数据']:
                return fitz.Rect(inst.x0, inst.y0 - 20, inst.x1 + 400, inst.y1 + 50)
            elif keyword_type in ['财务数据重大变动原因', '非经常性损益']:
                return fitz.Rect(inst.x0, inst.y0 - 20, inst.x1 + 400, inst.y1 + 50)
            else:
                return fitz.Rect(inst.x0 - 100, inst.y0 - 50, inst.x1 + 100, inst.y1 + 20)
        
        try:
            doc = fitz.open(pdf_path)
            
            start_info = None
            end_info = None
            remove_start_info = None
            remove_end_info = None
            
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                page_rect = page.rect
                
                if not start_info:
                    start_candidates = []
                    for keyword in ["一", "主要财务数据", "主要会计数据"]:
                        instances = page.search_for(keyword)
                        for inst in instances:
                            start_candidates.append((keyword, inst))
                    
                    start_candidates.sort(key=lambda item: (item[1].y0, item[1].x0))
                    
                    for keyword_type, inst in start_candidates:
                        rect = get_search_rect(inst, keyword_type)
                        text = page.get_text("text", clip=rect)
                        if start_pattern.search(text):
                            start_info = {
                                'page_number': page_num + 1,
                                'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                'page_dimensions': (page_rect.width, page_rect.height)
                            }
                            break
                
                if not end_info:
                    end_candidates = []
                    end_keywords = ["三", "发生变动的情况"] if not is_bse else ["前十名股东情况"]
                    for keyword in end_keywords:
                        instances = page.search_for(keyword)
                        for inst in instances:
                            end_candidates.append((keyword, inst))
                    
                    end_candidates.sort(key=lambda item: (item[1].y0, item[1].x0))
                    
                    for keyword_type, inst in end_candidates:
                        rect = get_search_rect(inst, keyword_type)
                        text = page.get_text("text", clip=rect)
                        if end_pattern.search(text):
                            end_info = {
                                'page_number': page_num + 1,
                                'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                'page_dimensions': (page_rect.width, page_rect.height)
                            }
                            break
                
                if is_bse:
                    if not remove_start_info:
                        remove_start_candidates = []
                        instances = page.search_for("财务数据重大变动原因")
                        for inst in instances:
                            remove_start_candidates.append(("财务数据重大变动原因", inst))
                        
                        remove_start_candidates.sort(key=lambda item: (item[1].y0, item[1].x0))
                        
                        for keyword_type, inst in remove_start_candidates:
                            rect = get_search_rect(inst, keyword_type)
                            text = page.get_text("text", clip=rect)
                            if remove_start_pattern.search(text):
                                remove_start_info = {
                                    'page_number': page_num + 1,
                                    'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                    'page_dimensions': (page_rect.width, page_rect.height)
                                }
                                break
                    
                    if not remove_end_info:
                        remove_end_candidates = []
                        instances = page.search_for("非经常性损益")
                        for inst in instances:
                            remove_end_candidates.append(("非经常性损益", inst))
                        
                        remove_end_candidates.sort(key=lambda item: (item[1].y0, item[1].x0))
                        
                        for keyword_type, inst in remove_end_candidates:
                            rect = get_search_rect(inst, keyword_type)
                            text = page.get_text("text", clip=rect)
                            if remove_end_pattern.search(text):
                                remove_end_info = {
                                    'page_number': page_num + 1,
                                    'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                    'page_dimensions': (page_rect.width, page_rect.height)
                                }
                                break
                
                if is_bse:
                    if start_info and end_info and remove_start_info and remove_end_info:
                        break
                else:
                    if start_info and end_info:
                        break
            
            doc.close()
            
            if is_bse:
                return {
                    'start': start_info,
                    'end': end_info,
                    'remove_start': remove_start_info,
                    'remove_end': remove_end_info,
                    'is_bse': True
                }
            else:
                return {
                    'start': start_info,
                    'end': end_info,
                    'is_bse': False
                }
            
        except Exception as e:
            logger.error(f"处理文件 {os.path.basename(pdf_path)} 时出错: {str(e)}")
            if is_bse:
                return {
                    'start': None,
                    'end': None,
                    'remove_start': None,
                    'remove_end': None,
                    'is_bse': True
                }
            else:
                return {
                    'start': None,
                    'end': None,
                    'is_bse': False
                }

    def crop_page_at_keyword(self, pdf_path: str, keyword_info: Dict, output_path: str) -> Optional[str]:
        """根据关键词位置裁剪PDF页面，保留关键词及之前的内容"""
        try:
            reader = PdfReader(pdf_path)
            
            page = reader.pages[keyword_info['page_number'] - 1]
            
            page_width, page_height = keyword_info['page_dimensions']
            min_x, min_y, max_x, max_y = keyword_info['keyword_box']

            pydf2_min_y = page_height - max_y
            
            cropped_page = page
            cropped_page.cropbox.lower_left = (0, pydf2_min_y)
            cropped_page.cropbox.upper_right = (page_width, page_height)
            
            writer = PdfWriter()
            
            for i in range(keyword_info['page_number'] - 1):
                writer.add_page(reader.pages[i])
            
            writer.add_page(cropped_page)
            
            with open(output_path, "wb") as output_file:
                writer.write(output_file)
            
            logger.info(f"已裁剪页面并保存到: {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"裁剪页面时出错: {str(e)}")
            return None

    def crop_same_page(self, pdf_path: str, start_info: Dict, end_info: Dict, output_path: str) -> Optional[str]:
        """在同一页裁剪，保留从开始关键词到结束关键词之间的内容"""
        try:
            reader = PdfReader(pdf_path)
            
            page_num = start_info['page_number'] - 1
            page = reader.pages[page_num]
            
            page_width, page_height = start_info['page_dimensions']
            start_min_x, start_min_y, start_max_x, start_max_y = start_info['keyword_box']
            end_min_x, end_min_y, end_max_x, end_max_y = end_info['keyword_box']

            start_pydf2_min_y = page_height - start_max_y
            end_pydf2_min_y = page_height - end_max_y
            
            cropped_page = page
            cropped_page.cropbox.lower_left = (0, end_pydf2_min_y)
            cropped_page.cropbox.upper_right = (page_width, page_height)
            
            writer = PdfWriter()
            
            writer.add_page(cropped_page)
            
            with open(output_path, "wb") as output_file:
                writer.write(output_file)
            
            logger.info(f"已裁剪页面并保存到: {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"裁剪页面时出错: {str(e)}")
            return None

    def process_bse_pdf(self, pdf_path: str, keywords: Dict, output_path: str) -> Optional[str]:
        """处理北交所PDF，包括中间内容移除逻辑"""
        start_info = keywords['start']
        end_info = keywords['end']
        remove_start_info = keywords['remove_start']
        remove_end_info = keywords['remove_end']
        
        if not start_info and not end_info:
            logger.info(f"在文件 {os.path.basename(pdf_path)} 中未找到任何关键词，保留原文件")
            shutil.copy2(pdf_path, output_path)
            return output_path
        
        if not start_info and end_info:
            logger.info(f"只找到结束关键词，按原逻辑处理")
            return self.crop_page_at_keyword(pdf_path, end_info, output_path)
        
        if start_info and not end_info:
            logger.info(f"只找到开始关键词，输出包含开始关键词页的后两页内容")
            try:
                reader = PdfReader(pdf_path)
                writer = PdfWriter()
                
                total_pages = len(reader.pages)
                end_page = min(start_info['page_number'] + 5, total_pages)
                
                for i in range(start_info['page_number'] - 1, end_page):
                    writer.add_page(reader.pages[i])
                
                with open(output_path, "wb") as output_file:
                    writer.write(output_file)
                
                logger.info(f"已处理并保存到: {output_path}")
                return output_path
            except Exception as e:
                logger.error(f"处理文件时出错: {str(e)}")
                return None
        
        if start_info and end_info:
            if start_info['page_number'] > end_info['page_number']:
                logger.info(f"开始关键词页码({start_info['page_number']})大于结束关键词页码({end_info['page_number']})，按只找到结束关键词处理")
                return self.crop_page_at_keyword(pdf_path, end_info, output_path)
            
            if start_info['page_number'] == end_info['page_number']:
                logger.info(f"开始关键词和结束关键词在同一页，直接裁剪该页面")
                return self.crop_same_page(pdf_path, start_info, end_info, output_path)
            
            try:
                reader = PdfReader(pdf_path)
                writer = PdfWriter()
                
                for page_num in range(start_info['page_number'] - 1, end_info['page_number']):
                    page = reader.pages[page_num]
                    
                    if (remove_start_info and remove_end_info and 
                        page_num + 1 >= remove_start_info['page_number'] and 
                        page_num + 1 <= remove_end_info['page_number']):
                        
                        if page_num + 1 == remove_start_info['page_number']:
                            page_width, page_height = remove_start_info['page_dimensions']
                            min_x, min_y, max_x, max_y = remove_start_info['keyword_box']
                            
                            pydf2_min_y = page_height - max_y
                            
                            cropped_page = page
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
                
                with open(output_path, "wb") as output_file:
                    writer.write(output_file)
                
                logger.info(f"已处理并保存到: {output_path}")
                return output_path
            except Exception as e:
                logger.error(f"处理文件时出错: {str(e)}")
                return None
        return None

    def process_shsz_pdf(self, pdf_path: str, keywords: Dict, output_path: str) -> Optional[str]:
        """处理沪深PDF，使用原有逻辑"""
        start_info = keywords['start']
        end_info = keywords['end']
        
        if not start_info and not end_info:
            logger.info(f"在文件 {os.path.basename(pdf_path)} 中未找到任何关键词，保留原文件")
            shutil.copy2(pdf_path, output_path)
            return output_path
        
        if not start_info and end_info:
            logger.info(f"只找到结束关键词，按原逻辑处理")
            return self.crop_page_at_keyword(pdf_path, end_info, output_path)
        
        if start_info and not end_info:
            logger.info(f"只找到开始关键词，输出包含开始关键词页的后两页内容")
            try:
                reader = PdfReader(pdf_path)
                writer = PdfWriter()
                
                total_pages = len(reader.pages)
                end_page = min(start_info['page_number'] + 2, total_pages)
                
                for i in range(start_info['page_number'] - 1, end_page):
                    writer.add_page(reader.pages[i])
                
                with open(output_path, "wb") as output_file:
                    writer.write(output_file)
                
                logger.info(f"已处理并保存到: {output_path}")
                return output_path
            except Exception as e:
                logger.error(f"处理文件时出错: {str(e)}")
                return None
        
        if start_info and end_info:
            if start_info['page_number'] > end_info['page_number']:
                logger.info(f"开始关键词页码({start_info['page_number']})大于结束关键词页码({end_info['page_number']})，按只找到结束关键词处理")
                return self.crop_page_at_keyword(pdf_path, end_info, output_path)
            
            if start_info['page_number'] == end_info['page_number']:
                logger.info(f"开始关键词和结束关键词在同一页，直接裁剪该页面")
                return self.crop_same_page(pdf_path, start_info, end_info, output_path)
            
            try:
                reader = PdfReader(pdf_path)
                writer = PdfWriter()
                
                for i in range(start_info['page_number'] - 1, end_info['page_number'] - 1):
                    writer.add_page(reader.pages[i])
                
                end_page = reader.pages[end_info['page_number'] - 1]
                page_width, page_height = end_info['page_dimensions']
                min_x, min_y, max_x, max_y = end_info['keyword_box']
                
                pydf2_min_y = page_height - max_y
                
                cropped_end_page = end_page
                cropped_end_page.cropbox.lower_left = (0, pydf2_min_y)
                cropped_end_page.cropbox.upper_right = (page_width, page_height)
                
                writer.add_page(cropped_end_page)
                
                with open(output_path, "wb") as output_file:
                    writer.write(output_file)
                
                logger.info(f"已处理并保存到: {output_path}")
                return output_path
            except Exception as e:
                logger.error(f"处理文件时出错: {str(e)}")
                return None
        return None

    def process_single_pdf(self, pdf_path: str) -> bool:
        """处理单个PDF文件，覆盖原文件"""
        if not self.is_processing:
            return False

        try:
            keywords = self.find_keywords(pdf_path)
            
            temp_output_path = pdf_path + '.temp'
            
            if keywords.get('is_bse', False):
                result = self.process_bse_pdf(pdf_path, keywords, temp_output_path)
            else:
                result = self.process_shsz_pdf(pdf_path, keywords, temp_output_path)
            
            if result and os.path.exists(temp_output_path):
                os.remove(pdf_path)
                os.rename(temp_output_path, pdf_path)
                
                with self.lock:
                    self.success_count += 1
                    self.processed_count += 1
                
                logger.info(f"处理成功: {os.path.basename(pdf_path)}")
                return True
            else:
                if os.path.exists(temp_output_path):
                    os.remove(temp_output_path)
                
                with self.lock:
                    self.failed_count += 1
                    self.processed_count += 1
                
                logger.warning(f"处理失败，保留原文件: {os.path.basename(pdf_path)}")
                return False
                
        except Exception as e:
            logger.error(f"处理文件异常 {os.path.basename(pdf_path)}: {str(e)}")
            
            with self.lock:
                self.failed_count += 1
                self.processed_count += 1
            
            return False

    def process_batch(self, pdf_files: List[str]) -> Tuple[int, int]:
        """批量处理PDF文件"""
        if not pdf_files:
            return 0, 0

        self.is_processing = True
        self.processed_count = 0
        self.success_count = 0
        self.failed_count = 0

        total_count = len(pdf_files)
        
        workers = min(8, total_count) if total_count > 10 else min(4, total_count)
        
        logger.info(f"开始批量处理 {total_count} 个PDF文件，使用 {workers} 个并发线程")
        print(f"\n开始处理 {total_count} 个PDF文件，使用 {workers} 个并发线程...")

        executor = ThreadPoolExecutor(max_workers=workers)
        
        try:
            futures = {
                executor.submit(self.process_single_pdf, pdf_file): pdf_file
                for pdf_file in pdf_files
            }

            for future in as_completed(futures):
                if not self.is_processing:
                    break
                
                if self.processed_count % 10 == 0:
                    print(f"处理进度: {self.processed_count}/{total_count} (成功: {self.success_count}, 失败: {self.failed_count})")

        except Exception as e:
            logger.error(f"批量处理过程中发生错误: {e}")
        finally:
            executor.shutdown(wait=True)
            self.is_processing = False

        print(f"\nPDF处理完成! 成功: {self.success_count}, 失败: {self.failed_count}")
        return self.success_count, self.failed_count

    def stop_processing(self):
        """停止处理"""
        self.is_processing = False
        logger.info("用户请求停止处理")


from typing import Tuple
