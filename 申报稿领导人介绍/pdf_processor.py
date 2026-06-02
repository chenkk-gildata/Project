"""
PDF切片处理模块 - 支持并发处理
"""
import os
import re
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple
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

    def get_exchange_code(self, pdf_path: str) -> Optional[str]:
        """根据文件名判断交易所类型

        文件名格式: {gpdm}-{rq}-{bt}-{zqbz}.{hz}
        zqbz映射:
            188 - 北交所(bjs)
            901 - 深交所(szs)
            906 - 创业板(cyb)
            831 - 上交所(shs)
            837 - 科创板(kcb)
        """
        filename = os.path.basename(pdf_path)
        name_without_ext = os.path.splitext(filename)[0]

        parts = name_without_ext.split('-')
        if len(parts) >= 4:
            zqbz = parts[-1]
            exchange_map = {
                '188': 'bjs',
                '901': 'szs',
                '906': 'cyb',
                '831': 'shs',
                '837': 'kcb'
            }
            return exchange_map.get(zqbz)

        return None

    def find_keywords(self, pdf_path: str) -> Dict[str, Any]:
        """使用PyMuPDF查找PDF文件中开始和结束关键词所在的页码和精确位置"""
        exchange_code = self.get_exchange_code(pdf_path)

        start_pattern = re.compile(
            r'^[（(]?[\d一二三四五六七八九十]+[)）]?[、.．]?\s*(?:发行人|公司)?董事.*(?:监事|审计委员会委员)?[、和与及]高级管理人员[、和与及]?(?:其他)?(?:核\s*心\s*(?:\s*技\s*术)?\s*人\s*员)?\s*(?:的?\s*(?:简\s*要|基\s*本)\s*情\s*况|(?:情\s*况(?:简\s*介)?|简\s*介))?\s*$',
            re.IGNORECASE | re.MULTILINE
        )
        end_pattern = re.compile(
            r'^[（(]?[\d一二三四五六七八九十]+[)）]?[、.．]?\s*(?:(?:发行人|公司)?董事.*(?:监事)?[、和与及]高级管理人员[、和与及]?(?:其他)?(?:核\s*心(?:\s*技\s*术)?\s*人\s*员)?\s*(?:在\s*其\s*他\s*单\s*位|对\s*外|的)?兼\s*职(?:/任职)?\s*情\s*况|直接或间接持有发行人股份的情况\s*$)',
            re.IGNORECASE | re.MULTILINE
        )

        if exchange_code == 'bjs':
            end_keyword = ["兼职", "职情况", "间接持有"]
        else:
            end_keyword = ["兼职", "职情况"]

        committee_start_pattern = re.compile(
            r'委员会.*(?:(?:建立健全|人员构成|设置|构成|制度)?及其?运行情况|设置情况(?:说明)?)\s*$',
            re.IGNORECASE | re.MULTILINE
        )
        committee_start_keyword = ["运行情况", "设置情况"]

        core_tech_start_pattern = None
        core_tech_start_keyword = None
        core_tech_end_pattern = None
        core_tech_end_keyword = None

        if exchange_code == 'bjs':
            core_tech_start_pattern = re.compile(
                r'^[（(]?[\d一二三四五六七八九十][)）]?[、.．]?\s*核心技术人员(?:总体|基本)?(?:情况|简历)\s*$',
                re.IGNORECASE | re.MULTILINE
            )
            core_tech_start_keyword = ["核心技术人员"]
            core_tech_end_pattern = re.compile(
                r'^(?!.*简历).*[（(]?[\d一二三四五六七八九十][)）]?[、.．]?\s*(?:核心技术人员)?.*(?:成果|持(?:股|有发行人|有公司).*情况)',
                re.IGNORECASE | re.MULTILINE
            )
            core_tech_end_keyword = ["成果", "持股", "持有"]

        search_ranges = [(35, 150)]

        def get_search_rect(page, inst, keyword_type):
            page_width = page.rect.width
            page_rotation = page.rotation

            if keyword_type in ['高级管理人员']:
                if page_rotation == 90:
                    return fitz.Rect(inst.x0 - 30, 0, inst.x1 + 50, page_width)
                else:
                    return fitz.Rect(0, inst.y0 - 30, page_width, inst.y1 + 50)
            else:
                return fitz.Rect(0, inst.y0 - 20, page_width, inst.y1 + 50)

        def collect_and_sort_instances(page, keywords, pattern, get_search_rect):
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

        try:
            doc = fitz.open(pdf_path)
            total_pages = len(doc)

            start_info = None
            end_info = None
            committee_info = None
            core_tech_start_info = None
            core_tech_end_info = None

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
                            logger.info(f"找到开始关键词在第 {page_num + 1} 页")

                    if not end_info and start_info:
                        result = collect_and_sort_instances(page, end_keyword, end_pattern, get_search_rect)
                        if result:
                            inst = result['inst']
                            end_info = {
                                'page_number': page_num + 1,
                                'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                'page_dimensions': (page_rect.width, page_rect.height)
                            }
                            logger.info(f"找到结束关键词在第 {page_num + 1} 页")

                    if start_info and end_info:
                        found = True
                        break

                if found:
                    break

            if exchange_code == 'bjs':
                committee_search_start = max(1, total_pages - 150)
            else:
                committee_search_start = total_pages

            for page_num in range(committee_search_start - 1, -1, -1):
                page = doc.load_page(page_num)
                page_rect = page.rect

                if not committee_info:
                    result = collect_and_sort_instances(page, committee_start_keyword, committee_start_pattern, get_search_rect)
                    if result:
                        inst = result['inst']
                        committee_info = {
                            'page_number': page_num + 1,
                            'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                            'page_dimensions': (page_rect.width, page_rect.height)
                        }
                        logger.info(f"找到委员会开始关键词在第 {page_num + 1} 页")
                        break

            if exchange_code == 'bjs':
                if committee_info:
                    core_tech_search_start = committee_info['page_number']
                else:
                    core_tech_search_start = committee_search_start

                for page_num in range(core_tech_search_start - 1, -1, -1):
                    page = doc.load_page(page_num)
                    page_rect = page.rect

                    if not core_tech_end_info:
                        result = collect_and_sort_instances(page, core_tech_end_keyword, core_tech_end_pattern, get_search_rect)
                        if result:
                            inst = result['inst']
                            core_tech_end_info = {
                                'page_number': page_num + 1,
                                'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                'page_dimensions': (page_rect.width, page_rect.height)
                            }
                            logger.info(f"找到核心技术人员结束关键词在第 {page_num + 1} 页")

                    if not core_tech_start_info and core_tech_end_info:
                        result = collect_and_sort_instances(page, core_tech_start_keyword, core_tech_start_pattern, get_search_rect)
                        if result:
                            inst = result['inst']
                            core_tech_start_info = {
                                'page_number': page_num + 1,
                                'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                'page_dimensions': (page_rect.width, page_rect.height)
                            }
                            logger.info(f"找到核心技术人员开始关键词在第 {page_num + 1} 页")
                            break

            doc.close()

            return {
                'start': start_info,
                'end': end_info,
                'committee': committee_info,
                'core_tech_start': core_tech_start_info,
                'core_tech_end': core_tech_end_info
            }

        except Exception as e:
            logger.error(f"处理文件 {os.path.basename(pdf_path)} 时出错: {str(e)}")
            return {
                'start': None,
                'end': None,
                'committee': None,
                'core_tech_start': None,
                'core_tech_end': None
            }

    def find_committee_additional_pages(self, pdf_path: str, start_page_number: int) -> List[int]:
        """从委员会开始页后一页开始，判断后续页面是否需要输出"""
        doc = fitz.open(pdf_path)
        total_pages = len(doc)
        additional_pages = []

        if start_page_number < total_pages:
            page_num = start_page_number
            page = doc.load_page(page_num)
            page_height = page.rect.height

            instances = page.search_for("委员会")

            if instances:
                first_instance = instances[0]
                if first_instance.y0 < page_height * 0.4:
                    additional_pages.append(page_num + 1)
                    logger.info(f"委员会后续页面第 {page_num + 1} 页包含'委员会'关键词且位置在40%以上，完整输出")
                else:
                    logger.info(f"委员会后续页面第 {page_num + 1} 页'委员会'关键词位置在40%以下，仅输出1页")
            else:
                logger.info(f"委员会后续页面第 {page_num + 1} 页不包含'委员会'关键词，仅输出1页")

        doc.close()
        return additional_pages

    def crop_page_with_rotation(self, page, keyword_info: Dict, crop_type: str = 'top'):
        """根据页面旋转角度裁剪页面

        参数:
            page: PyPDF2页面对象
            keyword_info: 关键词信息字典
            crop_type: 裁剪类型
                - 'top': 保留关键词及上方内容（裁剪下方）
                - 'bottom': 保留关键词及下方内容（裁剪上方）
        """
        page_rotation = page.rotation
        page_width, page_height = keyword_info['page_dimensions']
        min_x, min_y, max_x, max_y = keyword_info['keyword_box']

        if crop_type == 'top':
            if page_rotation == 90:
                page.cropbox.lower_left = (0, 0)
                page.cropbox.upper_right = (max_x, page_width)
            else:
                pydf2_min_y = page_height - max_y
                page.cropbox.lower_left = (0, pydf2_min_y)
                page.cropbox.upper_right = (page_width, page_height)
        elif crop_type == 'bottom':
            if page_rotation == 90:
                page.cropbox.lower_left = (min_x, 0)
                page.cropbox.upper_right = (page_height, page_width)
            else:
                pydf2_max_y = page_height - min_y
                page.cropbox.lower_left = (0, 0)
                page.cropbox.upper_right = (page_width, pydf2_max_y)

        return page

    def crop_page_before_keyword(self, pdf_path: str, keyword_info: Dict, output_path: str) -> Optional[str]:
        """根据关键词位置裁剪PDF页面，保留关键词及之前的内容"""
        try:
            reader = PdfReader(pdf_path)
            page = reader.pages[keyword_info['page_number'] - 1]

            page_rotation = page.rotation
            page_width, page_height = keyword_info['page_dimensions']
            min_x, min_y, max_x, max_y = keyword_info['keyword_box']

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

            writer = PdfWriter()

            for i in range(keyword_info['page_number'] - 5, keyword_info['page_number'] - 1):
                writer.add_page(reader.pages[i])

            writer.add_page(cropped_page)

            with open(output_path, "wb") as output_file:
                writer.write(output_file)

            logger.info(f"已裁剪页面并保存到: {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"裁剪页面时出错: {str(e)}")
            return None

    def crop_page_after_keyword(self, pdf_path: str, keyword_info: Dict, output_path: str, pagenum: int = 4) -> Optional[str]:
        """根据关键词位置裁剪PDF页面，保留关键词及之后的内容"""
        try:
            reader = PdfReader(pdf_path)
            page = reader.pages[keyword_info['page_number'] - 1]

            page_rotation = page.rotation
            page_width, page_height = keyword_info['page_dimensions']
            min_x, min_y, max_x, max_y = keyword_info['keyword_box']

            pydf2_max_y = page_height - min_y

            cropped_page = page
            if page_rotation == 90:
                cropped_page.cropbox.lower_left = (min_x, 0)
                cropped_page.cropbox.upper_right = (page_height, page_width)
            else:
                cropped_page.cropbox.lower_left = (0, 0)
                cropped_page.cropbox.upper_right = (page_width, pydf2_max_y)

            writer = PdfWriter()
            writer.add_page(cropped_page)

            for i in range(keyword_info['page_number'], keyword_info['page_number'] + pagenum):
                writer.add_page(reader.pages[i])

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

            page_rotation = page.get('/Rotate', 0)

            if page_rotation == 90:
                start_pydf2_min_x = start_min_x
                end_pydf2_max_x = end_max_x

                cropped_page = page
                cropped_page.cropbox.lower_left = (end_pydf2_max_x, 0)
                cropped_page.cropbox.upper_right = (start_pydf2_min_x, page_height)
            else:
                start_pydf2_max_y = page_height - start_min_y
                end_pydf2_min_y = page_height - end_max_y

                cropped_page = page
                cropped_page.cropbox.lower_left = (0, end_pydf2_min_y)
                cropped_page.cropbox.upper_right = (page_width, start_pydf2_max_y)

            writer = PdfWriter()
            writer.add_page(cropped_page)

            with open(output_path, "wb") as output_file:
                writer.write(output_file)

            logger.info(f"已裁剪页面并保存到: {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"裁剪页面时出错: {str(e)}")
            return None

    def process_pdf(self, pdf_path: str, keywords: Dict, output_path: str) -> Optional[str]:
        """通用PDF处理函数，处理交易所PDF"""
        start_info = keywords.get('start')
        end_info = keywords.get('end')
        committee_info = keywords.get('committee')
        core_tech_start_info = keywords.get('core_tech_start')
        core_tech_end_info = keywords.get('core_tech_end')
        exchange_code = self.get_exchange_code(pdf_path)

        if not start_info and not end_info:
            logger.info(f"在文件 {os.path.basename(pdf_path)} 中未找到任何关键词，保留原文件")
            shutil.copy2(pdf_path, output_path)
            return output_path

        if not start_info and end_info:
            logger.info(f"只找到结束关键词，输出包含结束关键词页的前5页内容")
            return self.crop_page_before_keyword(pdf_path, end_info, output_path)

        if start_info and not end_info:
            logger.info(f"只找到开始关键词，输出包含开始关键词页的后5页内容")
            return self.crop_page_after_keyword(pdf_path, start_info, output_path)

        if start_info and end_info:
            if start_info['page_number'] > end_info['page_number']:
                logger.info(f"开始关键词页码({start_info['page_number']})大于结束关键词页码({end_info['page_number']})，按只找到结束关键词处理")
                return self.crop_page_before_keyword(pdf_path, end_info, output_path)

            if start_info['page_number'] == end_info['page_number']:
                logger.info(f"开始关键词和结束关键词在同一页，直接裁剪该页面")
                return self.crop_same_page(pdf_path, start_info, end_info, output_path)

            try:
                reader = PdfReader(pdf_path)
                writer = PdfWriter()

                for page_num in range(start_info['page_number'] - 1, end_info['page_number']):
                    page = reader.pages[page_num]

                    if page_num == start_info['page_number'] - 1:
                        cropped_page = self.crop_page_with_rotation(page, start_info, 'bottom')
                        writer.add_page(cropped_page)
                    elif page_num == end_info['page_number'] - 1:
                        cropped_page = self.crop_page_with_rotation(page, end_info, 'top')
                        writer.add_page(cropped_page)
                    else:
                        writer.add_page(page)

                if core_tech_start_info and core_tech_end_info:
                    if core_tech_start_info['page_number'] == core_tech_end_info['page_number']:
                        page = reader.pages[core_tech_start_info['page_number'] - 1]
                        page_width, page_height = core_tech_start_info['page_dimensions']
                        start_min_x, start_min_y, start_max_x, start_max_y = core_tech_start_info['keyword_box']
                        end_min_x, end_min_y, end_max_x, end_max_y = core_tech_end_info['keyword_box']
                        page_rotation = page.get('/Rotate', 0)

                        if page_rotation == 90:
                            page.cropbox.lower_left = (end_max_x, 0)
                            page.cropbox.upper_right = (start_min_x, page_height)
                        else:
                            start_pydf2_max_y = page_height - start_min_y
                            end_pydf2_min_y = page_height - end_max_y
                            page.cropbox.lower_left = (0, end_pydf2_min_y)
                            page.cropbox.upper_right = (page_width, start_pydf2_max_y)
                        writer.add_page(page)
                    else:
                        for page_num in range(core_tech_start_info['page_number'] - 1, core_tech_end_info['page_number']):
                            page = reader.pages[page_num]

                            if page_num == core_tech_start_info['page_number'] - 1:
                                cropped_page = self.crop_page_with_rotation(page, core_tech_start_info, 'bottom')
                                writer.add_page(cropped_page)
                            elif page_num == core_tech_end_info['page_number'] - 1:
                                cropped_page = self.crop_page_with_rotation(page, core_tech_end_info, 'top')
                                writer.add_page(cropped_page)
                            else:
                                writer.add_page(page)
                else:
                    if exchange_code == 'bjs':
                        logger.info(f"未找到核心技术人员信息，跳过核心技术人员处理")

                if committee_info:
                    committee_page_num = committee_info['page_number'] - 1
                    page = reader.pages[committee_page_num]

                    cropped_page = self.crop_page_with_rotation(page, committee_info, 'bottom')
                    writer.add_page(cropped_page)

                    additional_pages = self.find_committee_additional_pages(pdf_path, committee_info['page_number'])
                    for page_num in additional_pages:
                        page = reader.pages[page_num - 1]
                        writer.add_page(page)
                else:
                    logger.info(f"未找到委员会信息，跳过委员会处理")

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

            result = self.process_pdf(pdf_path, keywords, temp_output_path)

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

        executor = ThreadPoolExecutor(max_workers=workers)

        try:
            futures = {
                executor.submit(self.process_single_pdf, pdf_file): pdf_file
                for pdf_file in pdf_files
            }

            completed = 0
            for future in as_completed(futures):
                if not self.is_processing:
                    break

                completed += 1

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
