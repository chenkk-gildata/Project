import os
import re
import fitz
from PyPDF2 import PdfReader, PdfWriter


def get_exchange_code(pdf_path):
    """根据文件名判断交易所类型"""
    filename = os.path.basename(pdf_path)
    if len(filename) >= 6:
        prefix = filename[:6]
        if prefix.startswith("00") or prefix.startswith("30") or prefix.startswith("20"):
            return "szs"
        elif prefix.startswith("60") or prefix.startswith("90"):
            return "shs"
        elif prefix.startswith("68"):
            return "kcb"
        elif prefix.startswith("92") or prefix.startswith("8") or prefix.startswith("4"):
            return "bjs"
    return None

_search_debug_counter = {}

def save_search_debug_info(pdf_path, page_num, keyword_type, rect, text):
    """封装测试代码，生产环境隐藏：保存搜索区域文本到临时文件，方便查看搜索内容"""
    debug_dir = os.path.join(os.path.dirname(pdf_path), "search_debug")
    if not os.path.exists(debug_dir):
        os.makedirs(debug_dir)

    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    key = f"{base_name}_page{page_num + 1}_{keyword_type}"
    if key not in _search_debug_counter:
        _search_debug_counter[key] = 0
    _search_debug_counter[key] += 1
    seq = _search_debug_counter[key]
    
    debug_file = os.path.join(debug_dir, f"{base_name}_page{page_num + 1}_{keyword_type}_{seq}.txt")

    with open(debug_file, "w", encoding="utf-8") as f:
        f.write(f"页面: {page_num + 1}\n")
        f.write(f"关键词类型: {keyword_type}\n")
        f.write(f"搜索区域: {rect}\n")
        f.write("=" * 50 + "\n")
        f.write(text)
        f.write("\n" + "=" * 50)


def find_lxr_keywords(pdf_path):
    """查找联系人关键词"""
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
    
    try:
        doc = fitz.open(pdf_path)
        
        start_info = None
        end_info = None
        
        total_pages = len(doc)
        search_ranges = [(3, 20)]
        
        start_keyword = ["联系人"]
        end_keyword = ["基本情况", "备置地点"]
        
        found = False
        skip_directory_check = False
        for start_range, end_range in search_ranges:
            actual_start = max(1, start_range)
            actual_end = min(end_range, total_pages)
            
            print(f"搜索联系人范围：第 {actual_start} 页到第 {actual_end} 页")
            
            for page_num in range(actual_start - 1, actual_end):
                page = doc.load_page(page_num)
                page_rect = page.rect
                
                if not skip_directory_check:
                    directory_instances = page.search_for("目录")
                    if directory_instances:
                        skip_directory_check = True
                        continue
                
                if not end_info:
                    start_candidates = []
                    for keyword in start_keyword:
                        instances = page.search_for(keyword)
                        for inst in instances:
                            start_candidates.append((keyword, inst))
                    
                    start_candidates.sort(key=lambda item: (item[1].y0, item[1].x0))
                    
                    for idx, (keyword_type, inst) in enumerate(start_candidates, 1):
                        rect = get_search_rect(page, inst, keyword_type)
                        text = page.get_text("text", clip=rect)
                        
                        # 测试功能：将搜索区域文本保存到临时文件，方便查看搜索内容
                        # 生产环境使用时请删除以下代码
                        save_search_debug_info(pdf_path, page_num, keyword_type, rect, text)

                        if start_pattern.search(text):
                            start_info = {
                                'page_number': page_num + 1,
                                'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                'page_dimensions': (page_rect.width, page_rect.height)
                            }
                            print(f"找到联系人开始关键词在第 {page_num + 1} 页")
                            break
                
                if start_info and not end_info:
                    end_candidates = []
                    for keyword in end_keyword:
                        instances = page.search_for(keyword)
                        for inst in instances:
                            end_candidates.append((keyword, inst))
                    
                    end_candidates.sort(key=lambda item: (item[1].y0, item[1].x0))
                    
                    for idx, (keyword_type, inst) in enumerate(end_candidates, 1):
                        rect = get_search_rect(page, inst, keyword_type)
                        text = page.get_text("text", clip=rect)
                        
                        # 测试功能：将搜索区域文本保存到临时文件，方便查看搜索内容
                        # 生产环境使用时请删除以下代码
                        save_search_debug_info(pdf_path, page_num, keyword_type, rect, text)

                        if end_pattern.search(text):
                            end_info = {
                                'page_number': page_num + 1,
                                'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                'page_dimensions': (page_rect.width, page_rect.height)
                            }
                            print(f"找到联系人结束关键词在第 {page_num + 1} 页")
                            break
                
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
    
    except Exception as e:
        print(f"处理文件 {os.path.basename(pdf_path)} 时出错: {str(e)}")
        return {
            'start': None,
            'end': None
        }


def find_keywords(pdf_path):
    """使用PyMuPDF查找PDF文件中开始和结束关键词所在的页码和精确位置"""

    exchange_code = get_exchange_code(pdf_path)

    start_pattern = re.compile(r'^[（(]?[\d一二三四五六七八九]*.*[）)]?[、.．]?\s*(?:本行|公司|现任)?董事(?:、监事)?[及和、]高级管理人员(?:的|基本)?情况', re.IGNORECASE | re.MULTILINE)

    if exchange_code == "bjs":
        end_pattern = re.compile(r'董事(?:、监事)?[和、]高级管理人员.*决策程序.*报酬.*支付情况', re.IGNORECASE | re.MULTILINE)
    else:
        end_pattern = re.compile(r'在(?:本公司)?(?:股东|其他)(?:及关联)?单位.*任职的?情况', re.IGNORECASE | re.MULTILINE)

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

    try:
        doc = fitz.open(pdf_path)

        start_info = None
        end_info = None

        start_keyword = ["高级管理人员"]
        if exchange_code == "bjs":
            end_keyword = ["决策程序", "报酬确定", "支付情况"]
        else:
            end_keyword = ["股东单位", "任职情况"]

        total_pages = len(doc)

        search_ranges = [
            (15, 50),
            (45, 70),
            (65, 130)
        ]

        found = False
        for start_range, end_range in search_ranges:
            actual_start = max(1, start_range)
            actual_end = min(end_range, total_pages)

            print(f"搜索领导人介绍范围：第 {actual_start} 页到第 {actual_end} 页")

            for page_num in range(actual_start - 1, actual_end):
                page = doc.load_page(page_num)
                page_rect = page.rect

                if not end_info and start_info:
                    end_candidates = []
                    for keyword in end_keyword:
                        instances = page.search_for(keyword)
                        for inst in instances:
                            end_candidates.append((keyword, inst))

                    end_candidates.sort(key=lambda item: (item[1].y0, item[1].x0))

                    for idx, (keyword_type, inst) in enumerate(end_candidates, 1):
                        rect = get_search_rect(page, inst, keyword_type)
                        text = page.get_text("text", clip=rect)

                        # 测试功能：将搜索区域文本保存到临时文件，方便查看搜索内容
                        # 生产环境使用时请删除以下代码
                        save_search_debug_info(pdf_path, page_num, keyword_type, rect, text)

                        if end_pattern.search(text):
                            end_info = {
                                'page_number': page_num + 1,
                                'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                'page_dimensions': (page_rect.width, page_rect.height)
                            }
                            print(f"找到结束关键词在第 {page_num + 1} 页")
                            break
                
                if not end_info:
                    start_candidates = []
                    for keyword in start_keyword:
                        instances = page.search_for(keyword)
                        for inst in instances:
                            start_candidates.append((keyword, inst))

                    start_candidates.sort(key=lambda item: (item[1].y0, item[1].x0))

                    for idx, (keyword_type, inst) in enumerate(start_candidates, 1):
                        rect = get_search_rect(page, inst, keyword_type)
                        text = page.get_text("text", clip=rect)

                        # 测试功能：将搜索区域文本保存到临时文件，方便查看搜索内容
                        # 生产环境使用时请删除以下代码
                        save_search_debug_info(pdf_path, page_num, keyword_type, rect, text)
                        
                        if start_pattern.search(text):
                            start_info = {
                                'page_number': page_num + 1,
                                'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                'page_dimensions': (page_rect.width, page_rect.height)
                            }
                            print(f"找到开始关键词在第 {page_num + 1} 页")
                            break
                                            
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

    except Exception as e:
        print(f"处理文件 {os.path.basename(pdf_path)} 时出错: {str(e)}")
        return {
            'start': None,
            'end': None
        }


def find_all_keywords(pdf_path):
    """查找所有关键词（联系人+领导人介绍）"""
    exchange_code = get_exchange_code(pdf_path)
    
    if exchange_code == "bjs":
        return {
            'lxr': {'start': None, 'end': None},
            'ldrjs': find_keywords(pdf_path)
        }
    else:
        return {
            'lxr': find_lxr_keywords(pdf_path),
            'ldrjs': find_keywords(pdf_path)
        }


def crop_page_before_keyword(pdf_path, keyword_info, output_dir):
    """根据关键词位置裁剪PDF页面，保留关键词及之前的内容"""
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

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

        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        output_path = os.path.join(output_dir, f"{base_name}.pdf")

        with open(output_path, "wb") as output_file:
            writer.write(output_file)

        print(f"已特殊处理并保存到: {output_path}")
        return output_path
    except Exception as e:
        print(f"裁剪页面时出错: {str(e)}")
        return None


def crop_page_after_keyword(pdf_path, keyword_info, output_dir):
    """根据关键词位置裁剪PDF页面，保留关键词及之后的5页内容"""
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

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
        for i in range(keyword_info['page_number'], keyword_info['page_number'] + 4):
            writer.add_page(reader.pages[i])

        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        output_path = os.path.join(output_dir, f"{base_name}.pdf")

        with open(output_path, "wb") as output_file:
            writer.write(output_file)

        print(f"已特殊处理并保存到: {output_path}")
        return output_path
    except Exception as e:
        print(f"裁剪页面时出错: {str(e)}")
        return None


def crop_same_page(pdf_path, start_info, end_info, output_dir):
    """在同一页裁剪,保留从开始关键词到结束关键词之间的内容"""
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        reader = PdfReader(pdf_path)
        page_num = start_info['page_number'] - 1
        page = reader.pages[page_num]
        
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
        
        writer = PdfWriter()
        writer.add_page(cropped_page)
        
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        output_path = os.path.join(output_dir, f"{base_name}.pdf")
        
        with open(output_path, "wb") as output_file:
            writer.write(output_file)
        
        print(f"已处理并保存到: {output_path}")
        return output_path
    except Exception as e:
        print(f"裁剪同一页时出错: {str(e)}")
        return None


def process_standard_pdf(pdf_path, start_info, end_info, output_dir):
    """标准PDF处理:从开始关键词裁剪到结束关键词"""
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        reader = PdfReader(pdf_path)
        writer = PdfWriter()
        
        start_page = reader.pages[start_info['page_number'] - 1]
        start_page_rotation = start_page.rotation
        page_width, page_height = start_info['page_dimensions']
        min_x, min_y, max_x, max_y = start_info['keyword_box']
        
        pydf2_max_y = page_height - min_y
        
        cropped_start_page = start_page
        if start_page_rotation == 90:
            cropped_start_page.cropbox.lower_left = (min_x, 0)
            cropped_start_page.cropbox.upper_right = (page_height, page_width)
        else:
            cropped_start_page.cropbox.lower_left = (0, 0)
            cropped_start_page.cropbox.upper_right = (page_width, pydf2_max_y)
        writer.add_page(cropped_start_page)
        
        for i in range(start_info['page_number'], end_info['page_number'] - 1):
            writer.add_page(reader.pages[i])
        
        end_page = reader.pages[end_info['page_number'] - 1]
        end_page_rotation = end_page.rotation
        page_width, page_height = end_info['page_dimensions']
        min_x, min_y, max_x, max_y = end_info['keyword_box']
        
        if end_page_rotation == 90:
            pydf2_min_y = page_height - max_x
        else:
            pydf2_min_y = page_height - max_y
        
        cropped_end_page = end_page
        if end_page_rotation == 90:
            cropped_end_page.cropbox.lower_left = (0, 0)
            cropped_end_page.cropbox.upper_right = (max_x, page_width)
        else:
            cropped_end_page.cropbox.lower_left = (0, pydf2_min_y)
            cropped_end_page.cropbox.upper_right = (page_width, page_height)
        writer.add_page(cropped_end_page)
        
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        output_path = os.path.join(output_dir, f"{base_name}.pdf")
        
        with open(output_path, "wb") as output_file:
            writer.write(output_file)
        
        print(f"已处理并保存到: {output_path}")
        return output_path
    except Exception as e:
        print(f"处理文件时出错: {str(e)}")
        return None


def crop_lxr_start_only(pdf_path, start_info, output_dir):
    """联系人只找到开始关键词：当前页裁剪 + 下一页完整"""
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
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
        output_path = os.path.join(output_dir, f"{base_name}.pdf")
        
        with open(output_path, "wb") as output_file:
            writer.write(output_file)
        
        print(f"联系人只找到开始关键词，已处理并保存到: {output_path}")
        return output_path
    except Exception as e:
        print(f"裁剪联系人开始页时出错: {str(e)}")
        return None


def crop_lxr_end_only(pdf_path, end_info, output_dir):
    """联系人只找到结束关键词：上一页完整 + 当前页裁剪"""
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
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
        output_path = os.path.join(output_dir, f"{base_name}.pdf")
        
        with open(output_path, "wb") as output_file:
            writer.write(output_file)
        
        print(f"联系人只找到结束关键词，已处理并保存到: {output_path}")
        return output_path
    except Exception as e:
        print(f"裁剪联系人结束页时出错: {str(e)}")
        return None


def crop_lxr_two_pages(pdf_path, start_info, end_info, output_dir):
    """联系人两页裁剪：开始页裁剪 + 结束页裁剪"""
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
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
        output_path = os.path.join(output_dir, f"{base_name}.pdf")
        
        with open(output_path, "wb") as output_file:
            writer.write(output_file)
        
        print(f"联系人两页裁剪，已处理并保存到: {output_path}")
        return output_path
    except Exception as e:
        print(f"裁剪联系人两页时出错: {str(e)}")
        return None


def process_lxr_only(pdf_path, lxr_info, output_dir):
    """单独处理联系人切片"""
    start_info = lxr_info.get('start')
    end_info = lxr_info.get('end')
    
    if not start_info and not end_info:
        return None
    
    if not start_info and end_info:
        print(f"联系人只找到结束关键词")
        return crop_lxr_end_only(pdf_path, end_info, output_dir)
    
    if start_info and not end_info:
        print(f"联系人只找到开始关键词")
        return crop_lxr_start_only(pdf_path, start_info, output_dir)
    
    if start_info and end_info:
        if start_info['page_number'] > end_info['page_number']:
            return crop_lxr_end_only(pdf_path, end_info, output_dir)
        
        if start_info['page_number'] == end_info['page_number']:
            print(f"联系人同一页")
            return crop_same_page(pdf_path, start_info, end_info, output_dir)
        
        if start_info['page_number'] + 1 == end_info['page_number']:
            print(f"联系人两页")
            return crop_lxr_two_pages(pdf_path, start_info, end_info, output_dir)
        
        print(f"联系人超过两页，按只找到结束处理")
        return crop_lxr_end_only(pdf_path, end_info, output_dir)
    
    return None


def process_ldrjs_only(pdf_path, ldrjs_info, output_dir):
    """单独处理领导人介绍切片"""
    start_info = ldrjs_info.get('start')
    end_info = ldrjs_info.get('end')
    
    if not start_info and not end_info:
        print(f"在文件 {os.path.basename(pdf_path)} 中未找到任何关键词，跳过处理")
        return None
    
    if not start_info and end_info:
        print(f"领导人介绍只找到结束关键词，输出包含结束关键词页的前5页内容")
        return crop_page_before_keyword(pdf_path, end_info, output_dir)
    
    if start_info and not end_info:
        print(f"领导人介绍只找到开始关键词，输出包含开始关键词页的后5页内容")
        return crop_page_after_keyword(pdf_path, start_info, output_dir)
    
    if start_info and end_info:
        if start_info['page_number'] > end_info['page_number']:
            print(f"开始关键词页码({start_info['page_number']})大于结束关键词页码({end_info['page_number']})，按只找到结束关键词处理")
            return crop_page_before_keyword(pdf_path, end_info, output_dir)
        
        if start_info['page_number'] == end_info['page_number']:
            print(f"开始关键词页码({start_info['page_number']})等于结束关键词页码({end_info['page_number']})，按只找到开始关键词处理")
            return crop_page_after_keyword(pdf_path, start_info, output_dir)
        
        return process_standard_pdf(pdf_path, start_info, end_info, output_dir)
    
    return None


def add_lxr_slice_to_writer(reader, writer, lxr_info):
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
            add_lxr_slice_to_writer(reader, writer, {'start': None, 'end': end_info})
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
        
        add_lxr_slice_to_writer(reader, writer, {'start': None, 'end': end_info})
        return


def add_ldrjs_slice_to_writer(reader, writer, ldrjs_info):
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
            add_ldrjs_slice_to_writer(reader, writer, {'start': None, 'end': end_info})
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


def merge_slices(pdf_path, lxr_info, ldrjs_info, output_dir):
    """合并联系人和领导人介绍切片"""
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        reader = PdfReader(pdf_path)
        writer = PdfWriter()
        
        add_lxr_slice_to_writer(reader, writer, lxr_info)
        
        add_ldrjs_slice_to_writer(reader, writer, ldrjs_info)
        
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        output_path = os.path.join(output_dir, f"{base_name}.pdf")
        
        with open(output_path, "wb") as output_file:
            writer.write(output_file)
        
        print(f"已合并处理并保存到: {output_path}")
        return output_path
    except Exception as e:
        print(f"合并切片时出错: {str(e)}")
        return None


def process_combined(pdf_path, lxr_info, ldrjs_info, output_dir):
    """合并处理联系人和领导人介绍"""
    lxr_start = lxr_info.get('start')
    ldrjs_start = ldrjs_info.get('start')
    
    if not lxr_start and not ldrjs_start:
        print(f"联系人和领导人介绍都未找到关键词")
        return None
    
    if lxr_start and not ldrjs_start:
        print(f"只找到联系人关键词")
        return process_lxr_only(pdf_path, lxr_info, output_dir)
    
    if not lxr_start and ldrjs_start:
        print(f"只找到领导人介绍关键词")
        return process_ldrjs_only(pdf_path, ldrjs_info, output_dir)
    
    print(f"找到联系人和领导人介绍关键词，合并输出")
    return merge_slices(pdf_path, lxr_info, ldrjs_info, output_dir)


def process_pdf(pdf_path, keywords, output_dir):
    """处理PDF（支持合并输出）"""
    if 'lxr' in keywords:
        lxr_info = keywords['lxr']
        ldrjs_info = keywords['ldrjs']
    else:
        lxr_info = {'start': None, 'end': None}
        ldrjs_info = keywords
    
    exchange_code = get_exchange_code(pdf_path)
    
    if exchange_code == "bjs":
        return process_ldrjs_only(pdf_path, ldrjs_info, output_dir)
    
    return process_combined(pdf_path, lxr_info, ldrjs_info, output_dir)


def process_pdf_with_keywords(pdf_path, output_dir):
    """根据找到的关键词情况处理PDF"""
    keywords = find_all_keywords(pdf_path)
    
    return process_pdf(pdf_path, keywords, output_dir)


def process_path(path):
    """处理用户输入的路径"""
    if os.path.isfile(path):
        if path.lower().endswith('.pdf'):
            output_dir = os.path.join(os.path.dirname(path), "领导人介绍")
            process_pdf_with_keywords(path, output_dir)
        else:
            print("指定的文件不是PDF文件")
    elif os.path.isdir(path):
        pdf_files = []
        for file in os.listdir(path):
            file_path = os.path.join(path, file)
            if os.path.isfile(file_path) and file.lower().endswith('.pdf'):
                pdf_files.append(file_path)

        if not pdf_files:
            print("指定目录中未找到PDF文件")
            return

        print(f"找到 {len(pdf_files)} 个PDF文件")
        output_dir = os.path.join(path, "领导人介绍")

        for pdf_file in pdf_files:
            process_pdf_with_keywords(pdf_file, output_dir)
    else:
        print("指定的路径不存在")


def main():
    """主函数"""
    print("PDF关键字页面裁剪工具（含联系人模块）")
    print("=" * 50)

    path = input("请输入文件或目录路径: ").strip()
    process_path(path)


if __name__ == "__main__":
    main()
