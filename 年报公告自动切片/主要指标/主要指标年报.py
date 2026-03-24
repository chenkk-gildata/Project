import os
import re
import shutil
import fitz  # PyMuPDF
from PyPDF2 import PdfReader, PdfWriter

def is_bse_pdf(pdf_path):
    """根据文件名判断是否为北交所PDF公告"""
    filename = os.path.basename(pdf_path)
    # 提取文件名前6个字符
    if len(filename) >= 6:
        prefix = filename[:6]
        # 检查是否以"92%" OR "8%" OR "4%"开头
        return prefix.startswith("92") or prefix.startswith("8") or prefix.startswith("4")
    return False

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

def find_keywords(pdf_path):
    """使用PyMuPDF查找PDF文件中开始和结束关键词所在的页码和精确位置"""
    
    # 判断是否为北交所PDF
    is_bse = is_bse_pdf(pdf_path)
    
    # 定义开始和结束关键词的正则表达式
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

    def get_search_rect(inst, keyword_type):
        """根据关键词类型智能调整搜索区域"""
        if keyword_type in ['会计数据', '财务指标']:
            return fitz.Rect(0, inst.y0 - 20, inst.x1 + 400, inst.y1 + 50)
        elif keyword_type in ['具体情况', '解释性公告', '董事会报告']:
            return fitz.Rect(0, inst.y0 - 20, inst.x1 + 400, inst.y1 + 50)
        elif keyword_type in ['财务数据差异', '非经常性损益']:
            return fitz.Rect(0, inst.y0 - 20, inst.x1 + 400, inst.y1 + 50)
        else:
            # 默认搜索区域
            return fitz.Rect(inst.x0 - 100, inst.y0 - 50, inst.x1 + 100, inst.y1 + 20)

    try:
        # 使用PyMuPDF打开PDF文件
        doc = fitz.open(pdf_path)

        # 初始化结果
        start_info = None
        end_info = None
        remove_start_info = None
        remove_end_info = None

        # 优化搜索策略：采用"重点优先"原则，提高搜索效率
        total_pages = len(doc)

        # 定义搜索范围优先级
        search_ranges = [
            (4, 10),  # 重点范围：最可能出现关键词的页码
            (9, 20),  # 扩展范围：覆盖大部分情况
            (19, 30),  # 扩展范围：覆盖大部分情况
            (29, 50)   # 最多搜索50页
        ]

        found = False
        skip_directory_check = False  # 用于标记是否需要跳过目录检查
        for start_range, end_range in search_ranges:
            # 调整范围以适应PDF实际页数
            actual_start = max(1, start_range)
            actual_end = min(end_range, total_pages)

            print(f"搜索范围：第 {actual_start} 页到第 {actual_end} 页")

            # 遍历当前搜索范围内的每一页
            for page_num in range(actual_start - 1, actual_end):
                page = doc.load_page(page_num)
                page_rect = page.rect

                # 检查是否为目录页，且还需要检查目录
                if not skip_directory_check:
                    # 搜索当前页是否包含"目录"关键字
                    directory_instances = page.search_for("目录")
                    if directory_instances:
                        skip_directory_check = True  # 目录页通常只有一页，跳过之后不再检查
                        continue
                
                # 如果还没找到开始关键词，则搜索开始关键词
                if not start_info:
                    # 搜索可能的开始关键词
                    start_candidates = []
                    for keyword in ['会计数据', '财务指标']:
                        instances = page.search_for(keyword)
                        for inst in instances:
                            start_candidates.append((keyword, inst))

                    # 按照y坐标（从上到下）和x坐标（从左到右）排序
                    start_candidates.sort(key=lambda item: (item[1].y0, item[1].x0))

                    # 对每个候选进行验证
                    for keyword_type, inst in start_candidates:
                        rect = get_search_rect(inst, keyword_type)
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

                # 如果还没找到结束关键词，则搜索结束关键词
                if start_info and not end_info:
                    # 搜索可能的结束关键词
                    end_candidates = []
                    end_keywords = ["具体情况", "解释性公告", "董事会报告"] if not is_bse else ["财务指标"]
                    for keyword in end_keywords:
                        instances = page.search_for(keyword)
                        for inst in instances:
                            end_candidates.append((keyword, inst))

                    # 按照y坐标（从上到下）和x坐标（从左到右）排序
                    end_candidates.sort(key=lambda item: (item[1].y0, item[1].x0))

                    # 对每个候选进行验证
                    for keyword_type, inst in end_candidates:
                        rect = get_search_rect(inst, keyword_type)
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

                # 如果是北交所PDF，搜索移除关键词
                if is_bse:
                    # 搜索移除开始关键词
                    if not remove_start_info:
                        remove_start_candidates = []
                        instances = page.search_for("财务数据差异")
                        for inst in instances:
                            remove_start_candidates.append(("财务数据差异", inst))

                        # 按照y坐标（从上到下）和x坐标（从左到右）排序
                        remove_start_candidates.sort(key=lambda item: (item[1].y0, item[1].x0))

                        # 对每个候选进行验证
                        for keyword_type, inst in remove_start_candidates:
                            rect = get_search_rect(inst, keyword_type)
                            text = page.get_text("text", clip=rect)

                            # 测试功能：将搜索区域文本保存到临时文件，方便查看搜索内容
                            # 生产环境使用时请删除以下代码
                            save_search_debug_info(pdf_path, page_num, keyword_type, rect, text)

                            if remove_start_pattern.search(text):
                                remove_start_info = {
                                    'page_number': page_num + 1,
                                    'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                    'page_dimensions': (page_rect.width, page_rect.height)
                                }
                                print(f"找到移除开始关键词在第 {page_num + 1} 页")
                                break

                    # 搜索移除结束关键词
                    if remove_start_info and not remove_end_info:
                        remove_end_candidates = []
                        instances = page.search_for("非经常性损益")
                        for inst in instances:
                            remove_end_candidates.append(("非经常性损益", inst))

                        # 按照y坐标（从上到下）和x坐标（从左到右）排序
                        remove_end_candidates.sort(key=lambda item: (item[1].y0, item[1].x0))

                        # 对每个候选进行验证
                        for keyword_type, inst in remove_end_candidates:
                            rect = get_search_rect(inst, keyword_type)
                            text = page.get_text("text", clip=rect)

                            # 测试功能：将搜索区域文本保存到临时文件，方便查看搜索内容
                            # 生产环境使用时请删除以下代码
                            save_search_debug_info(pdf_path, page_num, keyword_type, rect, text)

                            if remove_end_pattern.search(text):
                                remove_end_info = {
                                    'page_number': page_num + 1,
                                    'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                    'page_dimensions': (page_rect.width, page_rect.height)
                                }
                                print(f"找到移除结束关键词在第 {page_num + 1} 页")
                                break

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
        
        print(f"搜索追加内容范围：第 {append_search_start} 页到第 {total_pages} 页（从后往前）")
        
        doc = fitz.open(pdf_path)
        for page_num in range(total_pages - 1, append_search_start - 1, -1):
            page = doc.load_page(page_num)
            page_rect = page.rect
            
            if not append_start_info:
                for append_end_keyword in append_end_keywords:
                    instances = page.search_for(append_end_keyword)
                    for inst in instances:
                        rect = get_search_rect(inst, append_end_keyword)
                        text = page.get_text("text", clip=rect)

                        # 测试功能：将搜索区域文本保存到临时文件，方便查看搜索内容
                        # 生产环境使用时请删除以下代码
                        save_search_debug_info(pdf_path, page_num, keyword_type, rect, text)

                        if append_end_pattern.search(text):
                            append_end_info = {
                                'page_number': page_num + 1,
                                'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                'page_dimensions': (page_rect.width, page_rect.height)
                            }
                            print(f"找到追加结束关键词在第 {page_num + 1} 页")
                            break
                    if append_end_info:
                        break
            
            if not append_start_info:
                instances = page.search_for(append_start_keyword)
                for inst in instances:
                    rect = get_search_rect(inst, append_start_keyword)
                    text = page.get_text("text", clip=rect)
                    
                    # 测试功能：将搜索区域文本保存到临时文件，方便查看搜索内容
                    # 生产环境使用时请删除以下代码
                    save_search_debug_info(pdf_path, page_num, keyword_type, rect, text)
                    
                    if append_start_pattern.search(text):
                        append_start_info = {
                            'page_number': page_num + 1,
                            'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                            'page_dimensions': (page_rect.width, page_rect.height)
                        }
                        print(f"找到追加开始关键词在第 {page_num + 1} 页")
                        break
            
            if append_start_info and append_end_info:
                break
        
        doc.close()
        
        # 返回找到的关键词信息
        if is_bse:
            return {
                'start': start_info,
                'end': end_info,
                'remove_start': remove_start_info,
                'remove_end': remove_end_info,
                'is_bse': True,
                'append_start': append_start_info,
                'append_end': append_end_info
            }
        else:
            return {
                'start': start_info,
                'end': end_info,
                'is_bse': False,
                'append_start': append_start_info,
                'append_end': append_end_info
            }
        
    except Exception as e:
        print(f"处理文件 {os.path.basename(pdf_path)} 时出错: {str(e)}")
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

def crop_page_before_keyword(pdf_path, keyword_info, output_dir):
    """根据关键词位置裁剪PDF页面，保留关键词及之前的内容"""
    try:
        # 确保输出目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 读取原始PDF
        reader = PdfReader(pdf_path)
        
        # 获取包含关键词的页面
        page = reader.pages[keyword_info['page_number'] - 1]
        
        # 获取页面尺寸和关键词位置
        page_width, page_height = keyword_info['page_dimensions']
        min_x, min_y, max_x, max_y = keyword_info['keyword_box']

        # 需要转换y坐标
        # PyPDF2的cropbox坐标系原点在左下角，所以需要转换y坐标
        # 关键词的y坐标需要从页面高度中减去
        pydf2_min_y = page_height - max_y
        
        # 创建新的PDF页面并应用裁剪
        cropped_page = page
        cropped_page.cropbox.lower_left = (0, pydf2_min_y)
        cropped_page.cropbox.upper_right = (page_width, page_height)
        
        # 创建新的PDF写入器
        writer = PdfWriter()
        
        # 添加前面五页完整的页面
        for i in range(keyword_info['page_number'] - 4, keyword_info['page_number']):
            writer.add_page(reader.pages[i])
        
        # 添加裁剪后的页面
        writer.add_page(cropped_page)
        
        # 生成输出文件名
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        output_path = os.path.join(output_dir, f"{base_name}.pdf")
        
        # 保存新PDF
        with open(output_path, "wb") as output_file:
            writer.write(output_file)
        
        print(f"已裁剪页面并保存到: {output_path}")
        return output_path
    except Exception as e:
        print(f"裁剪页面时出错: {str(e)}")
        return None

def crop_page_after_keyword(pdf_path, keyword_info, output_dir):
    """根据关键词位置裁剪PDF页面，保留关键词及之后的五页内容"""
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        reader = PdfReader(pdf_path)

        page = reader.pages[keyword_info['page_number'] - 1]

        page_width, page_height = keyword_info['page_dimensions']
        min_x, min_y, max_x, max_y = keyword_info['keyword_box']

        pydf2_max_y = page_height - min_y

        cropped_page = page
        cropped_page.cropbox.lower_left = (0, 0)
        cropped_page.cropbox.upper_right = (page_width, pydf2_max_y)

        writer = PdfWriter()

        writer.add_page(cropped_page)
        total_pages = len(reader.pages)
        end_page = min(keyword_info['page_number'] + 4, total_pages)
        for i in range(keyword_info['page_number'], end_page):
            writer.add_page(reader.pages[i])

        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        output_path = os.path.join(output_dir, f"{base_name}.pdf")

        with open(output_path, "wb") as output_file:
            writer.write(output_file)

        print(f"已裁剪页面并保存到: {output_path}")
        return output_path
    except Exception as e:
        print(f"裁剪页面时出错: {str(e)}")
        return None

def extract_append_pages(pdf_path, append_start_info, append_end_info):
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
        print(f"追加内容：只找到结束关键词，提取第 {start_page} 页到第 {end_page} 页")
    
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
        print(f"追加内容：只找到开始关键词，提取第 {start_page} 页到第 {end_page} 页")
    
    elif append_start_info and append_end_info:
        start_page = append_start_info['page_number']
        end_page = append_end_info['page_number']
        
        if start_page > end_page:
            target_page = max(1, end_page - 1)
            append_pages.append(reader.pages[target_page - 1])
            print(f"追加内容：开始页>结束页，提取第 {target_page} 页")
        
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
            print(f"追加内容：开始和结束在同一页（第 {start_page} 页）")
        
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
            print(f"追加内容：正常情况，提取第 {start_page} 页到第 {end_page} 页")
    
    return append_pages

def append_content_to_pdf(existing_pdf_path, source_pdf_path, append_start_info, append_end_info, output_dir):
    """
    将追加内容合并到已存在的PDF中
    
    Args:
        existing_pdf_path: 已存在的PDF路径
        source_pdf_path: 源PDF路径
        append_start_info: 开始关键词信息
        append_end_info: 结束关键词信息
        output_dir: 输出目录
    
    Returns:
        str: 输出文件路径
    """
    try:
        append_pages = extract_append_pages(source_pdf_path, append_start_info, append_end_info)
        if not append_pages:
            print("没有找到追加内容")
            return existing_pdf_path
        
        existing_reader = PdfReader(existing_pdf_path)
        writer = PdfWriter()
        
        for page in existing_reader.pages:
            writer.add_page(page)
        
        for page in append_pages:
            writer.add_page(page)
        
        base_name = os.path.splitext(os.path.basename(existing_pdf_path))[0]
        output_path = os.path.join(output_dir, f"{base_name}.pdf")
        
        with open(output_path, "wb") as output_file:
            writer.write(output_file)
        
        print(f"已追加内容并保存到: {output_path}")
        return output_path
    except Exception as e:
        print(f"追加内容到PDF时出错: {str(e)}")
        return existing_pdf_path

def save_append_content_pdf(pdf_path, append_start_info, append_end_info, output_dir):
    """
    将净资产收益率/每股收益模块单独保存为PDF
    
    Args:
        pdf_path: 源PDF路径
        append_start_info: 开始关键词信息
        append_end_info: 结束关键词信息
        output_dir: 主输出目录
    
    Returns:
        str: 输出文件路径，如果无内容则返回None
    """
    try:
        append_pages = extract_append_pages(pdf_path, append_start_info, append_end_info)
        if not append_pages:
            print("没有找到净资产收益率/每股收益内容")
            return None
        
        mgsy_dir = os.path.join(output_dir, "净资产收益率和每股收益")
        if not os.path.exists(mgsy_dir):
            os.makedirs(mgsy_dir)
        
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        output_path = os.path.join(mgsy_dir, f"{base_name}_mgsy.pdf")
        
        writer = PdfWriter()
        for page in append_pages:
            writer.add_page(page)
        
        with open(output_path, "wb") as output_file:
            writer.write(output_file)
        
        print(f"净资产收益率/每股收益内容已保存到: {output_path}")
        return output_path
    except Exception as e:
        print(f"保存净资产收益率/每股收益内容时出错: {str(e)}")
        return None

def process_pdf(pdf_path, keywords, output_dir):
    """通用PDF处理函数，处理北交所和沪深交易所PDF"""
    start_info = keywords['start']
    end_info = keywords['end']
    remove_start_info = keywords.get('remove_start')
    remove_end_info = keywords.get('remove_end')
    is_bse = keywords.get('is_bse', False)
    append_start_info = keywords.get('append_start')
    append_end_info = keywords.get('append_end')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    output_path = os.path.join(output_dir, f"{base_name}.pdf")
    
    if not start_info and not end_info:
        print(f"在文件 {os.path.basename(pdf_path)} 中未找到任何关键词，原样输出")
        shutil.copy2(pdf_path, output_path)
        print(f"已复制原文件到: {output_path}")
        return output_path
    
    if not start_info and end_info:
        print(f"特殊处理：只找到结束关键词，按原逻辑处理")
        output_path = crop_page_before_keyword(pdf_path, end_info, output_dir)
        if output_path and (append_start_info or append_end_info):
            save_append_content_pdf(pdf_path, append_start_info, append_end_info, output_dir)
        return output_path
    
    if start_info and not end_info:
        print(f"特殊处理：只找到开始关键词，输出包含开始关键词页的后五页内容")
        output_path = crop_page_after_keyword(pdf_path, start_info, output_dir)
        if output_path and (append_start_info or append_end_info):
            save_append_content_pdf(pdf_path, append_start_info, append_end_info, output_dir)
        return output_path
    
    if start_info and end_info:
        if start_info['page_number'] > end_info['page_number']:
            print(f"特殊处理：开始关键词页码({start_info['page_number']})大于结束关键词页码({end_info['page_number']})，按只找到结束关键词处理")
            output_path = crop_page_before_keyword(pdf_path, end_info, output_dir)
            if output_path and (append_start_info or append_end_info):
                save_append_content_pdf(pdf_path, append_start_info, append_end_info, output_dir)
            return output_path
        
        reader = PdfReader(pdf_path)
        writer = PdfWriter()
        
        if start_info['page_number'] == end_info['page_number']:
            print(f"特殊处理：开始关键词和结束关键词在同一页，直接输出该页面")
            writer.add_page(reader.pages[start_info['page_number'] - 1])

            with open(output_path, "wb") as output_file:
                writer.write(output_file)

            print(f"已处理并保存到: {output_path}")
            
            if append_start_info or append_end_info:
                save_append_content_pdf(pdf_path, append_start_info, append_end_info, output_dir)
            
            return output_path
        
        try:
            if is_bse and remove_start_info and remove_end_info:
                for page_num in range(start_info['page_number'] - 1, end_info['page_number']):
                    page = reader.pages[page_num]

                    if page_num == start_info['page_number'] - 1 and page_num != remove_start_info['page_number'] - 1:
                        page_width, page_height = start_info['page_dimensions']
                        min_x, min_y, max_x, max_y = start_info['keyword_box']

                        pydf2_max_y = page_height - min_y

                        cropped_start_page = page
                        cropped_start_page.cropbox.lower_left = (0, 0)
                        cropped_start_page.cropbox.upper_right = (page_width, pydf2_max_y)

                        writer.add_page(cropped_start_page)
                        continue

                    if (remove_start_info and remove_end_info and
                            remove_start_info['page_number'] <= page_num + 1 <= remove_end_info['page_number']):
                        
                        if page_num + 1 == remove_start_info['page_number']:
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
            else:
                start_page = reader.pages[start_info['page_number'] - 1]
                page_width, page_height = start_info['page_dimensions']
                min_x, min_y, max_x, max_y = start_info['keyword_box']

                pydf2_max_y = page_height - min_y

                cropped_start_page = start_page
                cropped_start_page.cropbox.lower_left = (0, 0)
                cropped_start_page.cropbox.upper_right = (page_width, pydf2_max_y)

                writer.add_page(cropped_start_page)

                for i in range(start_info['page_number'], end_info['page_number'] - 1):
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
            
            print(f"已处理并保存到: {output_path}")
            
            if append_start_info or append_end_info:
                save_append_content_pdf(pdf_path, append_start_info, append_end_info, output_dir)
            
            return output_path
        except Exception as e:
            print(f"处理文件时出错: {str(e)}")
            return None
    return None

def process_pdf_with_keywords(pdf_path, output_dir):
    """根据找到的关键词情况处理PDF"""
    # 查找关键词
    keywords = find_keywords(pdf_path)
    
    # 直接使用通用处理函数
    return process_pdf(pdf_path, keywords, output_dir)

def process_path(path):
    """处理用户输入的路径"""
    if os.path.isfile(path):
        # 处理单个文件
        if path.lower().endswith('.pdf'):
            output_dir = os.path.join(os.path.dirname(path), "主要指标")
            process_pdf_with_keywords(path, output_dir)
        else:
            print("指定的文件不是PDF文件")
    elif os.path.isdir(path):
        # 处理目录中的所有PDF文件
        pdf_files = []
        for file in os.listdir(path):
            file_path = os.path.join(path, file)
            if os.path.isfile(file_path) and file.lower().endswith('.pdf'):
                pdf_files.append(file_path)
        
        if not pdf_files:
            print("指定目录中未找到PDF文件")
            return
        
        print(f"找到 {len(pdf_files)} 个PDF文件")
        output_dir = os.path.join(path, "主要指标")
        
        for pdf_file in pdf_files:
            process_pdf_with_keywords(pdf_file, output_dir)
    else:
        print("指定的路径不存在")

def main():
    """主函数"""
    print("PDF关键字页面裁剪工具")
    print("=" * 50)
    
    # 获取用户输入的路径
    path = input("请输入文件或目录路径: ").strip()
    process_path(path)

if __name__ == "__main__":
    main()