import os
import re
import fitz  # PyMuPDF
from PyPDF2 import PdfReader, PdfWriter

def get_exchange_code(pdf_path):
    """根据文件名判断交易所类型"""
    filename = os.path.basename(pdf_path)
    # 提取文件名前6个字符
    if len(filename) >= 6:
        prefix = filename[:6]
        # 根据不同前缀返回对应的交易所代码
        if prefix.startswith("00") or prefix.startswith("30") or prefix.startswith("20"):
            return "szs"
        elif prefix.startswith("60") or prefix.startswith("90"):
            return "shs"
        elif prefix.startswith("68"):
            return "kcb"
        elif prefix.startswith("92") or prefix.startswith("8") or prefix.startswith("4"):
            return "bjs"
    return None

def save_search_debug_info(pdf_path, page_num, keyword_type, rect, text):
    """封装测试代码，生产环境隐藏：保存搜索区域文本到临时文件，方便查看搜索内容"""
    debug_dir = os.path.join(os.path.dirname(pdf_path), "search_debug")
    if not os.path.exists(debug_dir):
        os.makedirs(debug_dir)

    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    debug_file = os.path.join(debug_dir, f"{base_name}_page{page_num + 1}_start_{keyword_type}.txt")

    with open(debug_file, "w", encoding="utf-8") as f:
        f.write(f"页面: {page_num + 1}\n")
        f.write(f"关键词类型: {keyword_type}\n")
        f.write(f"搜索区域: {rect}\n")
        f.write("=" * 50 + "\n")
        f.write(text)
        f.write("\n" + "=" * 50)

def find_keywords(pdf_path):
    """使用PyMuPDF查找PDF文件中开始和结束关键词所在的页码和精确位置"""
    
    # 判断是否为科创板PDF
    exchange_code = get_exchange_code(pdf_path)
    
    # 定义开始和结束关键词的正则表达式
    if exchange_code == "szs":
        # 深交所关键词
        start_pattern = re.compile(r'公司研发人员情况|^[\d][、.．]?\s*研发投入\s*$', re.IGNORECASE | re.MULTILINE)
        end_pattern = re.compile(r'研发人员构成发生重大变化的原因及影响|[\d四五六][）)]?[、.．]?\s*现金流', re.IGNORECASE | re.MULTILINE)
    elif exchange_code == "shs":
        # 上交所关键词
        start_pattern = re.compile(r'[\d三四五][）)]?[、.．]?\s*研发投入', re.IGNORECASE | re.MULTILINE)
        end_pattern = re.compile(r'[\d四五六][）)]?[、.．]?\s*现金流', re.IGNORECASE | re.MULTILINE)
    elif exchange_code == "bjs":
        # 北交所关键词
        start_pattern = re.compile(r'[（(]?[一二三四九12349][）)]?[、.．]?\s*研发支出情况', re.IGNORECASE | re.MULTILINE)
        end_pattern = re.compile(r'[\d一二三四五][）)]?[、.．]?\s*专利情况', re.IGNORECASE | re.MULTILINE)
    else:
        # 科创板关键词
        start_pattern = re.compile(r'\d?[）)]?[、.．]?\s*研发投入情况表', re.IGNORECASE | re.MULTILINE)
        end_pattern = re.compile(r'\d[、.．]?\s*其他说明', re.IGNORECASE | re.MULTILINE)
        remove_start_pattern = re.compile(r'研发.*总额较上年发生重大变化的原因', re.IGNORECASE | re.MULTILINE)
        remove_end_pattern = re.compile(r'\d[、.．]?\s*研发人员情况', re.IGNORECASE | re.MULTILINE)

    def get_search_rect(inst, keyword_type):
        """根据关键词类型智能调整搜索区域"""
        if keyword_type in ['研发投入', '研发人员', '研发情况', '现金流', '专利情况', '其他说明', '发生重大变化的原因', '研发人员情况']:
            return fitz.Rect(0, inst.y0 - 20, inst.x1 + 400, inst.y1 + 50)
        else:
            # 默认搜索区域
            return fitz.Rect(0, inst.y0 - 50, inst.x1 + 100, inst.y1 + 20)

    try:
        # 使用PyMuPDF打开PDF文件
        doc = fitz.open(pdf_path)

        # 初始化结果
        start_info = None
        end_info = None
        remove_start_info = None
        remove_end_info = None

        # 定义不同交易所的搜索关键词
        if exchange_code == "szs":
            start_keyword = ["研发投入", "研发人员"]
            end_keyword = ["发生重大变化的原因", "现金流"]
        elif exchange_code == "shs":
            start_keyword = ["研发投入"]
            end_keyword = ["现金流"]
        elif exchange_code == "bjs":
            start_keyword = ["研发情况"]
            end_keyword = ["专利情况"]
        else:
            start_keyword = ["研发投入"]
            end_keyword = ["其他说明"]
            remove_start_keyword = ["发生重大变化的原因"]
            remove_end_keyword = ["研发人员情况"]


        # 优化搜索策略：采用"重点优先"原则，提高搜索效率
        total_pages = len(doc)

        # 定义搜索范围优先级
        search_ranges = [
            (10, 40),  # 重点范围：最可能出现关键词的页码
            (35, 100)   # 最多搜索100页
        ]

        found = False
        for start_range, end_range in search_ranges:
            # 调整范围以适应PDF实际页数
            actual_start = max(1, start_range)
            actual_end = min(end_range, total_pages)

            print(f"搜索范围：第 {actual_start} 页到第 {actual_end} 页")

            # 遍历当前搜索范围内的每一页
            for page_num in range(actual_start - 1, actual_end):
                page = doc.load_page(page_num)
                page_rect = page.rect
                
                # 如果还没找到开始关键词，则搜索开始关键词
                if not start_info:
                    # 搜索可能的开始关键词
                    start_candidates = []
                    for keyword in start_keyword:
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
                    for keyword in end_keyword:
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

                # 如果是科创板PDF，搜索移除关键词
                if exchange_code == 'kcb':
                    # 搜索移除开始关键词
                    if not remove_start_info:
                        remove_start_candidates = []
                        for keyword in remove_start_keyword:
                            instances = page.search_for(keyword)
                            for inst in instances:
                                remove_start_candidates.append((keyword, inst))

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
                        for keyword in remove_end_keyword:
                            instances = page.search_for(keyword)
                            for inst in instances:
                                remove_end_candidates.append((keyword, inst))

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

                # 如果所有需要的关键词都找到了，提前结束搜索
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
        
        # 关闭文档
        doc.close()
        
        # 返回找到的关键词信息
        if exchange_code == 'kcb':
            return {
                'start': start_info,
                'end': end_info,
                'remove_start': remove_start_info,
                'remove_end': remove_end_info,
                'is_kcb': True
            }
        else:
            return {
                'start': start_info,
                'end': end_info,
                'is_kcb': False
            }
        
    except Exception as e:
        print(f"处理文件 {os.path.basename(pdf_path)} 时出错: {str(e)}")
        if exchange_code == 'kcb':
            return {
                'start': None,
                'end': None,
                'remove_start': None,
                'remove_end': None,
                'is_kcb': True
            }
        else:
            return {
                'start': None,
                'end': None,
                'is_kcb': False
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
        
        # 添加前面两页完整的页面
        for i in range(keyword_info['page_number'] - 1, keyword_info['page_number']):
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
    """根据关键词位置裁剪PDF页面，保留关键词及之后的两页内容"""
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

        # fitz的坐标系原点在左上角，而PyPDF2的cropbox坐标系原点在左下角，所以需要转换y坐标
        # 关键词的y坐标需要从页面高度中减去
        pydf2_max_y = page_height - min_y

        # 创建新的PDF页面并应用裁剪
        cropped_page = page
        cropped_page.cropbox.lower_left = (0, 0)
        cropped_page.cropbox.upper_right = (page_width, pydf2_max_y)

        # 创建新的PDF写入器
        writer = PdfWriter()

        # 添加裁剪后的页面
        writer.add_page(cropped_page)
        # 添加结束关键词页之后一页的页面（完整页面）
        writer.add_page(reader.pages[keyword_info['page_number']])

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

def crop_same_page(pdf_path, start_info, end_info, output_dir):
    """在同一页裁剪，保留从开始关键词到结束关键词之间的内容"""
    try:
        # 确保输出目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # 读取原始PDF
        reader = PdfReader(pdf_path)

        # 获取包含关键词的页面
        page_num = start_info['page_number'] - 1
        page = reader.pages[page_num]

        # 获取页面尺寸和关键词位置
        page_width, page_height = start_info['page_dimensions']
        start_min_x, start_min_y, start_max_x, start_max_y = start_info['keyword_box']
        end_min_x, end_min_y, end_max_x, end_max_y = end_info['keyword_box']

        # 转换y坐标
        start_pydf2_max_y = page_height - start_min_y
        end_pydf2_min_y = page_height - end_max_y

        # 创建新的PDF页面并应用裁剪
        cropped_page = page
        cropped_page.cropbox.lower_left = (0, end_pydf2_min_y)
        cropped_page.cropbox.upper_right = (page_width, start_pydf2_max_y)

        # 创建新的PDF写入器
        writer = PdfWriter()

        # 只添加裁剪后的页面
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

def process_pdf(pdf_path, keywords, output_dir):
    """通用PDF处理函数，处理交易所PDF"""
    start_info = keywords['start']
    end_info = keywords['end']
    # 科创板PDF才有移除信息，其他PDF没有
    remove_start_info = keywords.get('remove_start')
    remove_end_info = keywords.get('remove_end')
    is_kcb = keywords.get('is_kcb', False)

    # 确保输出目录存在
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 生成输出文件名
    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    output_path = os.path.join(output_dir, f"{base_name}.pdf")
    
    # 情况1：同时没有找到开始关键词和结束关键词，跳过处理
    if not start_info and not end_info:
        print(f"在文件 {os.path.basename(pdf_path)} 中未找到任何关键词，跳过处理")
        return None
    
    # 情况2：只找到了结束关键词，按crop_page_before_keyword()的逻辑处理
    if not start_info and end_info:
        print(f"特殊处理：只找到结束关键词，按原逻辑处理")
        return crop_page_before_keyword(pdf_path, end_info, output_dir)
    
    # 情况3：只找到了开始关键词，默认输出包含开始关键词页的后一页内容
    if start_info and not end_info:
        print(f"特殊处理：只找到开始关键词，输出包含开始关键词页的后两页内容")
        return crop_page_after_keyword(pdf_path, start_info, output_dir)
    
    # 情况4：找到了开始关键词和结束关键词
    if start_info and end_info:
        # 情况4.1：开始关键词页码大于结束关键词页码，按只找到结束关键词处理
        if start_info['page_number'] > end_info['page_number']:
            print(f"特殊处理：开始关键词页码({start_info['page_number']})大于结束关键词页码({end_info['page_number']})，按只找到结束关键词处理")
            return crop_page_before_keyword(pdf_path, end_info, output_dir)
        
        # 情况4.2：开始关键词和结束关键词在同一页，裁剪输出该页面
        if start_info['page_number'] == end_info['page_number']:
            return crop_same_page(pdf_path, start_info, end_info, output_dir)
                
        # 只有在这些情况下才需要创建PdfReader和PdfWriter对象
        # 读取原始PDF
        reader = PdfReader(pdf_path)
        writer = PdfWriter()
        
        # 情况4.3：正常情况
        try:
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
            
            # 保存新PDF
            with open(output_path, "wb") as output_file:
                writer.write(output_file)
            
            print(f"已处理并保存到: {output_path}")
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
            output_dir = os.path.join(os.path.dirname(path), "研发投入")
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
        output_dir = os.path.join(path, "研发投入")
        
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