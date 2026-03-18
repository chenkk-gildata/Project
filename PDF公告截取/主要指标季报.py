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

def find_keywords(pdf_path):
    """使用PyMuPDF查找PDF文件中开始和结束关键词所在的页码和精确位置"""
    # 判断是否为北交所PDF
    is_bse = is_bse_pdf(pdf_path)
    
    # 定义开始和结束关键词的正则表达式
    if is_bse:
        # 北交所关键词
        start_pattern = re.compile(r'主要财务数据|主要会计数据[及和]财务指标', re.IGNORECASE | re.MULTILINE)
        end_pattern = re.compile(r'前十名股东情况', re.IGNORECASE | re.MULTILINE)
        remove_start_pattern = re.compile(r'财务数据重大变动原因', re.IGNORECASE | re.MULTILINE)
        remove_end_pattern = re.compile(r'非经常性损益项目和金额', re.IGNORECASE | re.MULTILINE)
    else:
        # 沪深关键词
        start_pattern = re.compile(r'[（(]?[一1][）)]?[、.．]?\s*主要财务数据|主要会计数据[及和]财务指标', re.IGNORECASE | re.MULTILINE)
        end_pattern = re.compile(r'[（(]?[三3][）)]?[、.．]?\s*主要会计数据.*变动.*原因', re.IGNORECASE | re.MULTILINE)
        remove_start_pattern = None
        remove_end_pattern = None
    
    def get_search_rect(inst, keyword_type):
        """根据关键词类型智能调整搜索区域"""
        if keyword_type in ['三', '发生变动的情况', '前十名股东情况']:
            # "三"或"前十名股东情况"通常在前面，搜索区域向右下方扩展
            return fitz.Rect(inst.x0, inst.y0 - 20, inst.x1 + 400, inst.y1 + 50)
        elif keyword_type in ['一', '主要财务数据', '主要会计数据']:
            # "主要财务数据"通常在前面，搜索区域向右下方扩展
            return fitz.Rect(inst.x0, inst.y0 - 20, inst.x1 + 400, inst.y1 + 50)
        elif keyword_type in ['财务数据重大变动原因', '非经常性损益']:
            # 北交所移除关键词，搜索区域向右下方扩展
            return fitz.Rect(inst.x0, inst.y0 - 20, inst.x1 + 400, inst.y1 + 50)
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
        
        # 遍历每一页
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            page_rect = page.rect
            
            # 如果还没找到开始关键词，则搜索开始关键词
            if not start_info:
                # 搜索可能的开始关键词
                start_candidates = []
                for keyword in ["一", "主要财务数据", "主要会计数据"]:
                    instances = page.search_for(keyword)
                    for inst in instances:
                        start_candidates.append((keyword, inst))
                
                # 按照y坐标（从上到下）和x坐标（从左到右）排序
                start_candidates.sort(key=lambda item: (item[1].y0, item[1].x0))
                
                # 对每个候选进行验证
                for keyword_type, inst in start_candidates:
                    rect = get_search_rect(inst, keyword_type)
                    text = page.get_text("text", clip=rect)
                    if start_pattern.search(text):
                        start_info = {
                            'page_number': page_num + 1,
                            'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                            'page_dimensions': (page_rect.width, page_rect.height)
                        }
                        print(f"找到开始关键词在第 {page_num + 1} 页")
                        break
            
            # 如果还没找到结束关键词，则搜索结束关键词
            if not end_info:
                # 搜索可能的结束关键词
                end_candidates = []
                end_keywords = ["三", "发生变动的情况"] if not is_bse else ["前十名股东情况"]
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
                    instances = page.search_for("财务数据重大变动原因")
                    for inst in instances:
                        remove_start_candidates.append(("财务数据重大变动原因", inst))
                    
                    # 按照y坐标（从上到下）和x坐标（从左到右）排序
                    remove_start_candidates.sort(key=lambda item: (item[1].y0, item[1].x0))
                    
                    # 对每个候选进行验证
                    for keyword_type, inst in remove_start_candidates:
                        rect = get_search_rect(inst, keyword_type)
                        text = page.get_text("text", clip=rect)
                        if remove_start_pattern.search(text):
                            remove_start_info = {
                                'page_number': page_num + 1,
                                'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                'page_dimensions': (page_rect.width, page_rect.height)
                            }
                            print(f"找到移除开始关键词在第 {page_num + 1} 页")
                            break
                
                # 搜索移除结束关键词
                if not remove_end_info:
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
                        if remove_end_pattern.search(text):
                            remove_end_info = {
                                'page_number': page_num + 1,
                                'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                'page_dimensions': (page_rect.width, page_rect.height)
                            }
                            print(f"找到移除结束关键词在第 {page_num + 1} 页")
                            break
            
            # 如果所有需要的关键词都找到了，提前结束搜索
            if is_bse:
                if start_info and end_info and remove_start_info and remove_end_info:
                    break
            else:
                if start_info and end_info:
                    break
        
        # 关闭文档
        doc.close()
        
        # 返回找到的关键词信息
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

def crop_page_at_keyword(pdf_path, keyword_info, output_dir):
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
        
        # 添加前面所有完整的页面
        for i in range(keyword_info['page_number'] - 1):
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
        start_pydf2_min_y = page_height - start_max_y
        end_pydf2_min_y = page_height - end_max_y
        
        # 创建新的PDF页面并应用裁剪
        cropped_page = page
        cropped_page.cropbox.lower_left = (0, end_pydf2_min_y)
        cropped_page.cropbox.upper_right = (page_width, page_height)
        
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

def process_bse_pdf(pdf_path, keywords, output_dir):
    """处理北交所PDF，包括中间内容移除逻辑"""
    start_info = keywords['start']
    end_info = keywords['end']
    remove_start_info = keywords['remove_start']
    remove_end_info = keywords['remove_end']
    
    # 确保输出目录存在
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 生成输出文件名
    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    output_path = os.path.join(output_dir, f"{base_name}.pdf")
    
    # 情况1：同时没有找到开始关键词和结束关键词，原样输出原文件
    if not start_info and not end_info:
        print(f"在文件 {os.path.basename(pdf_path)} 中未找到任何关键词，原样输出")
        # 直接复制原文件
        shutil.copy2(pdf_path, output_path)
        print(f"已复制原文件到: {output_path}")
        return output_path
    
    # 情况2：只找到了结束关键词，按crop_page_at_keyword()的逻辑处理
    if not start_info and end_info:
        print(f"只找到结束关键词，按原逻辑处理")
        return crop_page_at_keyword(pdf_path, end_info, output_dir)
    
    # 情况3：只找到了开始关键词，默认输出包含开始关键词页的后两页内容
    if start_info and not end_info:
        print(f"只找到开始关键词，输出包含开始关键词页的后两页内容")
        try:
            # 读取原始PDF
            reader = PdfReader(pdf_path)
            writer = PdfWriter()
            
            # 只添加开始关键词页及后两页（如果存在）
            total_pages = len(reader.pages)
            end_page = min(start_info['page_number'] + 5, total_pages)  # 开始页+6页，但不超过总页数
            
            for i in range(start_info['page_number'] - 1, end_page):
                writer.add_page(reader.pages[i])
            
            # 保存新PDF
            with open(output_path, "wb") as output_file:
                writer.write(output_file)
            
            print(f"已处理并保存到: {output_path}")
            return output_path
        except Exception as e:
            print(f"处理文件时出错: {str(e)}")
            return None
    
    # 情况4：找到了开始关键词和结束关键词
    if start_info and end_info:
        # 情况4.1：开始关键词页码大于结束关键词页码，按只找到结束关键词处理
        if start_info['page_number'] > end_info['page_number']:
            print(f"开始关键词页码({start_info['page_number']})大于结束关键词页码({end_info['page_number']})，按只找到结束关键词处理")
            return crop_page_at_keyword(pdf_path, end_info, output_dir)
        
        # 情况4.2：开始关键词和结束关键词在同一页，直接裁剪该页面
        if start_info['page_number'] == end_info['page_number']:
            print(f"开始关键词和结束关键词在同一页，直接裁剪该页面")
            return crop_same_page(pdf_path, start_info, end_info, output_dir)
        
        # 情况4.3：正常情况，需要处理中间内容移除
        try:
            # 读取原始PDF
            reader = PdfReader(pdf_path)
            writer = PdfWriter()
            
            # 添加从开始关键词页到结束关键词页的所有页面，但需要处理移除区域
            for page_num in range(start_info['page_number'] - 1, end_info['page_number']):
                page = reader.pages[page_num]
                
                # 如果是移除区域内的页面，需要进行特殊处理
                if (remove_start_info and remove_end_info and 
                    page_num + 1 >= remove_start_info['page_number'] and 
                    page_num + 1 <= remove_end_info['page_number']):
                    
                    # 如果是移除开始页，只保留移除开始关键词之前的内容
                    if page_num + 1 == remove_start_info['page_number']:
                        page_width, page_height = remove_start_info['page_dimensions']
                        min_x, min_y, max_x, max_y = remove_start_info['keyword_box']
                        
                        # 转换y坐标
                        pydf2_min_y = page_height - max_y
                        
                        # 创建裁剪后的页面
                        cropped_page = page
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
    
    # 根据交易所类型选择不同的处理逻辑
    if keywords.get('is_bse', False):
        return process_bse_pdf(pdf_path, keywords, output_dir)
    else:
        return process_shsz_pdf(pdf_path, keywords, output_dir)

def process_shsz_pdf(pdf_path, keywords, output_dir):
    """处理沪深PDF，使用原有逻辑"""
    start_info = keywords['start']
    end_info = keywords['end']
    
    # 确保输出目录存在
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 生成输出文件名
    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    output_path = os.path.join(output_dir, f"{base_name}.pdf")
    
    # 情况1：同时没有找到开始关键词和结束关键词，原样输出原文件
    if not start_info and not end_info:
        print(f"在文件 {os.path.basename(pdf_path)} 中未找到任何关键词，原样输出")
        # 直接复制原文件
        shutil.copy2(pdf_path, output_path)
        print(f"已复制原文件到: {output_path}")
        return output_path
    
    # 情况2：只找到了结束关键词，按crop_page_at_keyword()的逻辑处理
    if not start_info and end_info:
        print(f"只找到结束关键词，按原逻辑处理")
        return crop_page_at_keyword(pdf_path, end_info, output_dir)
    
    # 情况3：只找到了开始关键词，默认输出包含开始关键词页的后两页内容
    if start_info and not end_info:
        print(f"只找到开始关键词，输出包含开始关键词页的后两页内容")
        try:
            # 读取原始PDF
            reader = PdfReader(pdf_path)
            writer = PdfWriter()
            
            # 只添加开始关键词页及后两页（如果存在）
            total_pages = len(reader.pages)
            end_page = min(start_info['page_number'] + 2, total_pages)  # 开始页+2页，但不超过总页数
            
            for i in range(start_info['page_number'] - 1, end_page):
                writer.add_page(reader.pages[i])
            
            # 保存新PDF
            with open(output_path, "wb") as output_file:
                writer.write(output_file)
            
            print(f"已处理并保存到: {output_path}")
            return output_path
        except Exception as e:
            print(f"处理文件时出错: {str(e)}")
            return None
    
    # 情况4：找到了开始关键词和结束关键词
    if start_info and end_info:
        # 情况4.1：开始关键词页码大于结束关键词页码，按只找到结束关键词处理
        if start_info['page_number'] > end_info['page_number']:
            print(f"开始关键词页码({start_info['page_number']})大于结束关键词页码({end_info['page_number']})，按只找到结束关键词处理")
            return crop_page_at_keyword(pdf_path, end_info, output_dir)
        
        # 情况4.2：开始关键词和结束关键词在同一页，直接裁剪该页面
        if start_info['page_number'] == end_info['page_number']:
            print(f"开始关键词和结束关键词在同一页，直接裁剪该页面")
            return crop_same_page(pdf_path, start_info, end_info, output_dir)
        
        # 情况4.3：正常情况，开始关键词页至结束关键词前一页的内容，加上结束关键词页的裁剪
        try:
            # 读取原始PDF
            reader = PdfReader(pdf_path)
            writer = PdfWriter()
            
            # 只添加从开始关键词页到结束关键词前一页的所有完整页面
            for i in range(start_info['page_number'] - 1, end_info['page_number'] - 1):
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