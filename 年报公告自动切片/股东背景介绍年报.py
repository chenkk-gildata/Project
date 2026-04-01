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


def find_keywords(pdf_path):
    """使用PyMuPDF查找PDF文件中开始和结束关键词所在的页码和精确位置"""

    exchange_code = get_exchange_code(pdf_path)

    if exchange_code == "bjs":
        start_pattern = re.compile(r'^[（(]?[\d一二三四五六七八九]*.*[）)]?[、.．]?\s*持股5%以上的股东或前十名股东情况\s*$', re.IGNORECASE | re.MULTILINE)
        end_pattern = re.compile(r'是否存在实际控制人', re.IGNORECASE | re.MULTILINE)
    else:
        start_pattern = re.compile(r'^[（(]?[\d一二三四五六七八九]*.*[）)]?[、.．]?\s*(?:控股)?股东[和及]实际控制人情况(?:介绍)?\s*$', re.IGNORECASE | re.MULTILINE)
        end_pattern = re.compile(r'^[（(]?[\d一二三四五六七八九]*.*[）)]?[、.．]?\s*[公司|本行]控股股东或第一大股东及其一致行动人累计质押股份数量占', re.IGNORECASE | re.MULTILINE)
   
    def get_search_rect(page, inst, keyword_type):
        page_width = page.rect.width
        page_rotation = page.rotation

        if keyword_type in ['实际控制人情况', '股东情况', '累计质押', '存在实际']:
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

        if exchange_code == "bjs":
            start_keyword = ["股东情况"]
            end_keyword = ["存在实际"]
        else:
            start_keyword = ["实际控制人情况"]
            end_keyword = ["累计质押"]

        total_pages = len(doc)
        
        search_ranges = [
            (30, 75),
            (70, 105),
            (100, 200)
        ]

        found = False
        for start_range, end_range in search_ranges:
            actual_start = max(1, start_range)
            actual_end = min(end_range, total_pages)

            print(f"搜索股东背景介绍范围：第 {actual_start} 页到第 {actual_end} 页")

            for page_num in range(actual_start - 1, actual_end):
                page = doc.load_page(page_num)
                page_rect = page.rect

                if not end_info:
                    end_candidates = []
                    for keyword in end_keyword:
                        instances = page.search_for(keyword)
                        for inst in instances:
                            end_candidates.append((keyword, inst))

                    end_candidates.sort(key=lambda item: (item[1].y0, item[1].x0))

                    for idx, (keyword_type, inst) in enumerate(end_candidates, 1):
                        rect = get_search_rect(page, inst, keyword_type)
                        text = page.get_text("text", clip=rect)

                        save_search_debug_info(pdf_path, page_num, keyword_type, rect, text)

                        if end_pattern.search(text):
                            end_info = {
                                'page_number': page_num + 1,
                                'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                'page_dimensions': (page_rect.width, page_rect.height)
                            }
                            print(f"找到结束关键词在第 {page_num + 1} 页")
                            break
                
                if not start_info:
                    start_candidates = []
                    for keyword in start_keyword:
                        instances = page.search_for(keyword)
                        for inst in instances:
                            start_candidates.append((keyword, inst))

                    start_candidates.sort(key=lambda item: (item[1].y0, item[1].x0))

                    for idx, (keyword_type, inst) in enumerate(start_candidates, 1):
                        rect = get_search_rect(page, inst, keyword_type)
                        text = page.get_text("text", clip=rect)

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


def process_gdbjjs_only(pdf_path, gdbjjs_info, output_dir):
    """单独处理股东背景介绍切片"""
    start_info = gdbjjs_info.get('start')
    end_info = gdbjjs_info.get('end')
    
    if not start_info and not end_info:
        print(f"在文件 {os.path.basename(pdf_path)} 中未找到任何关键词，跳过处理")
        return None
    
    if not start_info and end_info:
        print(f"股东背景介绍只找到结束关键词，输出包含结束关键词页的前5页内容")
        return crop_page_before_keyword(pdf_path, end_info, output_dir)
    
    if start_info and not end_info:
        print(f"股东背景介绍只找到开始关键词，输出包含开始关键词页的后5页内容")
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


def process_pdf_with_keywords(pdf_path, output_dir):
    """根据找到的关键词情况处理PDF"""
    exchange_code = get_exchange_code(pdf_path)
    
    if exchange_code == "kcb":
        print(f"科创板股票，跳过处理: {os.path.basename(pdf_path)}")
        return None
    
    keywords = find_keywords(pdf_path)
    return process_gdbjjs_only(pdf_path, keywords, output_dir)


def process_path(path):
    """处理用户输入的路径"""
    if os.path.isfile(path):
        if path.lower().endswith('.pdf'):
            output_dir = os.path.join(os.path.dirname(path), "股东背景介绍")
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
        output_dir = os.path.join(path, "股东背景介绍")

        for pdf_file in pdf_files:
            process_pdf_with_keywords(pdf_file, output_dir)
    else:
        print("指定的路径不存在")


def main():
    """主函数"""
    print("PDF关键字页面裁剪工具（股东背景介绍）")
    print("=" * 50)

    path = input("请输入文件或目录路径: ").strip()
    process_path(path)


if __name__ == "__main__":
    main()
