import os
import re
import fitz
from PyPDF2 import PdfReader, PdfWriter


def get_exchange_code(pdf_path):
    """根据文件名判断交易所类型"""
    filename = os.path.basename(pdf_path)
    if len(filename) >= 6:
        prefix = filename[:6]
        if prefix.startswith("00") or prefix.startswith("30"):
            return "szs"
        elif prefix.startswith("6") or prefix.startswith("90"):
            return "shs"
        elif prefix.startswith("92") or prefix.startswith("8") or prefix.startswith("4"):
            return "bjs"
    return None


def save_search_debug_info(pdf_path, page_num, keyword_type, rect, text):
    """封装测试代码，生产环境隐藏：保存搜索区域文本到临时文件，方便查看搜索内容"""
    debug_dir = os.path.join(os.path.dirname(pdf_path), "search_debug")
    if not os.path.exists(debug_dir):
        os.makedirs(debug_dir)

    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    debug_file = os.path.join(debug_dir, f"{base_name}_page{page_num + 1}_{keyword_type}.txt")

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
    
    start_pattern = re.compile(
        r'^[（(]?[\d一二三四五六七八九]?[）)]?[、.．]?\s*(?:本行|公司)?董事(?:[和及、]监事)?[和及、]高级管理人员(?:的|基本)?情况\s*$',
        re.IGNORECASE | re.MULTILINE
    )
    
    if exchange_code == "bjs":
        end_pattern = re.compile(r'[（(]?[二三四五六123456][）)]?[、.．]?\s*股权激励情况\s*$', re.IGNORECASE | re.MULTILINE)
        remove_start_pattern = re.compile(r'[（(]?[一二三四五12345][）)]?[、.．]?\s*变动情况\s*$', re.IGNORECASE | re.MULTILINE)
        remove_end_pattern = re.compile(r'董事(?:[和及、]监事)?[和及、]高级管理人员.*决策程序.*确定依据以及实际支付情况', re.IGNORECASE | re.MULTILINE)
        end_keyword = ["股权激励情况"]
        remove_start_keyword = ["变动情况"]
        remove_end_keyword = ["实际支付"]
    elif exchange_code == "shs":
        end_pattern = re.compile(
            r'^[（(]?[\d一二三四五六七八九]?[）)]?[、.．]?\s*(公司)?董事(?:、监事)?[和及、]高级管理人员(?:和核心技术人员)?变动情况\s*$',
            re.IGNORECASE | re.MULTILINE
        )
        remove_start_pattern = re.compile(r'主要工作经历', re.IGNORECASE | re.MULTILINE)
        remove_end_pattern = re.compile(r'董事(?:、监事)?[和及、]高级管理人员(?:和核心技术人员)?(?:报酬|薪酬)情况\s*$', re.IGNORECASE | re.MULTILINE)
        end_keyword = ["变动情况"]
        remove_start_keyword = ["主要工作经历"]
        remove_end_keyword = ["报酬情况", "薪酬情况"]
    else:
        end_pattern = re.compile(
            r'^[（(]?[\d二三四五六七八九][）)]?[、.．]?\s*(报告期内)?.*董事.*(履行职责的|履职|有关)情况\s*$',
            re.IGNORECASE | re.MULTILINE
        )
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
            
            save_search_debug_info(pdf_path, page.number, keyword, rect, text)
            
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

        print(f"搜索范围：第 {actual_start} 页到第 {actual_end} 页")

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
                    print(f"找到开始关键词在第 {page_num + 1} 页")

            if not end_info and start_info:
                result = collect_and_sort_instances(page, end_keyword, end_pattern, get_search_rect)
                if result:
                    inst = result['inst']
                    end_info = {
                        'page_number': page_num + 1,
                        'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                        'page_dimensions': (page_rect.width, page_rect.height)
                    }
                    print(f"找到结束关键词在第 {page_num + 1} 页")

            if start_info and not remove_start_info:
                result = collect_and_sort_instances(page, remove_start_keyword, remove_start_pattern, get_search_rect)
                if result:
                    inst = result['inst']
                    remove_start_info = {
                        'page_number': page_num + 1,
                        'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                        'page_dimensions': (page_rect.width, page_rect.height)
                    }
                    print(f"找到移除开始关键词在第 {page_num + 1} 页")

            if remove_start_info and not remove_end_info:
                result = collect_and_sort_instances(page, remove_end_keyword, remove_end_pattern, get_search_rect)
                if result:
                    inst = result['inst']
                    remove_end_info = {
                        'page_number': page_num + 1,
                        'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                        'page_dimensions': (page_rect.width, page_rect.height)
                    }
                    print(f"找到移除结束关键词在第 {page_num + 1} 页")
            
            if start_info and end_info and remove_start_info and remove_end_info:
                found = True
                break
        
        if found:
            break
    
    doc.close()
    
    return {
        'start': start_info,
        'end': end_info,
        'remove_start': remove_start_info,
        'remove_end': remove_end_info
    }


def crop_page_before_keyword(pdf_path, keyword_info, output_dir):
    """根据关键词位置裁剪PDF页面，保留关键词及之前的内容"""
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        reader = PdfReader(pdf_path)
        page = reader.pages[keyword_info['page_number'] - 1]
        page_width, page_height = keyword_info['page_dimensions']
        min_x, min_y, max_x, max_y = keyword_info['keyword_box']

        pydf2_min_y = page_height - max_y

        cropped_page = page
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


def crop_page_after_keyword(pdf_path, keyword_info, output_dir, pagenum=4):
    """根据关键词位置裁剪PDF页面，保留关键词及之后的内容"""
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
        
        for i in range(keyword_info['page_number'], keyword_info['page_number'] + pagenum):
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
    """在同一页裁剪，保留从开始关键词到结束关键词之间的内容"""
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        reader = PdfReader(pdf_path)
        page_num = start_info['page_number'] - 1
        page = reader.pages[page_num]

        page_width, page_height = start_info['page_dimensions']
        start_min_x, start_min_y, start_max_x, start_max_y = start_info['keyword_box']
        end_min_x, end_min_y, end_max_x, end_max_y = end_info['keyword_box']

        start_pydf2_max_y = page_height - start_min_y
        end_pydf2_min_y = page_height - end_max_y

        cropped_page = page
        cropped_page.cropbox.lower_left = (0, end_pydf2_min_y)
        cropped_page.cropbox.upper_right = (page_width, start_pydf2_max_y)

        writer = PdfWriter()
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


def process_pdf(pdf_path, keywords, output_dir):
    """通用PDF处理函数，处理交易所PDF"""
    start_info = keywords.get('start')
    end_info = keywords.get('end')
    remove_start_info = keywords.get('remove_start')
    remove_end_info = keywords.get('remove_end')
    
    if not start_info and not end_info:
        print(f"在文件 {os.path.basename(pdf_path)} 中未找到任何关键词，跳过处理")
        return None
    
    if not start_info and end_info:
        print(f"特殊处理：只找到结束关键词，输出包含结束关键词页的前5页内容")
        return crop_page_before_keyword(pdf_path, end_info, output_dir)
    
    if start_info and not end_info:
        print(f"特殊处理：只找到开始关键词，输出包含开始关键词页的后5页内容")
        return crop_page_after_keyword(pdf_path, start_info, output_dir)
    
    if start_info and end_info:
        if start_info['page_number'] > end_info['page_number']:
            print(f"特殊处理：开始关键词页码({start_info['page_number']})大于结束关键词页码({end_info['page_number']})，按只找到结束关键词处理")
            return crop_page_before_keyword(pdf_path, end_info, output_dir)
        
        if start_info['page_number'] == end_info['page_number']:
            return crop_same_page(pdf_path, start_info, end_info, output_dir)
        
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
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"{base_name}.pdf")
            
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
    keywords = find_keywords(pdf_path)
    return process_pdf(pdf_path, keywords, output_dir)


def process_path(path):
    """处理用户输入的路径"""
    if os.path.isfile(path):
        if path.lower().endswith('.pdf'):
            output_dir = os.path.join(os.path.dirname(path), "领导人持股")
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
        output_dir = os.path.join(path, "领导人持股")

        for pdf_file in pdf_files:
            process_pdf_with_keywords(pdf_file, output_dir)
    else:
        print("指定的路径不存在")


def main():
    """主函数"""
    print("PDF关键字页面裁剪工具")
    print("=" * 50)

    path = input("请输入文件或目录路径: ").strip()
    process_path(path)


if __name__ == "__main__":
    main()
