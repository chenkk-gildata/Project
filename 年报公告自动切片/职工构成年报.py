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

def save_search_debug_info(pdf_path, page_num, keyword_type, rect, text):
    """封装测试代码：保存搜索区域文本到临时文件，方便查看搜索内容"""
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
    # 判断是否为北交所PDF
    is_bse = is_bse_pdf(pdf_path)

    # 定义开始和结束关键词的正则表达式
    if is_bse:
        # 北交所关键词
        start_pattern = re.compile(r'在职[职|员]工.*基本情况', re.IGNORECASE | re.MULTILINE)
        end_pattern = re.compile(r'员工薪酬政策.*培训计划.*离退休职工人数', re.IGNORECASE | re.MULTILINE)
    else:
        # 沪深关键词
        start_pattern = re.compile(r'(?<!高级管理人员[和及])(?:公司|本集团(的)?)?员工情况\s*$|员工及其薪金\s*$', re.IGNORECASE | re.MULTILINE)
        end_pattern = re.compile(r'[（(]?[一二三四九12349][）)]?[、.．]?\s*(?:员工)?薪酬政策', re.IGNORECASE | re.MULTILINE)

    def get_search_rect(inst, keyword_type):
        """根据关键词类型智能调整搜索区域"""
        if keyword_type in ['在职职工', '基本情况', '员工情况']:
            return fitz.Rect(0, inst.y0 - 50, inst.x1 + 400, inst.y1 + 50)
        elif keyword_type in ['薪酬政策']:
            return fitz.Rect(0, inst.y0 - 50, inst.x1 + 400, inst.y1 + 50)
        else:
            # 默认搜索区域
            return fitz.Rect(inst.x0 - 100, inst.y0 - 50, inst.x1 + 100, inst.y1 + 20)

    try:
        # 使用PyMuPDF打开PDF文件
        doc = fitz.open(pdf_path)

        # 初始化结果
        start_info = None
        end_info = None

        # 优化搜索策略：采用"重点优先"原则，提高搜索效率
        total_pages = len(doc)
        
        # 定义搜索范围优先级
        search_ranges = [
            (30, 60),    # 重点范围：最可能出现关键词的页码
            (60, 90),    # 扩展范围：覆盖大部分情况
            (20, 30),    # 扩展范围：覆盖大部分情况
            (1, 20),
            (90, total_pages)
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
                if not end_info:
                    # 搜索可能的开始关键词
                    start_candidates = []
                    start_keyword = ["员工情况"] if not is_bse else ['在职职工', '基本情况']
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
                        # 测试功能结束
                        
                        if start_pattern.search(text):
                            start_info = {
                                'page_number': page_num + 1,
                                'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                'page_dimensions': (page_rect.width, page_rect.height)
                            }
                            print(f"找到开始关键词在第 {page_num + 1} 页")
                            break

                # 如果找到开始关键词并且还没找到结束关键词，则搜索结束关键词
                if start_info and not end_info:
                    # 搜索可能的结束关键词
                    end_candidates = []
                    for keyword in ['薪酬政策']:
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
                        # 测试功能结束
                        
                        if end_pattern.search(text):
                            end_info = {
                                'page_number': page_num + 1,
                                'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                                'page_dimensions': (page_rect.width, page_rect.height)
                            }
                            print(f"找到结束关键词在第 {page_num + 1} 页")
                            break

                # 如果所有需要的关键词都找到了，提前结束搜索
                if start_info and end_info:
                    found = True
                    break
            
            if found:
                break

        # 关闭文档
        doc.close()

        # 返回找到的关键词信息
        return {
            'start': start_info,
            'end': end_info,
            'is_bse': is_bse
        }

    except Exception as e:
        print(f"处理文件 {os.path.basename(pdf_path)} 时出错: {str(e)}")
        return {
            'start': None,
            'end': None,
            'is_bse': is_bse
        }

def crop_page_before_keyword(pdf_path, keyword_info, output_dir):
    """根据关键词位置裁剪PDF页面，保留关键词及之前的一页内容"""
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
        pydf2_min_y = page_height - max_y

        # 创建新的PDF页面并应用裁剪
        cropped_page = page
        cropped_page.cropbox.lower_left = (0, pydf2_min_y)
        cropped_page.cropbox.upper_right = (page_width, page_height)

        # 创建新的PDF写入器
        writer = PdfWriter()

        # 添加结束关键词页之前一页的页面（完整页面）
        writer.add_page(reader.pages[keyword_info['page_number']-1])
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
    """根据关键词位置裁剪PDF页面，保留关键词及之后的一页内容"""
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


def process_generic_pdf(pdf_path, keywords, output_dir):
    """通用PDF处理函数，处理北交所和沪深交易所PDF"""
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
        print(f"在文件 {os.path.basename(pdf_path)} 中未找到任何关键词，跳过处理")
        return None

    # 情况2：只找到了结束关键词，输出结束关键词页的前一页内容，并包括裁剪后的结束关键词页
    if not start_info and end_info:
        print(f"特殊处理：只找到结束关键词，输出结束关键词页的前一页内容")
        return crop_page_before_keyword(pdf_path, end_info, output_dir)

    # 情况3：只找到了开始关键词，默认输出包含开始关键词页的后一页内容
    if start_info and not end_info:
        print(f"特殊处理：只找到开始关键词，输出包含开始关键词页的后一页内容")
        return crop_page_after_keyword(pdf_path, start_info, output_dir)

    # 情况4：找到了开始关键词和结束关键词
    if start_info and end_info:
        # 情况4.1：开始关键词页码大于结束关键词页码，按只找到结束关键词处理
        if start_info['page_number'] > end_info['page_number']:
            print(
                f"特殊处理：开始关键词页码({start_info['page_number']})大于结束关键词页码({end_info['page_number']})，按只找到结束关键词处理")
            return crop_page_before_keyword(pdf_path, end_info, output_dir)

        # 情况4.2：开始关键词和结束关键词在同一页，直接裁剪该页面
        if start_info['page_number'] == end_info['page_number']:
            return crop_same_page(pdf_path, start_info, end_info, output_dir)

        # 情况4.3：正常情况，开始关键词页的裁剪至结束关键词前一页的内容，加上结束关键词页的裁剪
        try:
            # 读取原始PDF
            reader = PdfReader(pdf_path)
            writer = PdfWriter()

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

            # 开始关键词页和结束关键词页相差大于1页
            if start_info['page_number'] < end_info['page_number'] - 1:
                # 添加从开始关键词页后一页到结束关键词前一页的所有完整页面
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
    
    # 使用通用处理函数处理
    return process_generic_pdf(pdf_path, keywords, output_dir)


def process_path(path):
    """处理用户输入的路径"""
    if os.path.isfile(path):
        # 处理单个文件
        if path.lower().endswith('.pdf'):
            output_dir = os.path.join(os.path.dirname(path), "职工构成")
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
        output_dir = os.path.join(path, "职工构成")

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