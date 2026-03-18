"""
PDF处理工具函数
"""
import os
import re
import fitz  # PyMuPDF
from PyPDF2 import PdfReader, PdfWriter


def get_exchange_code(pdf_path: str) -> str:
    """
    根据文件名判断交易所类型
    
    Returns:
        str: 'szs'(深交所), 'shs'(上交所), 'kcb'(科创板), 'bjs'(北交所), None(未知)
    """
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


def is_bse_pdf(pdf_path: str) -> bool:
    """判断是否为北交所PDF"""
    return get_exchange_code(pdf_path) == "bjs"


def crop_page_before_keyword(pdf_path: str, keyword_info: dict, output_dir: str,
                             pre_pages: int = 5) -> str:
    """
    根据关键词位置裁剪PDF页面,保留关键词及之前的内容

    Args:
        pdf_path: 输入PDF路径
        keyword_info: 关键词信息字典
        output_dir: 输出目录
        pre_pages: 关键词前保留的完整页数

    Returns:
        str: 输出文件路径
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    reader = PdfReader(pdf_path)
    page = reader.pages[keyword_info['page_number'] - 1]

    page_width, page_height = keyword_info['page_dimensions']
    min_x, min_y, max_x, max_y = keyword_info['keyword_box']

    # 获取页面旋转角度
    page_rotation = page.rotation

    # 根据旋转角度处理坐标转换
    if page_rotation == 90:
        pydf2_min_y = page_height - max_x
    else:
        pydf2_min_y = page_height - max_y

    # 创建裁剪后的页面
    cropped_page = page
    if page_rotation == 90:
        cropped_page.cropbox.lower_left = (0, 0)
        cropped_page.cropbox.upper_right = (max_x, page_width)
    else:
        cropped_page.cropbox.lower_left = (0, pydf2_min_y)
        cropped_page.cropbox.upper_right = (page_width, page_height)

    writer = PdfWriter()

    # 添加前面完整页面
    start_page = max(0, keyword_info['page_number'] - pre_pages - 1)
    for i in range(start_page, keyword_info['page_number'] - 1):
        writer.add_page(reader.pages[i])

    writer.add_page(cropped_page)

    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    output_path = os.path.join(output_dir, f"{base_name}.pdf")

    with open(output_path, "wb") as output_file:
        writer.write(output_file)

    return output_path


def crop_page_after_keyword(pdf_path: str, keyword_info: dict, output_dir: str,
                            post_pages: int = 5) -> str:
    """
    根据关键词位置裁剪PDF页面,保留关键词及之后的内容

    Args:
        pdf_path: 输入PDF路径
        keyword_info: 关键词信息字典
        output_dir: 输出目录
        post_pages: 关键词后保留的完整页数

    Returns:
        str: 输出文件路径
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    reader = PdfReader(pdf_path)
    page = reader.pages[keyword_info['page_number'] - 1]

    page_width, page_height = keyword_info['page_dimensions']
    min_x, min_y, max_x, max_y = keyword_info['keyword_box']

    # 获取页面旋转角度
    page_rotation = page.rotation

    # 转换y坐标
    pydf2_max_y = page_height - min_y

    # 创建裁剪后的页面
    cropped_page = page
    if page_rotation == 90:
        cropped_page.cropbox.lower_left = (min_x, 0)
        cropped_page.cropbox.upper_right = (page_height, page_width)
    else:
        cropped_page.cropbox.lower_left = (0, 0)
        cropped_page.cropbox.upper_right = (page_width, pydf2_max_y)

    writer = PdfWriter()
    writer.add_page(cropped_page)

    # 添加后面完整页面
    total_pages = len(reader.pages)
    end_page = min(keyword_info['page_number'] + post_pages, total_pages)
    for i in range(keyword_info['page_number'], end_page):
        writer.add_page(reader.pages[i])

    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    output_path = os.path.join(output_dir, f"{base_name}.pdf")

    with open(output_path, "wb") as output_file:
        writer.write(output_file)

    return output_path


def crop_same_page(pdf_path: str, start_info: dict, end_info: dict, output_dir: str) -> str:
    """
    在同一页裁剪,保留从开始关键词到结束关键词之间的内容

    Returns:
        str: 输出文件路径
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    reader = PdfReader(pdf_path)
    page_num = start_info['page_number'] - 1
    page = reader.pages[page_num]

    page_width, page_height = start_info['page_dimensions']
    start_min_x, start_min_y, start_max_x, start_max_y = start_info['keyword_box']
    end_min_x, end_min_y, end_max_x, end_max_y = end_info['keyword_box']

    # 获取页面旋转角度
    page_rotation = page.rotation

    # 转换y坐标
    start_pydf2_max_y = page_height - start_min_y
    end_pydf2_min_y = page_height - end_max_y

    # 创建裁剪后的页面
    cropped_page = page
    if page_rotation == 90:
        cropped_page.cropbox.lower_left = (end_max_x, 0)
        cropped_page.cropbox.upper_right = (start_min_x, page_width)
    else:
        cropped_page.cropbox.lower_left = (0, end_pydf2_min_y)
        cropped_page.cropbox.upper_right = (page_width, start_pydf2_max_y)

    writer = PdfWriter()
    writer.add_page(cropped_page)

    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    output_path = os.path.join(output_dir, f"{base_name}.pdf")

    with open(output_path, "wb") as output_file:
        writer.write(output_file)

    return output_path


def process_standard_pdf(pdf_path: str, start_info: dict, end_info: dict,
                         output_dir: str) -> str:
    """
    标准PDF处理:从开始关键词裁剪到结束关键词

    Returns:
        str: 输出文件路径
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    reader = PdfReader(pdf_path)
    writer = PdfWriter()

    # 处理开始关键词页
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

    # 添加中间完整页面
    for i in range(start_info['page_number'], end_info['page_number'] - 1):
        writer.add_page(reader.pages[i])

    # 处理结束关键词页
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

    return output_path
