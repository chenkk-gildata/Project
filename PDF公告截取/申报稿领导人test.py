import os
import re
import fitz
from PyPDF2 import PdfReader, PdfWriter


def get_exchange_code(pdf_path):
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
    
    start_pattern = re.compile(r'^[（(]?[\d一二三四五六七八九十]+[)）]?[、.．]?\s*(?:发行人|公司)?董事.*(?:监事|审计委员会委员)?[、和与及]高级管理人员[、和与及]?(?:其他)?(?:核\s*心\s*(?:\s*技\s*术)?\s*人\s*员)?\s*(?:的?\s*(?:简\s*要|基\s*本)\s*情\s*况|(?:情\s*况(?:简\s*介)?|简\s*介))?\s*$', re.IGNORECASE | re.MULTILINE)
    end_pattern = re.compile(r'^[（(]?[\d一二三四五六七八九十]+[)）]?[、.．]?\s*(?:(?:发行人|公司)?董事.*(?:监事)?[、和与及]高级管理人员[、和与及]?(?:其他)?(?:核\s*心(?:\s*技\s*术)?\s*人\s*员)?\s*(?:在\s*其\s*他\s*单\s*位|对\s*外|的)?兼\s*职(?:/任职)?\s*情\s*况|直接或间接持有发行人股份的情况\s*$)', re.IGNORECASE | re.MULTILINE)
    if exchange_code == 'bjs':
        end_keyword = ["兼职", "职情况", "间接持有"]
    else:    
        end_keyword = ["兼职", "职情况"]
    
    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    
    start_info = None
    end_info = None
    committee_info = None
    core_tech_start_info = None
    core_tech_end_info = None
    
    committee_start_pattern = re.compile(r'委员会.*(?:(?:建立健全|人员构成|设置|构成|制度)?及其?运行情况|设置情况(?:说明)?)\s*$', re.IGNORECASE | re.MULTILINE)
    committee_start_keyword = ["运行情况", "设置情况"]
    
    if exchange_code == 'bjs':
        core_tech_start_pattern = re.compile(r'^[（(]?[\d一二三四五六七八九十][)）]?[、.．]?\s*核心技术人员(?:总体|基本)?(?:情况|简历)\s*$', re.IGNORECASE | re.MULTILINE)
        core_tech_start_keyword = ["核心技术人员"]
        core_tech_end_pattern = re.compile(r'^(?!.*简历).*[（(]?[\d一二三四五六七八九十][)）]?[、.．]?\s*(?:核心技术人员)?.*(?:成果|持(?:股|有发行人|有公司).*情况)', re.IGNORECASE | re.MULTILINE)
        core_tech_end_keyword = ["成果", "持股", "持有"]
    
    search_ranges = [(35, 150)]

    def get_search_rect(page, inst, keyword_type):
        """根据关键词类型和页面旋转角度智能调整搜索区域"""
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

            if not end_info:
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

            if start_info and end_info:
                found = True
                break
        
        if found:
            break
    
    if exchange_code == 'bjs':
        committee_search_start = max(1, total_pages - 150)
        print(f"委员会搜索范围：从第 {committee_search_start} 页开始往前搜索")
    else:
        committee_search_start = total_pages
        print(f"委员会搜索范围：从第 {total_pages} 页开始往前搜索")
    
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
                print(f"找到委员会开始关键词在第 {page_num + 1} 页")
                break
    
    if exchange_code == 'bjs':
        if committee_info:
            core_tech_search_start = committee_info['page_number']
            print(f"核心技术人员搜索范围：从委员会开始页第 {core_tech_search_start} 页继续往前搜索")
        else:
            core_tech_search_start = committee_search_start
            print(f"核心技术人员搜索范围：未找到委员会，从第 {core_tech_search_start} 页开始往前搜索")
        
        for page_num in range(core_tech_search_start - 1, -1, -1):
            page = doc.load_page(page_num)
            page_rect = page.rect
            
            if not core_tech_start_info:
                result = collect_and_sort_instances(page, core_tech_end_keyword, core_tech_end_pattern, get_search_rect)
                if result:
                    inst = result['inst']
                    core_tech_end_info = {
                        'page_number': page_num + 1,
                        'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                        'page_dimensions': (page_rect.width, page_rect.height)
                    }
                    print(f"找到核心技术人员结束关键词在第 {page_num + 1} 页")
            
            if not core_tech_start_info and core_tech_end_info:
                result = collect_and_sort_instances(page, core_tech_start_keyword, core_tech_start_pattern, get_search_rect)
                if result:
                    inst = result['inst']
                    core_tech_start_info = {
                        'page_number': page_num + 1,
                        'keyword_box': (inst.x0, inst.y0, inst.x1, inst.y1),
                        'page_dimensions': (page_rect.width, page_rect.height)
                    }
                    print(f"找到核心技术人员开始关键词在第 {page_num + 1} 页")
                    break
    
    doc.close()
    
    return {
        'start': start_info,
        'end': end_info,
        'committee': committee_info,
        'core_tech_start': core_tech_start_info,
        'core_tech_end': core_tech_end_info
    }


def find_committee_additional_pages(pdf_path, start_page_number):
    """
    从委员会开始页后一页开始，判断后续页面是否需要输出
    
    判断逻辑：
    1. 只检查开始关键词页的后一页
    2. 查询页面是否包含"委员会"关键词
    3. 如果包含，检查第一个出现位置是否在页面30%以上（y0 < page_height * 0.3）
    4. 满足条件则完整输出该页
    5. 不满足条件则不输出该页
    
    参数:
        pdf_path: PDF文件路径
        start_page_number: 委员会开始页码（1-based）
    
    返回:
        list: 需要完整输出的后续页面号列表（1-based），最多1页
    """
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
                print(f"委员会后续页面第 {page_num + 1} 页包含'委员会'关键词且位置在40%以上，完整输出")
            else:
                print(f"委员会后续页面第 {page_num + 1} 页'委员会'关键词位置在40%以下，仅输出1页")
        else:
            print(f"委员会后续页面第 {page_num + 1} 页不包含'委员会'关键词，仅输出1页")
    
    doc.close()
    return additional_pages


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


def crop_page_after_keyword(pdf_path, keyword_info, output_dir, pagenum=4):
    """根据关键词位置裁剪PDF页面，保留关键词及之后的内容"""
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

        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        output_path = os.path.join(output_dir, f"{base_name}.pdf")

        with open(output_path, "wb") as output_file:
            writer.write(output_file)

        print(f"已特殊处理并保存到: {output_path}")
        return output_path
    except Exception as e:
        print(f"裁剪页面时出错: {str(e)}")
        return None


def crop_page_with_rotation(page, keyword_info, crop_type='top'):
    """
    根据页面旋转角度裁剪页面
    
    参数:
        page: PyPDF2页面对象
        keyword_info: 关键词信息字典
        crop_type: 裁剪类型
            - 'top': 保留关键词及上方内容（裁剪下方）
            - 'bottom': 保留关键词及下方内容（裁剪上方）
            - 'middle': 保留两个关键词之间的内容
    
    返回:
        裁剪后的页面对象
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


def process_pdf(pdf_path, keywords, output_dir):
    """通用PDF处理函数，处理交易所PDF"""
    start_info = keywords.get('start')
    end_info = keywords.get('end')
    committee_info = keywords.get('committee')
    core_tech_start_info = keywords.get('core_tech_start')
    core_tech_end_info = keywords.get('core_tech_end')
    
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
            
            for page_num in range(start_info['page_number'] - 1, end_info['page_number']):
                page = reader.pages[page_num]
                
                if page_num == start_info['page_number'] - 1:
                    cropped_page = crop_page_with_rotation(page, start_info, 'bottom')
                    writer.add_page(cropped_page)
                elif page_num == end_info['page_number'] - 1:
                    cropped_page = crop_page_with_rotation(page, end_info, 'top')
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
                            cropped_page = crop_page_with_rotation(page, core_tech_start_info, 'bottom')
                            writer.add_page(cropped_page)
                        elif page_num == core_tech_end_info['page_number'] - 1:
                            cropped_page = crop_page_with_rotation(page, core_tech_end_info, 'top')
                            writer.add_page(cropped_page)
                        else:
                            writer.add_page(page)
            else:
                exchange_code = get_exchange_code(pdf_path)
                if exchange_code == 'bjs':
                    print(f"未找到核心技术人员信息，跳过核心技术人员处理")
                else:
                    print(f"非北交所代码，跳过核心技术人员处理")
            
            if committee_info:
                committee_page_num = committee_info['page_number'] - 1
                page = reader.pages[committee_page_num]
                
                cropped_page = crop_page_with_rotation(page, committee_info, 'bottom')
                writer.add_page(cropped_page)
                
                additional_pages = find_committee_additional_pages(pdf_path, committee_info['page_number'])
                for page_num in additional_pages:
                    page = reader.pages[page_num - 1]
                    writer.add_page(page)
            else:
                print(f"未找到委员会信息，跳过委员会处理")
            
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
            output_dir = os.path.join(os.path.dirname(path), "申报稿领导人")
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
        output_dir = os.path.join(path, "申报稿领导人")

        for pdf_file in pdf_files:
            process_pdf_with_keywords(pdf_file, output_dir)
    else:
        print("指定的路径不存在")


def test_committee_search(path):
    """
    测试委员会搜索功能，支持单文件或目录
    
    输出调试信息：
    - 交易所类型
    - 搜索范围
    - 找到的委员会开始页
    - 后续页面判断结果
    """
    if os.path.isfile(path):
        if not path.lower().endswith('.pdf'):
            print("指定的文件不是PDF文件")
            return
        pdf_files = [path]
    elif os.path.isdir(path):
        pdf_files = [os.path.join(path, f) for f in os.listdir(path)
                     if os.path.isfile(os.path.join(path, f)) and f.lower().endswith('.pdf')]
        if not pdf_files:
            print("指定目录中未找到PDF文件")
            return
        print(f"找到 {len(pdf_files)} 个PDF文件")
    else:
        print("指定的路径不存在")
        return
    
    for pdf_path in pdf_files:
        print(f"\n{'=' * 50}")
        print(f"文件: {os.path.basename(pdf_path)}")
        
        exchange_code = get_exchange_code(pdf_path)
        print(f"交易所类型: {exchange_code}")
        
        doc = fitz.open(pdf_path)
        total_pages = len(doc)
        doc.close()
        
        if exchange_code == 'bjs':
            search_start = max(1, total_pages - 150)
            print(f"搜索范围: 第 {search_start} 页到第 {total_pages} 页（从后往前）")
        else:
            search_start = total_pages
            print(f"搜索范围: 从第 {total_pages} 页开始往前搜索")
        
        keywords = find_keywords(pdf_path)
        committee_info = keywords.get('committee')
        
        if committee_info:
            print(f"找到委员会开始页: 第 {committee_info['page_number']} 页")
            
            additional_pages = find_committee_additional_pages(pdf_path, committee_info['page_number'])
            if additional_pages:
                print(f"需要输出的后续页面: {additional_pages}")
            else:
                print("后续页面不满足条件，不输出")
        else:
            print("未找到委员会信息")


def test_core_tech_search(path):
    """测试核心技术人员搜索功能，支持单文件或目录"""
    if os.path.isfile(path):
        if not path.lower().endswith('.pdf'):
            print("指定的文件不是PDF文件")
            return
        pdf_files = [path]
    elif os.path.isdir(path):
        pdf_files = [os.path.join(path, f) for f in os.listdir(path)
                     if os.path.isfile(os.path.join(path, f)) and f.lower().endswith('.pdf')]
        if not pdf_files:
            print("指定目录中未找到PDF文件")
            return
        print(f"找到 {len(pdf_files)} 个PDF文件")
    else:
        print("指定的路径不存在")
        return
    
    for pdf_path in pdf_files:
        print(f"\n{'=' * 50}")
        print(f"文件: {os.path.basename(pdf_path)}")
        
        exchange_code = get_exchange_code(pdf_path)
        if exchange_code != 'bjs':
            print("非北交所文件，无需处理核心技术人员")
            continue
        
        print(f"交易所类型: {exchange_code}")
        
        keywords = find_keywords(pdf_path)
        core_tech_start = keywords.get('core_tech_start')
        core_tech_end = keywords.get('core_tech_end')
        
        if core_tech_start and core_tech_end:
            print(f"找到核心技术人员开始页: 第 {core_tech_start['page_number']} 页")
            print(f"找到核心技术人员结束页: 第 {core_tech_end['page_number']} 页")
        elif core_tech_end and not core_tech_start:
            print(f"只找到核心技术人员结束页: 第 {core_tech_end['page_number']} 页，未找到开始页")
        else:
            print("未找到核心技术人员信息")


def main():
    """主函数"""
    print("PDF关键字页面裁剪工具")
    print("=" * 50)

    path = input("请输入文件或目录路径: ").strip()
    process_path(path)


if __name__ == "__main__":
    main()
    # test_core_tech_search(r"C:\Users\chenkk\Desktop\发行公告书")
