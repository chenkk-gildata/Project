import re
import requests
import time
import os
from datetime import datetime, timedelta
import pandas as pd
import random
import concurrent.futures

class CnInfoCrawler:
    def __init__(self, max_workers=5):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search',
            'X-Requested-With': 'XMLHttpRequest'
        })
        self.url = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
        self.base_url = "https://www.cninfo.com.cn/new/disclosure/detail"
        self.page_size = 30
        self.max_workers = max_workers  # 最大并发线程数

    def search_announcements(self, start_date, end_date):
        """搜索符合条件的公告（支持分页）"""
        filtered_announcements = []
        bj_end_date = pd.to_datetime(end_date)
        t_1_datetime = pd.to_datetime(start_date) - timedelta(days=1)

        # 构建请求参数配置
        form_data_config ={
            "data": {
                "stock": "",
                "searchkey": "法律意见",
                "plate": "",
                "category": "category_gddh_szsh",
                "trade": "",
                "column": "szse",
                "pageNum": 1,
                "pageSize": self.page_size,
                "tabName": "fulltext",
                "sortName": "",
                "sortType": "",
                "limit": "",
                "showTitle": "",
                "seDate": f"{t_1_datetime}~{end_date}"
            }
        }

        page_num = 1
        config_announcements = []

        while True:
            try:
                # 更新页码
                form_data_config['data']['pageNum'] = page_num

                response = self.session.post(self.url, data=form_data_config['data'])
                response.raise_for_status()
                data = response.json()

                if data.get('announcements'):
                    for announcement in data['announcements']:
                        page_column = announcement.get('pageColumn', '')
                        announcement_time = datetime.fromtimestamp(announcement.get('announcementTime', '') / 1000)

                        try:
                            ann_datetime = pd.to_datetime(announcement_time)

                        except:
                            continue
                        # 应用筛选条件: (pageColumn != 'BJS' AND announcementTime > 开始 t-1日00:00:00.000)
                        # OR pageColumn == 'BJS' AND announcementTime < 结束 t日00:00:00.000)
                        if (page_column != 'BJS' and ann_datetime > t_1_datetime) or (page_column == 'BJS' and ann_datetime < bj_end_date):
                            filtered_announcements.append(announcement)
                            config_announcements.append(announcement)

                    # 检查是否还有下一页
                    total_pages = (data['totalRecordNum'] + self.page_size - 1) // self.page_size
                    if page_num >= total_pages:
                        break
                    page_num += 1

                    # 添加随机延迟，避免请求过快
                    time.sleep(random.uniform(1, 3))
                else:
                    break

            except Exception as e:
                print(f"搜索第 {page_num} 页公告时出错: {e}")
                break

        print(f"总共获取 {len(filtered_announcements)} 条符合条件的公告")

        return filtered_announcements

    def download_pdf(self, announcement, output_dir):
        """下载单个PDF文件"""
        try:
            stock_code = announcement['secCode']
            stock_name = announcement['secName']
            title = announcement['announcementTitle']
            announcement_date = datetime.fromtimestamp(announcement['announcementTime'] / 1000).strftime('%Y.%m.%d')

            # 构建文件名
            clean_title = re.sub(r'[<>:"/\\|?* ]', '', title)
            clean_stock_name = re.sub(r'[<>:"/\\|?*]', '', stock_name)
            filename = f"{stock_code}-{clean_stock_name}-{announcement_date}-{clean_title}.pdf"
            filepath = os.path.join(output_dir, filename)

            # 如果文件已存在，跳过下载
            if os.path.exists(filepath):
                print(f"文件已存在，跳过下载: {filename}")
                return True, filename

            # 获取下载链接
            adjuncturl = announcement.get('adjunctUrl')
            detail_url = f"https://static.cninfo.com.cn/{adjuncturl}"

            # 下载PDF文件
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            pdf_response = requests.get(detail_url, headers=headers, timeout=30)
            pdf_response.raise_for_status()

            # 保存文件
            with open(filepath, 'wb') as f:
                f.write(pdf_response.content)

            print(f"成功下载: {filename}")
            return True, filename

        except Exception as e:
            print(f"下载PDF时出错: {e}")
            return False, announcement.get('announcementTitle', 'Unknown')

    def download_pdf_wrapper(self, args):
        """包装函数用于多线程调用"""
        return self.download_pdf(*args)

    def crawl(self, start_date=None, end_date=None, output_dir="./files"):
        """主爬取函数"""
        # 设置日期范围
        if not start_date or not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = end_date  # 默认只下载当天

        # 创建以当前时间命名的子目录 (格式: YYYYMMDDHHMMSS)
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        output_subdir = os.path.join(output_dir, timestamp)
        os.makedirs(output_subdir, exist_ok=True)

        # 搜索公告
        print(f"搜索 {start_date} 到 {end_date} 的公告...")
        announcements = self.search_announcements(start_date, end_date)

        if not announcements:
            print("没有找到符合条件的公告")
            return

        print(f"开始使用 {self.max_workers} 个线程并发下载...")

        # 准备下载参数
        download_args = [(announcement, output_subdir) for announcement in announcements]

        # 使用线程池并发下载
        successful_downloads = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有下载任务
            future_to_announcement = {
                executor.submit(self.download_pdf_wrapper, args): args[0]
                for args in download_args
            }

            # 处理完成的任务
            for future in concurrent.futures.as_completed(future_to_announcement):
                announcement = future_to_announcement[future]
                try:
                    success, filename = future.result()
                    if success:
                        successful_downloads += 1
                except Exception as e:
                    print(f"下载失败: {announcement.get('announcementTitle', 'Unknown')}, 错误: {e}")

        print(f"采集程序执行成功! 共下载 {successful_downloads}/{len(announcements)} 个文件到目录: {output_subdir}")


def normalize_date(date_str):
    """将各种日期格式转换为标准格式 YYYY-MM-DD"""
    if not date_str:
        return datetime.now().strftime('%Y-%m-%d')

    # 匹配常见的日期分隔符：. / - 等
    pattern = r'(\d{4})[\.\/\-]?(\d{1,2})[\.\/\-]?(\d{1,2})'
    match = re.search(pattern, date_str)

    if match:
        year, month, day = match.groups()
        # 格式化月份和日期为两位数
        month = month.zfill(2)
        day = day.zfill(2)
        return f"{year}-{month}-{day}"
    else:
        # 如果无法识别格式，返回当天日期
        return datetime.now().strftime('%Y-%m-%d')


def main():
    crawler = CnInfoCrawler()
    # 获取用户输入的时间范围
    start_date_input = input(f"请输入开始日期 (空直接查询当天日期): ")
    start_date = normalize_date(start_date_input)

    if start_date_input:
        while True:
            end_date_input = input(f"请输入结束日期: ")
            if end_date_input:
                end_date = normalize_date(end_date_input)
                if end_date < start_date:
                    print("错误：结束日期不能小于开始日期，请重新输入")
                    continue
                break
            else:
                end_date = start_date
                break
    else:
        start_date = end_date = datetime.now().strftime('%Y-%m-%d')

    # 开始爬取
    return crawler.crawl(start_date, end_date)


# 添加可导入的函数
def run_crawler(start_date=None, end_date=None, output_dir="./files"):
    """可导入的爬虫运行函数"""
    crawler = CnInfoCrawler()
    return crawler.crawl(start_date, end_date, output_dir)


if __name__ == "__main__":
    main()
    # print("采集程序执行成功")
