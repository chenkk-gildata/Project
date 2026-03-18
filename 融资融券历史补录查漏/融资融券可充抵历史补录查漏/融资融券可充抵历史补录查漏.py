import glob
import time
import pandas as pd
import pyodbc
import re
from tqdm import tqdm
from datetime import datetime
import os

# 数据库连接配置 - 请根据实际情况修改
SERVER = '10.102.25.11,8080'  # 服务器名称或IP地址
USERNAME = 'WebResourceNew_Read'  # 登录用户名
PASSWORD = 'New_45ted'  # 登录密码
DRIVER = 'ODBC Driver 17 for SQL Server'  # ODBC驱动版本


def get_db_connection():
    """建立SQL Server数据库连接"""
    try:
        conn_str = (
            f'DRIVER={{{DRIVER}}};'
            f'SERVER={SERVER};'
            f'UID={USERNAME};'
            f'PWD={PASSWORD}'
        )
        conn = pyodbc.connect(conn_str)
        print("\n数据库连接成功，程序正在执行...")
        return conn
    except Exception as e:
        print(f"\n数据库连接失败: {e}")
        return None


def read_data_from_sql(start_date, end_date, info_source):
    """从SQL Server读取数据，根据日期区间和信息来源"""
    conn = get_db_connection()
    if conn:
        try:
            sql_query = f"""
            SELECT CONVERT(DATE,XXFBRQ) XXFBRQ,ZQDM,ZQJC
            FROM [10.101.0.212].JYPRIME.dbo.usrRZRQBDZQMX
            WHERE XXLY='{info_source}' AND XXFBRQ BETWEEN '{start_date}' AND '{end_date}'
            """

            df = pd.read_sql(sql_query, conn)
            print(f"\n成功读取数据库数据： {len(df)} 条")
            return df
        except Exception as e:
            print(f"\nSQL查询失败: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    else:
        return pd.DataFrame()


def select_excel_files():
    """选择Excel文件并返回文件列表和日期范围"""
    print("\n当前目录下的Excel文件:")
    excel_files = glob.glob("*.xlsx") + glob.glob("*.xls")

    if not excel_files:
        print("未找到任何Excel文件")
        return None, None, None

    for i, file in enumerate(excel_files, 1):
        print(f"{i}. {file}")

    while True:
        try:
            choice = input("\n请选择要比对的Excel文件 (输入文件编号，多个文件用逗号分隔，输入q退出): ").strip()
            if choice.lower() == 'q':
                return None, None, None

            selected_files = []
            selected_indices = []

            # 处理多文件选择
            if ',' in choice:
                indices = [int(x.strip()) - 1 for x in choice.split(',') if x.strip().isdigit()]
                for idx in indices:
                    if 0 <= idx < len(excel_files):
                        selected_files.append(excel_files[idx])
                        selected_indices.append(idx)
            else:
                # 单文件选择
                file_index = int(choice) - 1
                if 0 <= file_index < len(excel_files):
                    selected_files.append(excel_files[file_index])
                    selected_indices.append(file_index)

            if not selected_files:
                print("无效的选择，请重新输入")
                continue

            print(f"已选择 {len(selected_files)} 个文件:")
            for i, file in enumerate(selected_files, 1):
                print(f"{i}. {file}")

            # 从文件名中提取日期范围
            all_dates = []
            for file in selected_files:
                date_range = extract_dates_from_filename(file)
                if date_range:
                    all_dates.extend(date_range)

            if all_dates:
                # 转换为datetime对象以便比较
                date_objs = [datetime.strptime(d, '%Y-%m-%d') for d in all_dates]
                start_date = min(date_objs).strftime('%Y-%m-%d')
                end_date = max(date_objs).strftime('%Y-%m-%d')
                print(f"从文件名提取的日期范围: {start_date} 至 {end_date}")
                return selected_files, start_date, end_date
            else:
                print("无法从文件名中提取日期范围，请手动输入日期")
                start_date = input("请输入开始日期(格式: YYYY-MM-DD): ").strip()
                end_date = input("请输入结束日期(格式: YYYY-MM-DD): ").strip()
                return selected_files, start_date, end_date

        except ValueError:
            print("请输入有效的数字")


def extract_dates_from_filename(filename):
    """从文件名中提取日期范围"""
    # 尝试匹配 YYYYMMDD-YYYYMMDD 格式
    pattern1 = r'(\d{4})(\d{2})(\d{2})-(\d{4})(\d{2})(\d{2})'
    match1 = re.search(pattern1, filename)

    if match1:
        start_date = f"{match1.group(1)}-{match1.group(2)}-{match1.group(3)}"
        end_date = f"{match1.group(4)}-{match1.group(5)}-{match1.group(6)}"
        return [start_date, end_date]

    # 尝试匹配 YYYY-MM-DD_YYYY-MM-DD 格式
    pattern2 = r'(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})'
    match2 = re.search(pattern2, filename)

    if match2:
        return [match2.group(1), match2.group(2)]

    # 尝试匹配单个日期
    single_date_patterns = [
        r'(\d{4})(\d{2})(\d{2})',
        r'(\d{4}-\d{2}-\d{2})'
    ]

    dates = []
    for pattern in single_date_patterns:
        matches = re.findall(pattern, filename)
        for match in matches:
            if isinstance(match, tuple):
                date_str = f"{match[0]}-{match[1]}-{match[2]}"
            else:
                date_str = match
            dates.append(date_str)

    if dates:
        return dates

    return None


def read_excel_data(file_paths):
    """读取多个Excel文件数据并合并"""
    all_dfs = []
    total_records = 0

    for file_path in file_paths:
        try:
            # 使用dtype参数指定stockCode列为字符串类型，保留前导零
            df = pd.read_excel(file_path, dtype={'stockCode': str})
            print(f"成功读取Excel文件: {file_path}, 共 {len(df)} 条数据")
            all_dfs.append(df)
            total_records += len(df)
        except Exception as e:
            print(f"读取Excel文件 {file_path} 失败: {e}")

    if all_dfs:
        # 合并所有DataFrame
        combined_df = pd.concat(all_dfs, ignore_index=True)
        print(f"合并后总数据量: {len(combined_df)} 条")
        return combined_df
    else:
        print("所有Excel文件读取失败")
        return pd.DataFrame()


def optimize_compare_data(sql_df, excel_df):
    print("开始数据比对...")

    # 确保日期格式一致
    sql_df['XXFBRQ'] = pd.to_datetime(sql_df['XXFBRQ']).dt.date
    if '日期' in excel_df.columns:
        excel_df['日期'] = pd.to_datetime(excel_df['日期']).dt.date

    # 创建复合键用于比对
    sql_df['复合键'] = sql_df['XXFBRQ'].astype(str) + '_' + sql_df['ZQDM'].astype(str)
    excel_df['复合键'] = excel_df['日期'].astype(str) + '_' + excel_df['stockCode'].astype(str)

    # 检查是否有重复的复合键
    sql_duplicates = sql_df['复合键'].duplicated().sum()
    excel_duplicates = excel_df['复合键'].duplicated().sum()

    if sql_duplicates > 0:
        print(f"警告: 数据库中存在 {sql_duplicates} 条重复记录")
        # 去除重复记录，保留第一条
        sql_df = sql_df.drop_duplicates(subset=['复合键'], keep='first')

    if excel_duplicates > 0:
        print(f"警告: Excel中存在 {excel_duplicates} 条重复记录")
        # 去除重复记录，保留第一条
        excel_df = excel_df.drop_duplicates(subset=['复合键'], keep='first')

    # 使用集合操作提高比对效率
    sql_keys = set(sql_df['复合键'].values)
    excel_keys = set(excel_df['复合键'].values)

    print(f"数据库记录数: {len(sql_keys)}, Excel记录数: {len(excel_keys)}")

    # 找出差异
    print("计算差异...")
    missing_in_sql = excel_keys - sql_keys  # Excel有但数据库没有
    missing_in_excel = sql_keys - excel_keys  # 数据库有但Excel没有

    print(f"Excel多出记录: {len(missing_in_sql)} 条")
    print(f"数据库多出记录: {len(missing_in_excel)} 条")

    # 创建结果DataFrame
    result_data = []

    # 处理Excel中多出的记录
    print("处理Excel多出记录...")
    # 使用字典推导式创建映射，避免重复键问题
    excel_dict = {row['复合键']: dict(row) for _, row in excel_df.iterrows()}

    if missing_in_sql:
        for key in tqdm(missing_in_sql, desc="处理Excel多出记录"):
            if key in excel_dict:
                row_data = excel_dict[key].copy()
                row_data['备注'] = '漏处理'
                result_data.append(row_data)

    # 处理数据库中多出的记录
    print("处理数据库多出记录...")
    sql_dict = {row['复合键']: dict(row) for _, row in sql_df.iterrows()}

    if missing_in_excel:
        for key in tqdm(missing_in_excel, desc="处理数据库多出记录"):
            if key in sql_dict:
                row_data = sql_dict[key].copy()
                row_data['备注'] = '多处理'
                # 重命名列以匹配Excel格式
                row_data['日期'] = row_data.pop('XXFBRQ')
                row_data['stockCode'] = row_data.pop('ZQDM')
                row_data['stockName'] = row_data.pop('ZQJC')
                result_data.append(row_data)

    if result_data:
        result_df = pd.DataFrame(result_data)
        # 移除临时添加的复合键列
        if '复合键' in result_df.columns:
            result_df = result_df.drop('复合键', axis=1)
        return result_df
    else:
        print("没有发现差异数据")
        return pd.DataFrame()


def save_result_to_excel(result_df, excel_filenames, start_date, end_date, info_source):
    """保存比对结果到Excel文件"""
    try:
        # 按日期和股票代码正序排序
        if '日期' in result_df.columns and 'stockCode' in result_df.columns:
            result_df = result_df.sort_values(by=['日期', 'stockCode'], ascending=[True, True])

        # 生成输出文件名
        if len(excel_filenames) == 1:
            base_name = os.path.splitext(excel_filenames[0])[0]
            output_filename = f"{base_name}_比对结果_{info_source}.xlsx"
        else:
            # 多文件情况下使用日期范围作为文件名
            start_str = start_date.replace('-', '')
            end_str = end_date.replace('-', '')
            output_filename = f"多文件比对结果_{info_source}_{start_str}-{end_str}.xlsx"

        result_df.to_excel(output_filename, index=False, engine='openpyxl')
        print(f"比对结果已保存到: {output_filename}")
        return True
    except Exception as e:
        print(f"保存结果失败: {e}")
        return False


def get_info_source():
    """获取用户输入的信息来源"""
    print("\n" + "=" * 50)
    info_source = input("请输入信息来源(例如: 方正证券): ").strip()
    return info_source


def validate_dates(start_date, end_date):
    """验证日期格式"""
    try:
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
        if start_date > end_date:
            print("错误：开始日期不能晚于结束日期")
            return False
        return True
    except ValueError:
        print("错误：日期格式不正确")
        return False


def main_process():
    """主处理流程"""
    # 选择Excel文件并提取日期
    excel_files, start_date, end_date = select_excel_files()
    if not excel_files:
        return False

    # 验证日期格式
    if not validate_dates(start_date, end_date):
        print("日期格式验证失败，请重新选择文件")
        return False

    # 获取信息来源
    info_source = get_info_source()

    # 从SQL读取数据
    print(f"\n开始读取数据库数据({start_date} 至 {end_date}, 来源: {info_source})...")
    sql_df = read_data_from_sql(start_date, end_date, info_source)

    if sql_df.empty:
        print("数据库读取，请检查参数后重试")
        return False

    # 读取Excel文件
    print(f"读取 {len(excel_files)} 个Excel文件...")
    excel_df = read_excel_data(excel_files)

    if excel_df.empty:
        print("Excel文件读取失败")
        return False

    start_time = time.time()
    result_df = optimize_compare_data(sql_df, excel_df)
    end_time = time.time()

    print(f"\n比对完成，耗时: {end_time - start_time:.2f} 秒，正在导出比对结果……")

    if not result_df.empty:
        # 保存结果
        save_result_to_excel(result_df, excel_files, start_date, end_date, info_source)
        print(f"共发现 {len(result_df)} 条差异记录")
    else:
        print("数据完全一致，没有差异记录")

    return True


# 主程序
if __name__ == "__main__":
    print("=" * 50 + "\n")
    print("融资融券可充抵查漏比对程序")
    print("\n" + "=" * 50)

    while True:
        try:
            success = main_process()

            # 询问是否继续查询
            print("\n" + "=" * 50)
            continue_query = input("是否继续查询？(y/n): ").strip().lower()

            if continue_query not in ['y', 'yes', '是']:
                print("程序结束，感谢使用！")
                break

        except KeyboardInterrupt:
            print("\n\n程序被用户中断")
            break
        except Exception as e:
            print(f"\n程序执行出错: {e}")
            retry = input("是否重试？(y/n): ").strip().lower()
            if retry not in ['y', 'yes', '是']:
                break