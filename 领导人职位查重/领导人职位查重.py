import pandas as pd
from collections import Counter
import pyodbc
import sys


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


def read_data_from_sql():
    """从SQL Server读取数据"""
    conn = get_db_connection()
    if conn:
        try:
            sql_query = """
            --领导人持股
            SELECT '领导人持股' SJBD,A.ID,B.GPDM,A.LDXM XM,A.ZWMS,CONVERT(DATE,A.XXFBSJ) XXFBRQ
            FROM [10.101.0.212].JYPRIME.dbo.usrZYLDRCG A
             INNER JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B
              ON A.INBBM = B.INBBM AND B.ZQSC IN (83,90,18) AND B.ZQLB IN (1,2,41)
            WHERE A.ZWMS LIKE '%、%' 
            UNION ALL
            --领导人介绍
            SELECT '领导人介绍' SJBD,A.ID,B.GPDM,A.XM,A.ZWMS,CONVERT(DATE,A.XXFBRQ) XXFBRQ
            FROM [10.101.0.212].JYPRIME.dbo.usrGSZYLDRJS A
             INNER JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B
              ON A.INBBM = B.INBBM AND B.ZQSC IN (83,90,18) AND B.ZQLB IN (1,2,41)
            WHERE A.ZWMS LIKE '%、%'
            UNION ALL
            --领导人股份变动
            SELECT '领导人股份变动' SJBD,A.ID,B.GPDM,A.LDXM XM,A.ZWMS,CONVERT(DATE,A.XXFBRQ) XXFBRQ
            FROM [10.101.0.212].JYPRIME.dbo.usrGSJYLDRGFBD A
             INNER JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B
              ON A.INBBM = B.INBBM AND B.ZQSC IN (83,90,18) AND B.ZQLB IN (1,2,41)
            WHERE A.ZWMS LIKE '%、%'
            ORDER BY 4 DESC
            """

            df = pd.read_sql(sql_query, conn)
            print(f"\n成功读取 {len(df)} 条数据")
            return df
        except Exception as e:
            print(f"\nSQL查询失败: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    else:
        return pd.DataFrame()


def filter_duplicate_positions(df):
    """过滤重复职位的数据"""

    def has_duplicate_in_string(positions_str):
        if pd.isna(positions_str):
            return False
        positions = [pos.strip() for pos in positions_str.split('、') if pos.strip()]
        position_counts = Counter(positions)
        return any(count > 1 for count in position_counts.values())

    result_df = df[df['ZWMS'].apply(has_duplicate_in_string)]
    print(f"\n过滤出 {len(result_df)} 条重复职位数据")
    return result_df


# 主程序
if __name__ == "__main__":

    # 程序说明

    print("=" * 22 + " 领导人职位查重小程序 " + "=" * 22 + "\n")
    print("功能说明：查询领导人介绍表和领导人持股表里的职位描述是否有重复\n")
    print("=" * 65+ "\n")

    # 执行选项
    choice = ""
    while choice != "2":

        choice = input("请选择操作：\n\n1. 执行程序\n2. 退出程序\n\n请输入选择 : ")

        if choice == "1":

            # 从SQL读取数据
            df = read_data_from_sql()

            if not df.empty:
                # 过滤重复职位的数据
                filtered_df = filter_duplicate_positions(df)

                if not filtered_df.empty:
                    # 保存到Excel
                    filtered_df.to_excel("领导人持股职位重复.xlsx", index=False)
                    print("\n数据处理完成，已保存到Excel文件\n")
                else:
                    print("\n未找到重复职位的数据\n")
            else:
                print("\n未能从数据库读取数据\n")

            exit_choice = ""
            while exit_choice.lower() not in ['n', 'no', '否']:
                exit_choice = input("程序执行完成，是否退出？ (y/n): ")
                if exit_choice.lower() in ['y', 'yes', '是']:
                    print("程序退出")
                    sys.exit()

                elif exit_choice.lower() in ['n', 'no', '否']:
                    break
                else:
                    print("无效选择，请重新输入...")
                continue

        elif choice == "2":
            print("程序退出")
            sys.exit()
        else:
            input("无效选择，请重新输入：")
            continue



