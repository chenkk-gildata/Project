"""
公司主要领导人介绍中英文姓名比对。

运行前请确保已安装依赖：
    python -m pip install pandas pyodbc openpyxl pypinyin

数据库连接优先读取环境变量 SQLSERVER_CONN_STR，例如：
    $env:SQLSERVER_CONN_STR="DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.101.0.212;DATABASE=JYPRIME;Trusted_Connection=yes;TrustServerCertificate=yes"

也可以在命令行传入：
    python compare_leader_names.py --conn-str "DRIVER=...;SERVER=...;DATABASE=..."
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import unicodedata
import warnings
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyodbc
import requests
from pypinyin import Style, lazy_pinyin

SERVER = '10.102.25.11,8080'  # 服务器名称或IP地址
USERNAME = 'WebResourceNew_Read'  # 登录用户名
PASSWORD = 'New_45ted'  # 登录密码
DRIVER = 'ODBC Driver 17 for SQL Server'  # ODBC驱动版本


ROOT_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = ROOT_DIR / "领导人介绍中英文姓名比对结果.xlsx"
DINGTALK_WEBHOOK_URL = (
    # "https://oapi.dingtalk.com/robot/send?access_token=3050a412c9039d5d3471f64b9b6b23463d6c79bc5cb6fe217ea915542855cf8c"  # 测试
    "https://oapi.dingtalk.com/robot/send?access_token=4075b24e20854d960db8b393f9b592b323a5eadb21a591df7e6adb596fad5d6f"  # 外包群
)
DINGTALK_SECURITY_KEYWORD = ""

# 已确认无问题的原数据库 ID 写在这里，支持中文记录 ID 或英文记录 ID。
IGNORED_SOURCE_IDS = set()

# 通知人-默认 许豆豆
phone_at = "@15940201885"

FINAL_OUTPUT_COLUMNS = [
    "股票代码",
    "证券简称",
    "姓名_中文",
    "姓名_英文",
    "性别",
    "出生日期_中文",
    "出生日期_英文",
]
COMPOUND_SURNAMES = [
    "欧阳",
    "司马",
    "上官",
    "诸葛",
    "东方",
    "夏侯",
    "皇甫",
    "尉迟",
    "公孙",
    "慕容",
    "长孙",
    "司徒",
    "司空",
]

DEFAULT_CONN_STR = (
    f"DRIVER={DRIVER};"
    f"SERVER={SERVER};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
)

ENGLISH_NAME_SQL = """
SELECT B.GPDM 股票代码,
       B.ZQJC 证券简称,
       CONVERT(DATE,A.XXFBRQ) 信息发布日期,
       A.XM 姓名,
       A.XB 性别,
       A.CSRQ 出身日期,
       A.CZYF 存在与否,
       A.XGSJ,
       A.ID
FROM [10.101.0.212].JYPRIME.dbo.usrGSZYLDRJS A
JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B
  ON A.IGSDM=B.IGSDM
 AND B.ZQLB IN (1,2)
 AND B.SSBZ IN (1,2,6,7,8)
 AND B.ZQJC NOT LIKE '%无效%'
WHERE A.XM NOT LIKE N'%[一-龥]%'
ORDER BY B.GPDM
"""

CHINESE_NAME_SQL = """
SELECT B.GPDM 股票代码,
       B.ZQJC 证券简称,
       CONVERT(DATE,A.XXFBRQ) 信息发布日期,
       A.XM 姓名,
       A.XB 性别,
       A.CSRQ 出身日期,
       A.CZYF 存在与否,
       A.XGSJ,
       A.ID
FROM [10.101.0.212].JYPRIME.dbo.usrGSZYLDRJS A
JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B
  ON A.IGSDM=B.IGSDM
 AND B.ZQLB IN (1,2)
 AND B.SSBZ IN (1,2,6,7,8)
 AND B.ZQJC NOT LIKE '%无效%'
WHERE A.XM NOT LIKE '%[a-zA-Z]%'
ORDER BY B.GPDM
"""


def is_blank(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def normalize_text(value: Any) -> str:
    if is_blank(value):
        return ""
    return unicodedata.normalize("NFKC", str(value)).strip()


def chinese_name_to_pinyin(name: Any) -> str:
    text = normalize_text(name)
    if not text:
        return ""
    parts = lazy_pinyin(
        text,
        style=Style.NORMAL,
        errors="ignore",
        strict=False,
        v_to_u=False,
    )
    return " ".join(part for part in parts if part)


def chinese_name_parts(name: Any) -> tuple[str, str]:
    text = normalize_text(name)
    if not text:
        return "", ""
    for surname in COMPOUND_SURNAMES:
        if text.startswith(surname) and len(text) > len(surname):
            return surname, text[len(surname):]
    if len(text) <= 1:
        return text, ""
    return text[:1], text[1:]


def chinese_name_match_keys(name: Any) -> list[str]:
    surname, given_name = chinese_name_parts(name)
    full_key = normalize_name(chinese_name_to_pinyin(name))
    if not surname or not given_name:
        return [full_key] if full_key else []

    surname_key = normalize_name(chinese_name_to_pinyin(surname))
    given_name_key = normalize_name(chinese_name_to_pinyin(given_name))
    keys = [full_key, given_name_key + surname_key]
    return list(dict.fromkeys(key for key in keys if key))


def normalize_name(name: Any) -> str:
    text = normalize_text(name).lower()
    text = text.replace("ü", "v").replace("u:", "v")
    return re.sub(r"[^a-z0-9]", "", text)


def normalize_gender(gender: Any) -> str:
    text = normalize_text(gender).upper()
    if text in {"男", "M", "MALE"}:
        return "M"
    if text in {"女", "F", "FEMALE"}:
        return "F"
    return text


def normalize_birth_date(value: Any) -> str:
    if is_blank(value):
        return ""
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    text = normalize_text(value)
    if not text:
        return ""

    chinese_match = re.search(
        r"(?P<year>\d{4})\s*年(?:\s*(?P<month>\d{1,2})\s*月(?:\s*(?P<day>\d{1,2})\s*日?)?)?",
        text,
    )
    if chinese_match:
        return format_date_parts(
            chinese_match.group("year"),
            chinese_match.group("month"),
            chinese_match.group("day"),
        )

    numeric_match = re.search(
        r"(?P<year>\d{4})(?:[-/.](?P<month>\d{1,2})(?:[-/.](?P<day>\d{1,2}))?)?",
        text,
    )
    if numeric_match:
        return format_date_parts(
            numeric_match.group("year"),
            numeric_match.group("month"),
            numeric_match.group("day"),
        )

    return text


def format_date_parts(year: str, month: str | None, day: str | None) -> str:
    result = year
    if month:
        result += f"-{int(month):02d}"
    if day:
        result += f"-{int(day):02d}"
    return result


def birth_dates_match(left: Any, right: Any) -> bool:
    left_date = normalize_birth_date(left)
    right_date = normalize_birth_date(right)
    if not left_date or not right_date:
        return False
    shorter, longer = sorted((left_date, right_date), key=len)
    return longer == shorter or longer.startswith(shorter + "-")


def normalize_source_id(value: Any) -> str:
    if is_blank(value):
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return normalize_text(value)


def add_match_columns(df: pd.DataFrame, *, is_chinese: bool) -> pd.DataFrame:
    result = df.copy()
    if is_chinese:
        result["姓名拼音"] = result["姓名"].map(chinese_name_to_pinyin)
        result["姓名_标准化"] = result["姓名"].map(chinese_name_match_keys)
        result = result.explode("姓名_标准化")
    else:
        result["姓名拼音"] = ""
        result["姓名_标准化"] = result["姓名"].map(normalize_name)
    result["性别_标准化"] = result["性别"].map(normalize_gender)
    result["出生日期_标准化"] = result["出身日期"].map(normalize_birth_date)
    return result


def read_sql_data(conn_str: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    with pyodbc.connect(conn_str) as conn:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="pandas only supports SQLAlchemy connectable.*",
                category=UserWarning,
            )
            english_df = pd.read_sql(ENGLISH_NAME_SQL, conn)
            chinese_df = pd.read_sql(CHINESE_NAME_SQL, conn)
    return english_df, chinese_df


def apply_ignored_source_ids(matched: pd.DataFrame) -> pd.DataFrame:
    ignored_ids = {normalize_source_id(item) for item in IGNORED_SOURCE_IDS}
    ignored_ids.discard("")
    if not ignored_ids or matched.empty:
        return matched

    keep_mask = pd.Series(True, index=matched.index)
    for column in ("ID_中文", "ID_英文"):
        if column in matched.columns:
            keep_mask &= ~matched[column].map(normalize_source_id).isin(ignored_ids)
    return matched[keep_mask].copy()


def filter_chinese_by_english_stocks(
    chinese_df: pd.DataFrame,
    english_df: pd.DataFrame,
) -> pd.DataFrame:
    if chinese_df.empty or english_df.empty:
        return chinese_df.iloc[0:0].copy()
    english_stocks = set(english_df["股票代码"].dropna())
    return chinese_df[chinese_df["股票代码"].isin(english_stocks)].copy()


def compare_leaders(english_df: pd.DataFrame, chinese_df: pd.DataFrame) -> pd.DataFrame:
    english = add_match_columns(english_df, is_chinese=False)
    chinese_df = filter_chinese_by_english_stocks(chinese_df, english_df)
    if chinese_df.empty:
        return pd.DataFrame(columns=FINAL_OUTPUT_COLUMNS)
    chinese = add_match_columns(chinese_df, is_chinese=True)

    candidates = chinese.merge(
        english,
        on=["股票代码", "姓名_标准化", "性别_标准化"],
        how="inner",
        suffixes=("_中文", "_英文"),
    )
    if candidates.empty:
        return pd.DataFrame(columns=FINAL_OUTPUT_COLUMNS)

    matched = candidates[
        candidates.apply(
            lambda row: birth_dates_match(row["出生日期_标准化_中文"], row["出生日期_标准化_英文"]),
            axis=1,
        )
    ].copy()
    matched = apply_ignored_source_ids(matched)
    if matched.empty:
        return pd.DataFrame(columns=FINAL_OUTPUT_COLUMNS)

    output = pd.DataFrame(
        {
            "股票代码": matched["股票代码"],
            "证券简称": matched["证券简称_中文"],
            "姓名_中文": matched["姓名_中文"],
            "姓名_英文": matched["姓名_英文"],
            "性别": matched["性别_中文"],
            "出生日期_中文": matched["出生日期_标准化_中文"],
            "出生日期_英文": matched["出生日期_标准化_英文"],
        }
    )
    return output.drop_duplicates().sort_values(["股票代码", "姓名_中文", "姓名_英文"])


def build_dingtalk_message(result_df: pd.DataFrame, send_time: str | None = None) -> str:
    if send_time is None:
        send_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    header = "## <font color=#FF7F24>特别组-领导人介绍中英文姓名重复</font>\n"
    sub_header = f"> 执行时间：{send_time}\n\n"
    body = ">  股票代码\t\t姓名_中文\t姓名_英文\n"
    for _, row in result_df.iterrows():
        body += f"+  {row['股票代码']}\t\t{row['姓名_中文']}\t\t{row['姓名_英文']}\n"
    footer = f">  <font color=#FF7F24>异常数据量：**{len(result_df)}**</font>"
    msg_at = f"\n\n{phone_at}"
    message = f"{header}{sub_header}{body}{footer}{msg_at}"
    if DINGTALK_SECURITY_KEYWORD:
        message += f"\n<!-- {DINGTALK_SECURITY_KEYWORD} -->"
    return message


def send_dingtalk_message(message: str, webhook_url: str = DINGTALK_WEBHOOK_URL) -> bool:
    headers = {"Content-Type": "application/json"}
    payload = {
        "msgtype": "markdown",
        "markdown": {
            "title": "特别组-领导人介绍中英文姓名重复",
            "text": message,
        },
        "at": {
            "atMobiles": [phone_at.replace("@", "")],
            "isAtAll": False
        }
    }
    response = requests.post(
        webhook_url,
        headers=headers,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        timeout=15,
    )
    if response.status_code != 200:
        print(f"钉钉消息发送失败，HTTP状态码: {response.status_code}，响应: {response.text}")
        return False
    try:
        result = response.json()
    except ValueError:
        print("钉钉消息发送成功！")
        return True
    if result.get("errcode", 0) != 0:
        print(f"钉钉消息发送失败: {result}")
        return False
    print("钉钉消息发送成功！")
    return True


def send_dingtalk_if_needed(
    result_df: pd.DataFrame,
    send_func: Any = send_dingtalk_message,
) -> bool:
    if result_df.empty:
        print("匹配结果为空，不发送钉钉消息。")
        return False
    message = build_dingtalk_message(result_df)
    return bool(send_func(message))


def write_result_excel(result_df: pd.DataFrame, output_path: Path) -> Path:
    try:
        result_df.to_excel(output_path, index=False)
        return output_path
    except PermissionError:
        fallback_path = output_path.with_name(
            f"{output_path.stem}_{datetime.now():%Y%m%d_%H%M%S}{output_path.suffix}"
        )
        result_df.to_excel(fallback_path, index=False)
        return fallback_path


def write_excel_if_enabled(
    result_df: pd.DataFrame,
    output_path: Path,
    *,
    enabled: bool,
    writer: Any = write_result_excel,
) -> Path | None:
    if not enabled:
        print("默认不输出 Excel 文件；如需输出，请添加 --output-excel。")
        return None
    return writer(result_df, output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="比对公司主要领导人介绍中的中英文姓名记录。")
    parser.add_argument(
        "--conn-str",
        default=os.getenv("SQLSERVER_CONN_STR", DEFAULT_CONN_STR),
        help="SQL Server ODBC 连接串；默认读取 SQLSERVER_CONN_STR，未设置则使用脚本内默认连接。",
    )
    parser.add_argument(
        "--output",
        default=str(OUTPUT_FILE),
        help="输出 Excel 文件路径；需配合 --output-excel 使用。",
    )
    parser.add_argument(
        "--output-excel",
        action="store_true",
        help="显式输出 Excel 文件；默认不输出。",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    english_df, chinese_df = read_sql_data(args.conn_str)
    result_df = compare_leaders(english_df, chinese_df)
    output_path = Path(args.output).resolve()
    actual_output_path = write_excel_if_enabled(result_df, output_path, enabled=args.output_excel)
    print(f"英文姓名记录数: {len(english_df)}")
    print(f"中文姓名记录数: {len(chinese_df)}")
    print(f"匹配结果数: {len(result_df)}")
    if actual_output_path:
        print(f"结果文件: {actual_output_path}")
    if actual_output_path and actual_output_path != output_path:
        print(f"原结果文件可能正被占用，已自动另存为: {actual_output_path}")
    send_dingtalk_if_needed(result_df)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except pyodbc.Error as exc:
        print(f"数据库读取失败: {exc}", file=sys.stderr)
        raise SystemExit(1)
