import unittest
from pathlib import Path
from unittest.mock import Mock

import pandas as pd

import compare_leader_names as leader_module
from compare_leader_names import (
    build_dingtalk_message,
    birth_dates_match,
    chinese_name_match_keys,
    compare_leaders,
    send_dingtalk_if_needed,
    write_excel_if_enabled,
    write_result_excel,
    normalize_birth_date,
    normalize_name,
    chinese_name_to_pinyin,
)


class CompareLeaderNamesTest(unittest.TestCase):
    def test_normalizes_disclosed_birth_date_parts_only(self):
        self.assertEqual(normalize_birth_date("1980年1月1日"), "1980-01-01")
        self.assertEqual(normalize_birth_date("1973年1月出生"), "1973-01")
        self.assertEqual(normalize_birth_date("1965年出生"), "1965")
        self.assertEqual(normalize_birth_date("1976-02-27"), "1976-02-27")

    def test_birth_date_prefix_containment_counts_as_match(self):
        self.assertTrue(birth_dates_match("1996", "1996-07"))
        self.assertTrue(birth_dates_match("1976-02", "1976-02-27"))
        self.assertFalse(birth_dates_match("1966-01", "1965"))

    def test_name_normalization_matches_chinese_pinyin_to_english(self):
        self.assertEqual(chinese_name_to_pinyin("张三"), "zhang san")
        self.assertEqual(normalize_name("ZHANG San"), normalize_name("zhang san"))
        self.assertEqual(normalize_name("Zhang-San"), normalize_name("zhang san"))

    def test_chinese_name_match_keys_include_given_name_first_order(self):
        self.assertEqual(chinese_name_match_keys("张三"), ["zhangsan", "sanzhang"])
        self.assertEqual(chinese_name_match_keys("欧阳娜娜"), ["ouyangnana", "nanaouyang"])

    def test_compare_leaders_outputs_requested_excel_columns_only(self):
        english_df = pd.DataFrame(
            [
                {
                    "股票代码": "000001",
                    "证券简称": "平安银行",
                    "信息发布日期": "2026-01-01",
                    "姓名": "Zhang San",
                    "性别": "男",
                    "出身日期": "1976-02-27",
                    "存在与否": "1",
                    "XGSJ": "2026-01-02",
                    "ID": 2,
                }
            ]
        )
        chinese_df = pd.DataFrame(
            [
                {
                    "股票代码": "000001",
                    "证券简称": "平安银行",
                    "信息发布日期": "2026-01-01",
                    "姓名": "张三",
                    "性别": "男",
                    "出身日期": "1976年2月出生",
                    "存在与否": "1",
                    "XGSJ": "2026-01-02",
                    "ID": 1,
                }
            ]
        )

        result = compare_leaders(english_df, chinese_df)

        self.assertEqual(
            list(result.columns),
            [
                "股票代码",
                "证券简称",
                "姓名_中文",
                "姓名_英文",
                "性别",
                "出生日期_中文",
                "出生日期_英文",
            ],
        )
        self.assertEqual(result.iloc[0].to_dict()["出生日期_中文"], "1976-02")
        self.assertEqual(result.iloc[0].to_dict()["出生日期_英文"], "1976-02-27")

    def test_compare_leaders_matches_english_given_name_first_order(self):
        english_df = pd.DataFrame(
            [
                {
                    "股票代码": "000001",
                    "证券简称": "平安银行",
                    "信息发布日期": "2026-01-01",
                    "姓名": "San Zhang",
                    "性别": "男",
                    "出身日期": "1976-02-27",
                    "存在与否": "1",
                    "XGSJ": "2026-01-02",
                    "ID": 2,
                },
                {
                    "股票代码": "000002",
                    "证券简称": "测试公司",
                    "信息发布日期": "2026-01-01",
                    "姓名": "Nana Ouyang",
                    "性别": "女",
                    "出身日期": "1988-01",
                    "存在与否": "1",
                    "XGSJ": "2026-01-02",
                    "ID": 4,
                },
            ]
        )
        chinese_df = pd.DataFrame(
            [
                {
                    "股票代码": "000001",
                    "证券简称": "平安银行",
                    "信息发布日期": "2026-01-01",
                    "姓名": "张三",
                    "性别": "男",
                    "出身日期": "1976年2月出生",
                    "存在与否": "1",
                    "XGSJ": "2026-01-02",
                    "ID": 1,
                },
                {
                    "股票代码": "000002",
                    "证券简称": "测试公司",
                    "信息发布日期": "2026-01-01",
                    "姓名": "欧阳娜娜",
                    "性别": "女",
                    "出身日期": "1988年1月1日",
                    "存在与否": "1",
                    "XGSJ": "2026-01-02",
                    "ID": 3,
                },
            ]
        )

        result = compare_leaders(english_df, chinese_df)

        self.assertEqual(len(result), 2)
        self.assertEqual(set(result["姓名_英文"]), {"San Zhang", "Nana Ouyang"})

    def test_compare_leaders_keeps_direct_and_reverse_order_matches(self):
        english_df = pd.DataFrame(
            [
                {
                    "股票代码": "000001",
                    "证券简称": "平安银行",
                    "信息发布日期": "2026-01-01",
                    "姓名": "Zhang San",
                    "性别": "男",
                    "出身日期": "1976",
                    "存在与否": "1",
                    "XGSJ": "2026-01-02",
                    "ID": 2,
                },
                {
                    "股票代码": "000001",
                    "证券简称": "平安银行",
                    "信息发布日期": "2026-01-01",
                    "姓名": "San Zhang",
                    "性别": "男",
                    "出身日期": "1976-02",
                    "存在与否": "1",
                    "XGSJ": "2026-01-02",
                    "ID": 3,
                },
            ]
        )
        chinese_df = pd.DataFrame(
            [
                {
                    "股票代码": "000001",
                    "证券简称": "平安银行",
                    "信息发布日期": "2026-01-01",
                    "姓名": "张三",
                    "性别": "男",
                    "出身日期": "1976年2月出生",
                    "存在与否": "1",
                    "XGSJ": "2026-01-02",
                    "ID": 1,
                }
            ]
        )

        result = compare_leaders(english_df, chinese_df)

        self.assertEqual(len(result), 2)
        self.assertEqual(set(result["姓名_英文"]), {"Zhang San", "San Zhang"})

    def test_compare_leaders_filters_ignored_source_ids(self):
        english_df = pd.DataFrame(
            [
                {
                    "股票代码": "000001",
                    "证券简称": "平安银行",
                    "信息发布日期": "2026-01-01",
                    "姓名": "Zhang San",
                    "性别": "男",
                    "出身日期": "1976",
                    "存在与否": "1",
                    "XGSJ": "2026-01-02",
                    "ID": 2,
                },
                {
                    "股票代码": "000002",
                    "证券简称": "测试公司",
                    "信息发布日期": "2026-01-01",
                    "姓名": "Li Si",
                    "性别": "男",
                    "出身日期": "1977",
                    "存在与否": "1",
                    "XGSJ": "2026-01-02",
                    "ID": 4,
                },
            ]
        )
        chinese_df = pd.DataFrame(
            [
                {
                    "股票代码": "000001",
                    "证券简称": "平安银行",
                    "信息发布日期": "2026-01-01",
                    "姓名": "张三",
                    "性别": "男",
                    "出身日期": "1976年出生",
                    "存在与否": "1",
                    "XGSJ": "2026-01-02",
                    "ID": 1,
                },
                {
                    "股票代码": "000002",
                    "证券简称": "测试公司",
                    "信息发布日期": "2026-01-01",
                    "姓名": "李四",
                    "性别": "男",
                    "出身日期": "1977年出生",
                    "存在与否": "1",
                    "XGSJ": "2026-01-02",
                    "ID": 3,
                },
            ]
        )

        original_ignored_ids = leader_module.IGNORED_SOURCE_IDS
        try:
            leader_module.IGNORED_SOURCE_IDS = {2}
            result = compare_leaders(english_df, chinese_df)
        finally:
            leader_module.IGNORED_SOURCE_IDS = original_ignored_ids

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["股票代码"], "000002")

    def test_compare_leaders_filters_chinese_rows_before_pinyin_conversion(self):
        english_df = pd.DataFrame(
            [
                {
                    "股票代码": "000001",
                    "证券简称": "平安银行",
                    "信息发布日期": "2026-01-01",
                    "姓名": "Zhang San",
                    "性别": "男",
                    "出身日期": "1976",
                    "存在与否": "1",
                    "XGSJ": "2026-01-02",
                    "ID": 2,
                }
            ]
        )
        chinese_df = pd.DataFrame(
            [
                {
                    "股票代码": "000001",
                    "证券简称": "平安银行",
                    "信息发布日期": "2026-01-01",
                    "姓名": "张三",
                    "性别": "男",
                    "出身日期": "1976年出生",
                    "存在与否": "1",
                    "XGSJ": "2026-01-02",
                    "ID": 1,
                },
                {
                    "股票代码": "999999",
                    "证券简称": "无关公司",
                    "信息发布日期": "2026-01-01",
                    "姓名": "王五",
                    "性别": "男",
                    "出身日期": "1978年出生",
                    "存在与否": "1",
                    "XGSJ": "2026-01-02",
                    "ID": 3,
                },
            ]
        )
        original_match_keys = leader_module.chinese_name_match_keys
        converted_names = []

        def record_match_keys(name):
            converted_names.append(name)
            return original_match_keys(name)

        try:
            leader_module.chinese_name_match_keys = record_match_keys
            result = compare_leaders(english_df, chinese_df)
        finally:
            leader_module.chinese_name_match_keys = original_match_keys

        self.assertEqual(len(result), 1)
        self.assertEqual(converted_names, ["张三"])

    def test_build_dingtalk_message_uses_requested_markdown_format(self):
        result_df = pd.DataFrame(
            [
                {"股票代码": "000001", "姓名_中文": "张三", "姓名_英文": "Zhang San"},
                {"股票代码": "000002", "姓名_中文": "李四", "姓名_英文": "Li Si"},
            ]
        )

        message = build_dingtalk_message(result_df, "2026-06-16 09:30:00")

        self.assertIn("## <font color=#FF7F24>特别组-领导人介绍中英文姓名重复</font>", message)
        self.assertIn("> 执行时间：2026-06-16 09:30:00", message)
        self.assertIn(">  股票代码\t\t姓名_中文\t姓名_英文", message)
        self.assertIn("+  000001\t\t张三\t\tZhang San", message)
        self.assertIn("+  000002\t\t李四\t\tLi Si", message)
        self.assertIn(">  <font color=#FF7F24>异常数据量：**2**</font>", message)

    def test_send_dingtalk_if_needed_skips_empty_result(self):
        sender = Mock()

        sent = send_dingtalk_if_needed(pd.DataFrame(), send_func=sender)

        self.assertFalse(sent)
        sender.assert_not_called()

    def test_write_excel_if_enabled_skips_excel_by_default(self):
        writer = Mock()

        actual_path = write_excel_if_enabled(
            pd.DataFrame(),
            Path("result.xlsx"),
            enabled=False,
            writer=writer,
        )

        self.assertIsNone(actual_path)
        writer.assert_not_called()

    def test_write_excel_if_enabled_writes_when_enabled(self):
        writer = Mock(return_value=Path("result.xlsx"))

        actual_path = write_excel_if_enabled(
            pd.DataFrame(),
            Path("result.xlsx"),
            enabled=True,
            writer=writer,
        )

        self.assertEqual(actual_path, Path("result.xlsx"))
        writer.assert_called_once()

    def test_write_result_excel_falls_back_when_target_is_locked(self):
        df = Mock()
        df.to_excel.side_effect = [PermissionError("locked"), None]
        output_path = Path("result.xlsx")

        actual_path = write_result_excel(df, output_path)

        self.assertNotEqual(actual_path, output_path)
        self.assertEqual(actual_path.parent, output_path.parent)
        self.assertTrue(actual_path.name.startswith("result_"))
        self.assertEqual(actual_path.suffix, ".xlsx")
        self.assertEqual(df.to_excel.call_count, 2)


if __name__ == "__main__":
    unittest.main()
