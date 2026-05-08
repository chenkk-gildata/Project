"""
根据提示词规则，识别并修正比对结果中的明显AI错误
- 如果AI明显错误（误提），则置空该条比对结果
- 如果正式库与AI的差异无法判断，保留原样
"""
import pandas as pd
import re

def should_clear_comparison(result_str):
    """
    根据提示词规则判断比对结果是否应该被置空
    返回 True 表示AI明显错误，应该置空
    返回 False 表示应该保留原样
    """
    if pd.isna(result_str) or result_str == '数据一致':
        return False

    # 按换行符分割多个字段的问题
    lines = result_str.split('\n')

    for line in lines:
        # 匹配格式：字段名【正式库：XXX；AI：YYY】
        match = re.match(r'([^【]+)【正式库：([^；]+)；AI：([^】]+)】', line)
        if not match:
            continue

        field_name = match.group(1)
        db_value = match.group(2)
        ai_value = match.group(3)

        # 判断是否为空
        is_db_empty = db_value in ['', '空', '-', '无', 'None'] or pd.isna(db_value)
        is_ai_empty = ai_value in ['', '空', '-', '无', 'None'] or pd.isna(ai_value)

        # ========== 补贴津贴判断（强触发字段）==========
        if '补贴津贴' in field_name:
            # 如果正式库为空，但AI输出了具体金额，很可能是误提
            if is_db_empty and not is_ai_empty:
                return True
            continue

        # ========== 间接持股数判断 ==========
        if '间接持股数' in field_name:
            # 如果正式库为空，但AI输出了具体金额，很可能是误提
            if is_db_empty and not is_ai_empty:
                return True
            continue

        # ========== 期初/期末持股数判断 ==========
        if '期初持股数' in field_name or '期末持股数' in field_name:
            # 正式库为0但AI为空 - 根据提示词，AI应该输出0，这是明显错误
            if not is_db_empty and is_ai_empty:
                if db_value == '0':
                    return True

            # 正式库为空但AI为0 - 很可能是误将空识别为0
            if is_db_empty and ai_value == '0':
                return True

            # 数值差异超过100倍，可能是单位问题（万元vs元）
            try:
                db_num = float(db_value) if not is_db_empty else 0
                ai_num = float(ai_value) if not is_ai_empty else 0
                if db_num > 0 and ai_num > 0:
                    ratio = max(db_num / ai_num, ai_num / db_num)
                    if ratio > 100:
                        return True
            except (ValueError, ZeroDivisionError):
                pass
            continue

        # ========== 从公司获得年度报酬总额判断 ==========
        if '从公司获得年度报酬总额' in field_name:
            # 正式库为空但AI输出了具体金额，很可能是误提
            if is_db_empty and not is_ai_empty:
                return True

            # 数值差异超过100倍，可能是单位问题
            try:
                db_num = float(db_value) if not is_db_empty else 0
                ai_num = float(ai_value) if not is_ai_empty else 0
                if db_num > 0 and ai_num > 0:
                    ratio = max(db_num / ai_num, ai_num / db_num)
                    if ratio > 100:
                        return True
            except (ValueError, ZeroDivisionError):
                pass
            continue

        # ========== 是否在股东或关联单位领取报酬津贴判断 ==========
        if '是否在股东或关联单位领取报酬津贴' in field_name:
            # 如果正式库为空但AI输出"否"，很可能是误提
            if is_db_empty and ai_value == '否':
                return True
            continue

        # ========== 变动原因说明判断 ==========
        if '变动原因说明' in field_name:
            # 如果正式库为空但AI输出"0"或"0.00"，很可能是误提
            if is_db_empty and ai_value in ['0', '0.00', '0.0000']:
                return True
            continue

    return False

def main():
    # 读取比对结果
    input_file = r'e:\Project\领导人持股报酬比对\report\领导人持股报酬比对报告_20260423_231253.xlsx'
    output_file = r'e:\Project\领导人持股报酬比对\report\领导人持股报酬比对报告_20260423_231253_修正版.xlsx'

    df = pd.read_excel(input_file, sheet_name='比对结果')
    original_count = len(df)

    print(f"读取数据: {original_count} 条")

    # 统计修正前的状态
    issues_before = df[df['比对结果'].notna() & (df['比对结果'] != '数据一致')]
    print(f"修正前问题记录: {len(issues_before)} 条")

    # 创建修正列
    cleared_count = 0
    new_results = []

    for idx, row in df.iterrows():
        result = row['比对结果']

        if should_clear_comparison(result):
            new_results.append('')  # 置空
            cleared_count += 1
        else:
            new_results.append(result)

    df['比对结果'] = new_results

    # 统计修正后的状态
    issues_after = df[df['比对结果'].notna() & (df['比对结果'] != '数据一致')]
    print(f"修正后问题记录: {len(issues_after)} 条")
    print(f"置空错误记录: {cleared_count} 条")

    # 保存修正后的文件
    df.to_excel(output_file, sheet_name='比对结果', index=False)
    print(f"已保存到: {output_file}")

    # 显示被置空的记录样例
    print("\n=== 被置空的记录样例 ===")
    cleared_df = df[df['比对结果'] == ''].head(20)
    for idx, row in cleared_df.iterrows():
        print(f"公告: {row['公告标题']}, 领导人: {row['领导人姓名']}")

if __name__ == '__main__':
    main()