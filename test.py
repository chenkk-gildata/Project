def main(params: Dict) -> Dict:
    import re
    import json
    from datetime import datetime, timedelta

    def parse_range(val):
        """若平台传来字符串则解析为数组"""
        if val is None:
            return None
        if isinstance(val, str):
            try:
                return json.loads(val)
            except:
                return None
        return val

    def safe_get(arr, i):
        """安全取数组第i个元素，越界或非数组返回None"""
        if arr is None:
            return None
        if not isinstance(arr, list):
            return arr
        return arr[i] if i < len(arr) else None

    # 取入参（平台传来的是各字段的数组，每个index对应一个科目）
    subject_name_arr      = params.get('subject_name') or []
    period_start_arr      = params.get('period_start') or []
    period_end_arr        = params.get('period_end') or []
    amount_prior_year_arr = params.get('amount_prior_year') or []
    amount_range_arr      = params.get('amount_range') or []
    net_change_range_arr  = params.get('net_change_range') or []
    pct_change_range_arr  = params.get('pct_change_range') or []

    # 科目总数以 subject_name 为准
    n = len(subject_name_arr) if isinstance(subject_name_arr, list) else 1

    # 单位常量映射
    UNIT_MAP = {
        "元":    ("FCC00000006T", 1),
        "千元":  ("FCC00000006U", 1000),
        "万元":  ("FCC00000006V", 10000),
        "百万元": ("FCC00000006W", 1000000),
        "千万元": ("FCC00000006X", 10000000),
        "亿元":  ("FCC00000006Y", 100000000),
    }

    # 按字符串长度倒序排列，避免"元"截胡"万元"等
    UNIT_KEYS_SORTED = sorted(UNIT_MAP.keys(), key=len, reverse=True)

    def extract_unit(value_str):
        if value_str is None:
            return None
        if isinstance(value_str, list):
            value_str = next((v for v in value_str if v is not None), None)
        if value_str is None:
            return None
        for unit in UNIT_KEYS_SORTED:
            if unit in str(value_str):
                return unit
        return None

    def extract_number(value_str):
        if value_str is None:
            return None
        if isinstance(value_str, list):
            value_str = next((v for v in value_str if v is not None), None)
        if value_str is None:
            return None
        number = re.sub(r'[^\d\.\+\-]', '', str(value_str).replace(',', ''))
        return number if number else None

    def to_float(value_str):
        if value_str is None:
            return None
        n = extract_number(value_str)
        if n is None:
            return None
        try:
            return float(n)
        except:
            return None

    def convert_value(value_str, from_unit, to_unit):
        if from_unit is None or to_unit is None:
            return None
        number_str = extract_number(value_str)
        if number_str is None:
            return None
        sign = -1 if number_str.startswith('-') else 1
        number_clean = float(number_str.lstrip('+-'))
        converted = number_clean * UNIT_MAP[from_unit][1] / UNIT_MAP[to_unit][1] * sign
        result = f"{converted:.6f}".rstrip('0').rstrip('.')
        if sign > 0:
            result = '+' + result
        return result

    def process_range(range_val, from_unit, to_unit):
        """换算 range 并拆出 start/end，保证 start <= end"""
        if range_val is None:
            return None, None
        range_val = parse_range(range_val)
        if not isinstance(range_val, list):
            return None, None
        start_raw = range_val[0] if len(range_val) > 0 else None
        end_raw   = range_val[1] if len(range_val) > 1 else None
        start = convert_value(start_raw, from_unit, to_unit) if start_raw is not None else None
        end   = convert_value(end_raw,   from_unit, to_unit) if end_raw   is not None else None
        if start is not None and end is not None:
            s_num = to_float(start)
            e_num = to_float(end)
            if s_num is not None and e_num is not None and s_num > e_num:
                start, end = end, start
        return start, end

    def validate_period(period_start, period_end):
        """校验报告期差值是否为3/6/9/12个月
        period_end 加一天后再算月份差，避免01-01到12-31算出11个月的问题"""
        try:
            ps = datetime.strptime(period_start, "%Y-%m-%d")
            pe = datetime.strptime(period_end,   "%Y-%m-%d")
            pe_next = pe + timedelta(days=1)
            months = (pe_next.year - ps.year) * 12 + (pe_next.month - ps.month)
            if months in [3, 6, 9, 12]:
                return period_start, period_end
        except:
            pass
        return None, None

    # 遍历每个科目，逐条处理
    result = []
    for i in range(n):
        subject_name      = safe_get(subject_name_arr, i)
        period_start      = safe_get(period_start_arr, i)
        period_end        = safe_get(period_end_arr, i)
        amount_prior_year = safe_get(amount_prior_year_arr, i)
        amount_range      = parse_range(safe_get(amount_range_arr, i))
        net_change_range  = parse_range(safe_get(net_change_range_arr, i))
        pct_change_range  = parse_range(safe_get(pct_change_range_arr, i))

        # 报告期校验
        period_start_out, period_end_out = validate_period(period_start, period_end)

        # 收集单位，确定最小单位
        units_found = []
        for range_val in [amount_range, net_change_range]:
            if range_val is None:
                continue
            for v in range_val:
                u = extract_unit(v)
                if u:
                    units_found.append(u)
        u = extract_unit(amount_prior_year)
        if u:
            units_found.append(u)

        # 换算各金额字段
        unit                  = None
        amount_start          = None
        amount_end            = None
        net_change_start      = None
        net_change_end        = None
        amount_prior_year_out = amount_prior_year

        if units_found:
            min_unit = min(units_found, key=lambda u: UNIT_MAP[u][1])
            unit     = UNIT_MAP[min_unit][0]

            if amount_range:
                a_from = extract_unit(amount_range[0] or amount_range[1])
                amount_start, amount_end = process_range(amount_range, a_from, min_unit)

            if net_change_range:
                n_from = extract_unit(net_change_range[0] or net_change_range[1])
                net_change_start, net_change_end = process_range(net_change_range, n_from, min_unit)

            if amount_prior_year:
                apy_unit = extract_unit(amount_prior_year)
                amount_prior_year_out = convert_value(amount_prior_year, apy_unit, min_unit)
        else:
            # 无金额单位时，默认万元
            unit = "FCC00000006V"

        # pct_change_range 拆出 start/end
        pct_change_start = None
        pct_change_end   = None
        if pct_change_range:
            s = pct_change_range[0] if len(pct_change_range) > 0 else None
            e = pct_change_range[1] if len(pct_change_range) > 1 else None
            if s is not None and e is not None:
                s_num = to_float(s)
                e_num = to_float(e)
                if s_num is not None and e_num is not None and s_num > e_num:
                    s, e = e, s
            pct_change_start = s
            pct_change_end   = e

        # 校验：本期数值字段至少有一个不为null，否则不产出该条记录
        has_value = any([
            amount_start, amount_end,
            net_change_start, net_change_end,
            pct_change_start, pct_change_end
        ])
        if not has_value:
            continue

        result.append({
            "subject_name":       subject_name,
            "period_start":       period_start_out,
            "period_end":         period_end_out,
            "amount_range":       amount_range,
            "net_change_range":   net_change_range,
            "pct_change_range":   pct_change_range,
            "amount_prior_year":  amount_prior_year_out,
            "unit":               unit,
            "amount_start":       amount_start,
            "amount_end":         amount_end,
            "net_change_start":   net_change_start,
            "net_change_end":     net_change_end,
            "pct_change_start":   pct_change_start,
            "pct_change_end":     pct_change_end,
        })

    return {"subjects": result}
