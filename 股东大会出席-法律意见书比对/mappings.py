# 股东大会出席-法律意见书
# 字段映射配置

FIELD_MAPPING = {
    "律师事务所": "LSSWS",
    "经办律师": "JBLS"
}

ATTEND_TYPE_MAPPING = {
    1: "总体出席",
    2: "现场出席",
    3: "网络出席"
}

ATTEND_FIELD_MAPPING = {
    1: {
        "CXGD": "出席股东总人数",
        "ZTAGGDRS": "出席A股股东总人数",
        "ZTHGGDRS": "出席H股股东总人数",
        "ZTQTGDRS": "出席其他股股东总人数",
        "DBGF": "出席股东总股数",
        "ZTAGGDDBGF": "出席A股股东总股数",
        "ZTHGGDDBGF": "出席H股股东总股数",
        "ZTQTGDDBGF": "出席其他股股东总股数",
        "ZB": "出席总股数占比",
        "ZTAGGDZB": "出席A股总股数占比",
        "ZTHGGDZB": "出席H股总股数占比",
        "ZTQTGDZB": "出席其他股总股数占比",
        "ZXGDCXRS": "中小股东总体出席人数",
        "ZXGDDBGF": "中小股东总体出席股数",
        "ZXGDZB": "中小股东总体出席股数占比"
    },
    2: {
        "CXGD": "出席现场人数",
        "ZTAGGDRS": "出席现场A股股东人数",
        "ZTHGGDRS": "出席现场H股股东人数",
        "ZTQTGDRS": "出席现场其他股股东人数",
        "DBGF": "出席现场股数",
        "ZTAGGDDBGF": "出席现场A股股东股数",
        "ZTHGGDDBGF": "出席现场H股股东股数",
        "ZTQTGDDBGF": "出席现场其他股股东股数",
        "ZB": "出席现场股数占比",
        "ZTAGGDZB": "出席现场A股股东股数占比",
        "ZTHGGDZB": "出席现场H股股东股数占比",
        "ZTQTGDZB": "出席现场其他股股东股数占比",
        "ZXGDCXRS": "中小股东现场出席人数",
        "ZXGDDBGF": "中小股东现场出席股数",
        "ZXGDZB": "中小股东现场出席股数占比"
    },
    3: {
        "CXGD": "出席网络人数",
        "ZTAGGDRS": "出席网络A股股东人数",
        "ZTHGGDRS": "出席网络H股股东人数",
        "ZTQTGDRS": "出席网络其他股股东人数",
        "DBGF": "出席网络股数",
        "ZTAGGDDBGF": "出席网络A股股东股数",
        "ZTHGGDDBGF": "出席网络H股股东股数",
        "ZTQTGDDBGF": "出席网络其他股股东股数",
        "ZB": "出席网络股数占比",
        "ZTAGGDZB": "出席网络A股股东股数占比",
        "ZTHGGDZB": "出席网络H股股东股数占比",
        "ZTQTGDZB": "出席网络其他股股东股数占比",
        "ZXGDCXRS": "中小股东网络出席人数",
        "ZXGDDBGF": "中小股东网络出席股数",
        "ZXGDZB": "中小股东网络出席股数占比"
    }
}
