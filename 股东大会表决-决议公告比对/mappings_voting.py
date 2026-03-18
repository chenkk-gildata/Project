# 股东大会表决-决议公告
# 字段映射配置

BASIC_MAPPING = {
    "股票代码": "GPDM",
    "信息发布日期": "XXFBRQ",
    "股东大会类别": "GDDHLB",
    "届次": "JC",
    "大议案序号": "DBTXH",
    "小议案序号": "XBTXH",
    "主持人": "ZCR",
    "主持人职位": "ZW",
    "见证律师事务所": "LSSWS",
    "经办律师": "JBLS"
}

PROPOSAL_VOTING_MAPPING = {
    "是否通过": "SFTG",
    "A股同意股数": "QBAGTYGS",
    "A股反对股数": "QBAGFDGS",
    "A股弃权股数": "QBAGQQGS",
    "H股同意股数": "QBHGTYGS",
    "H股反对股数": "QBHGFDGS",
    "H股弃权股数": "QBHGQQGS",
    "其他股同意股数": "QBQTGDTYGS",
    "其他股反对股数": "QBQTGDFDGS",
    "其他股弃权股数": "QBQTGDQQGS",
    "中小股同意股数": "ZXGDTYGS",
    "中小股反对股数": "ZXGDFDGS",
    "中小股弃权股数": "ZXGDQQGS",
    "优先股同意股数": "YXGGDTYGS",
    "优先股反对股数": "YXGGDFDGS",
    "优先股弃权股数": "YXGGDQQGS",
    "同意股数": "QBTYGS",
    "反对股数": "QBFDGS",
    "弃权股数": "QBQQGS"
}
