"""
配置文件 - 统一管理所有配置项
"""
import os
from dataclasses import dataclass
from typing import Optional, List


@dataclass
class PerformanceObject:
    """业绩对象配置"""
    code: str  # 常量代码
    type: str  # 业绩类型：income(收入), profit(利润), eps(每股收益), other(其他)
    include_pattern: str  # 包含匹配的正则表达式
    exclude_keywords: List[str]  # 必须排除的关键词
    description: str  # 业绩指标描述
    priority: int = 999  # 提取优先级，数字越小优先级越高，默认999表示最低优先级
    is_required: bool = False  # 是否必做，True表示必做，False表示非必做


@dataclass
class DatabaseConfig:
    """数据库配置"""
    server: str = os.getenv('DB_SERVER', '10.102.25.11,8080')
    username: str = os.getenv('DB_USERNAME', 'WebResourceNew_Read')
    password: str = os.getenv('DB_PASSWORD', 'New_45ted')
    driver: str = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
    connection_timeout: int = int(os.getenv('DB_CONNECTION_TIMEOUT', '30'))
    command_timeout: int = int(os.getenv('DB_COMMAND_TIMEOUT', '300'))


@dataclass
class AIConfig:
    """AI服务配置"""
    api_key: str = os.getenv('AI_API_KEY', 'sk-c88c51dd13074e6ebc14bf8339568c3f')
    base_url: str = os.getenv('AI_BASE_URL', 'https://dashscope.aliyuncs.com/compatible-mode/v1')
    model: str = os.getenv('AI_MODEL', 'qwen-long')
    temperature: float = float(os.getenv('AI_TEMPERATURE', '0.0'))
    top_p: float = float(os.getenv('AI_TOP_P', '0.1'))
    max_retries: int = int(os.getenv('AI_MAX_RETRIES', '3'))
    timeout: int = int(os.getenv('AI_TIMEOUT', '300'))  # AI处理超时时间，单位：秒
    file_upload_timeout: int = int(os.getenv('AI_FILE_UPLOAD_TIMEOUT', '10'))  # 文件上传超时时间，单位：秒


@dataclass
class ProcessingConfig:
    """处理配置"""
    default_workers: int = int(os.getenv('DEFAULT_WORKERS', '12'))
    upload_workers: int = int(os.getenv('UPLOAD_WORKERS', '3'))
    upload_semaphore_limit: int = int(os.getenv('UPLOAD_SEMAPHORE_LIMIT', '3'))
    upload_timeout: int = int(os.getenv('UPLOAD_TIMEOUT', '60'))
    process_timeout: int = int(os.getenv('PROCESS_TIMEOUT', '300'))
    overall_timeout: int = int(os.getenv('OVERALL_TIMEOUT', '3600'))
    retry_attempts: int = int(os.getenv('RETRY_ATTEMPTS', '3'))
    retry_wait_min: int = int(os.getenv('RETRY_WAIT_MIN', '2'))
    retry_wait_max: int = int(os.getenv('RETRY_WAIT_MAX', '10'))


@dataclass
class LoggingConfig:
    """日志配置"""
    level: str = os.getenv('LOG_LEVEL', 'INFO')
    format: str = os.getenv('LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_path: Optional[str] = os.getenv('LOG_FILE_PATH')


# 全局配置实例
db_config = DatabaseConfig()
ai_config = AIConfig()
processing_config = ProcessingConfig()
logging_config = LoggingConfig()


def validate_config():
    """验证配置的有效性"""
    errors = []
    
    if not ai_config.api_key:
        errors.append("AI_API_KEY 环境变量未设置")
    
    if processing_config.default_workers <= 0:
        errors.append("DEFAULT_WORKERS 必须大于0")
    
    if processing_config.upload_workers <= 0:
        errors.append("UPLOAD_WORKERS 必须大于0")
    
    if errors:
        raise ValueError(f"配置验证失败: {'; '.join(errors)}")
    
    return True


# 业绩对象匹配配置
performance_objects = [
    # 收入相关
    PerformanceObject(
        code="CIS000000002",
        type="income",
        include_pattern=r"营.*收|收入",
        exclude_keywords=["扣", "总", "益", "主"],
        description="营业收入",
        is_required=True
    ),
    PerformanceObject(
        code="CFI0000000KL",
        type="income",
        include_pattern=r"收.*扣|扣.*收",
        exclude_keywords=["前", "每股"],
        description="扣除后营业收入",
        is_required=True
    ),
    PerformanceObject(
        code="CIS000000001",
        type="income",
        include_pattern=r"总.*收",
        exclude_keywords=["扣"],
        description="营业总收入",
        is_required=True
    ),
    PerformanceObject(
        code="CIS00000003T",
        type="income",
        include_pattern=r"主.*收",
        exclude_keywords=["扣"],
        description="主营业务收入",
        priority=1,
        is_required=False
    ),
    # 利润相关
    PerformanceObject(
        code="CIS00000001X",
        type="profit",
        include_pattern=r"\b(?:净利润|利润|税后净利润)\b",
        exclude_keywords=[],
        description="净利润",
        is_required=True
    ),
    PerformanceObject(
        code="CIS000000020",
        type="profit",
        include_pattern=r"归.*净(?:利润|亏损)",
        exclude_keywords=["普通", "扣"],
        description="归母净利润",
        is_required=True
    ),
    PerformanceObject(
        code="CFI000000036",
        type="profit",
        include_pattern=r"扣.*净(?:利润|亏损)",
        exclude_keywords=["普通"],
        description="扣非后净利润",
        is_required=True
    ),
    PerformanceObject(
        code="CFI0000000H7",
        type="profit",
        include_pattern=r"普通股.*利润",
        exclude_keywords=["扣"],
        description="归母普通股净利润",
        is_required=True
    ),
    PerformanceObject(
        code="CFI0000000H8",
        type="profit",
        include_pattern=r"扣.*普通股.*利润",
        exclude_keywords=[],
        description="扣非后普通股净利润",
        is_required=True
    ),
    PerformanceObject(
        code="CIS00000001S",
        type="profit",
        include_pattern=r"利润总额",
        exclude_keywords=[],
        description="利润总额",
        priority=1,
        is_required=False
    ),
    PerformanceObject(
        code="CIS00000001M",
        type="profit",
        include_pattern=r"营业利润|经营利润",
        exclude_keywords=[],
        description="营业利润",
        priority=2,
        is_required=False
    ),
    PerformanceObject(
        code="CIS00000001M",
        type="profit",
        include_pattern=r"毛利",
        exclude_keywords=[],
        description="毛利",
        priority=3,
        is_required=False
    ),
    PerformanceObject(
        code="CIS000000069",
        type="profit",
        include_pattern=r"主.*营.*(?:利润|收益)",
        exclude_keywords=[],
        description="主营业务利润",
        priority=4,
        is_required=False
    ),
    # 每股收益相关
    PerformanceObject(
        code="CIS00000003G",
        type="eps",
        include_pattern=r".*每股.*",
        exclude_keywords=["扣", "稀"],
        description="基本每股收益",
        is_required=True
    ),
    PerformanceObject(
        code="CIS00000002U",
        type="eps",
        include_pattern=r".*稀释每股.*",
        exclude_keywords=["扣"],
        description="稀释每股收益",
        is_required=True
    ),
    PerformanceObject(
        code="CFI0000000K1",
        type="eps",
        include_pattern=r"扣.*每股|每股.*扣",
        exclude_keywords=["稀"],
        description="每股收益(扣除)",
        is_required=True
    ),
    PerformanceObject(
        code="CFI00000002W",
        type="eps",
        include_pattern=r"扣.*稀释每股|稀释每股.*扣",
        exclude_keywords=[],
        description="稀释每股收益(扣除)",
        is_required=True
    ),
    # 其他类
    PerformanceObject(
        code="CFI000000031",
        type="other",
        include_pattern=r"非.*(?:损益|收益)",
        exclude_keywords=["收入", "利润", "每股"],
        description="非经常性损益",
        is_required=True
    ),
    PerformanceObject(
        code="CBS00000004W",
        type="other",
        include_pattern=r"归?.*(?:所有者权益|净资产)",
        exclude_keywords=[],
        description="归属母公司所有者权益",
        is_required=True
    ),
]
