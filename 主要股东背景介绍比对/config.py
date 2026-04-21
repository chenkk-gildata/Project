"""
配置文件 - 统一管理所有配置项
"""
import os
from dataclasses import dataclass
from typing import Optional


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
    temperature: float = float(os.getenv('AI_TEMPERATURE', '0.3'))
    top_p: float = float(os.getenv('AI_TOP_P', '0.5'))
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
