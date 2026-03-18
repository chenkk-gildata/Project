"""
数据模型 - 年报公告自动处理系统
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, List
from enum import Enum


class DownloadStatus(Enum):
    """下载状态枚举"""
    PENDING = "pending"      # 等待下载
    DOWNLOADING = "downloading"  # 下载中
    SUCCESS = "success"      # 下载成功
    FAILED = "failed"        # 下载失败
    RETRYING = "retrying"    # 重试中


class ProcessStatus(Enum):
    """处理状态枚举"""
    PENDING = "pending"      # 等待处理
    PROCESSING = "processing"  # 处理中
    SUCCESS = "success"      # 处理成功
    FAILED = "failed"        # 处理失败
    SKIPPED = "skipped"      # 跳过(已处理过)
    NO_OUTPUT = "no_output"  # 正常执行但无输出


@dataclass
class Announcement:
    """公告数据模型"""
    hashcode: str                    # 主键: MD5/Hashcode
    gpdm: Optional[str] = None      # 股票代码
    zqjc: Optional[str] = None      # 证券简称
    publish_date: Optional[str] = None  # 发布日期
    title: Optional[str] = None     # 公告标题
    fbsj: Optional[datetime] = None # 信息时间(用于增量查询)
    
    # 下载相关
    download_status: DownloadStatus = field(default=DownloadStatus.PENDING)
    download_time: Optional[datetime] = None
    download_retry_count: int = 0
    download_error: Optional[str] = None
    file_path: Optional[str] = None  # 下载后的文件路径
    
    # 处理相关
    process_status: ProcessStatus = field(default=ProcessStatus.PENDING)
    process_time: Optional[datetime] = None
    process_retry_count: int = 0
    process_error: Optional[str] = None
    
    # 各模块处理状态
    module_status: Dict[str, ProcessStatus] = field(default_factory=dict)
    module_retry_count: Dict[str, int] = field(default_factory=dict)
    module_error: Dict[str, str] = field(default_factory=dict)
    
    def __post_init__(self):
        """初始化后处理"""
        if isinstance(self.download_status, str):
            self.download_status = DownloadStatus(self.download_status)
        if isinstance(self.process_status, str):
            self.process_status = ProcessStatus(self.process_status)
    
    def to_db_dict(self) -> dict:
        """转换为数据库存储格式"""
        return {
            "hashcode": self.hashcode,
            "gpdm": self.gpdm,
            "zqjc": self.zqjc,
            "publish_date": self.publish_date,
            "title": self.title,
            "fbsj": self.fbsj.isoformat() if self.fbsj else None,
            "download_status": self.download_status.value,
            "download_time": self.download_time.isoformat() if self.download_time else None,
            "download_retry_count": self.download_retry_count,
            "download_error": self.download_error,
            "file_path": self.file_path,
            "process_status": self.process_status.value,
            "process_time": self.process_time.isoformat() if self.process_time else None,
            "process_retry_count": self.process_retry_count,
            "process_error": self.process_error
        }
    
    @classmethod
    def from_db_dict(cls, data: dict) -> "Announcement":
        """从数据库记录创建实例"""
        return cls(
            hashcode=data["hashcode"],
            gpdm=data.get("gpdm"),
            zqjc=data.get("zqjc"),
            publish_date=data.get("publish_date"),
            title=data.get("title"),
            fbsj=datetime.fromisoformat(data["fbsj"]) if data.get("fbsj") else None,
            download_status=DownloadStatus(data.get("download_status", "pending")),
            download_time=datetime.fromisoformat(data["download_time"]) if data.get("download_time") else None,
            download_retry_count=data.get("download_retry_count", 0),
            download_error=data.get("download_error"),
            file_path=data.get("file_path"),
            process_status=ProcessStatus(data.get("process_status", "pending")),
            process_time=datetime.fromisoformat(data["process_time"]) if data.get("process_time") else None,
            process_retry_count=data.get("process_retry_count", 0),
            process_error=data.get("process_error")
        )


@dataclass
class ProcessTask:
    """处理任务数据模型"""
    hashcode: str
    file_path: str
    module_name: str
    retry_count: int = 0
    created_at: datetime = field(default_factory=datetime.now)
    
    def __hash__(self):
        return hash(f"{self.hashcode}_{self.module_name}")
    
    def __eq__(self, other):
        if isinstance(other, ProcessTask):
            return self.hashcode == other.hashcode and self.module_name == other.module_name
        return False


@dataclass
class DownloadTask:
    """下载任务数据模型"""
    announcement: Announcement
    retry_count: int = 0
    created_at: datetime = field(default_factory=datetime.now)
