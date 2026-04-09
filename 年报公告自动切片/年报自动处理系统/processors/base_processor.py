"""
处理器基类 - 年报公告自动处理系统
"""
import os
import fitz
from abc import ABC, abstractmethod
from typing import Optional, Dict, Tuple

from models import ProcessTask, ProcessStatus
from database import db
from logger import logger
from config import get_module_output_dir


class BaseProcessor(ABC):
    """业务处理器基类"""
    
    MODULE_NAME: str = ""
    SUB_MODULES: list = []
    
    def __init__(self):
        if not self.MODULE_NAME:
            raise ValueError("子类必须定义 MODULE_NAME")
        self._current_hashcode = None
        self._current_retry_count = 0
    
    @property
    def output_dir(self) -> str:
        """动态获取输出目录"""
        custom_dir = db.get_system_status("custom_output_dir")
        if custom_dir and custom_dir.strip():
            return get_module_output_dir(self.MODULE_NAME, custom_dir)
        return get_module_output_dir(self.MODULE_NAME)
    
    @abstractmethod
    def find_keywords(self, pdf_path: str) -> Dict:
        """
        查找PDF中的开始和结束关键词
        
        Returns:
            Dict: 包含start, end等关键词信息的字典
        """
        pass
    
    @abstractmethod
    def process_pdf(self, pdf_path: str, keywords: Dict) -> Optional[str]:
        """
        处理PDF文件
        
        Args:
            pdf_path: PDF文件路径
            keywords: 关键词信息
            
        Returns:
            Optional[str]: 输出文件路径,处理失败返回None
        """
        pass
    
    def check_already_processed(self, hashcode: str) -> bool:
        """检查文件是否已处理过"""
        status = db.get_module_status(hashcode, self.MODULE_NAME)
        return status == ProcessStatus.SUCCESS
    
    def get_output_path(self, pdf_path: str) -> str:
        """获取输出文件路径"""
        os.makedirs(self.output_dir, exist_ok=True)
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        return os.path.join(self.output_dir, f"{base_name}.pdf")
    
    def update_sub_module_status(self, sub_module_name: str, status: ProcessStatus, error: str = None):
        """更新子模块状态（同步写入，确保数据一致性）"""
        if not self._current_hashcode:
            return
        success = db.update_module_status(
            self._current_hashcode,
            sub_module_name,
            status,
            error,
            self._current_retry_count,
            sync=True
        )
        if not success:
            logger.error(f"更新子模块状态失败: {sub_module_name} - {status.value}")
    
    def execute(self, task: ProcessTask) -> Tuple[bool, str, ProcessStatus]:
        """
        执行处理任务
        
        Returns:
            Tuple[bool, str, ProcessStatus]: (是否成功, 消息, 最终状态)
        """
        hashcode = task.hashcode
        pdf_path = task.file_path
        
        self._current_hashcode = hashcode
        self._current_retry_count = task.retry_count
        
        if self.check_already_processed(hashcode):
            logger.debug(f"{self.MODULE_NAME}: {hashcode} 已处理过,跳过")
            db.update_module_status(
                hashcode, self.MODULE_NAME, 
                ProcessStatus.SKIPPED
            )
            return True, "已处理过,跳过", ProcessStatus.SKIPPED
        
        if not os.path.exists(pdf_path):
            error_msg = f"PDF文件不存在: {pdf_path}"
            logger.error(f"{self.MODULE_NAME}: {error_msg}")
            db.update_module_status(
                hashcode, self.MODULE_NAME,
                ProcessStatus.FAILED, error_msg, task.retry_count
            )
            return False, error_msg, ProcessStatus.FAILED
        
        db.update_module_status(
            hashcode, self.MODULE_NAME,
            ProcessStatus.PROCESSING
        )
        
        try:
            logger.debug(f"{self.MODULE_NAME}: 开始查找关键词 {os.path.basename(pdf_path)}")
            keywords = self.find_keywords(pdf_path)
            
            output_path = self.process_pdf(pdf_path, keywords)
            
            if output_path and os.path.exists(output_path):
                db.update_module_status(
                    hashcode, self.MODULE_NAME,
                    ProcessStatus.SUCCESS
                )
                logger.debug(f"{self.MODULE_NAME}: 处理成功 {os.path.basename(pdf_path)}")
                return True, f"处理成功: {output_path}", ProcessStatus.SUCCESS
            else:
                db.update_module_status(
                    hashcode, self.MODULE_NAME,
                    ProcessStatus.NO_OUTPUT,
                    "未找到关键词或无法提取内容",
                    task.retry_count
                )
                logger.debug(f"{self.MODULE_NAME}: 无输出 {os.path.basename(pdf_path)}")
                return True, "无输出(正常执行)", ProcessStatus.NO_OUTPUT
                
        except Exception as e:
            error_msg = f"处理异常: {str(e)}"
            logger.error(f"{self.MODULE_NAME}: {error_msg}")
            
            retry_count = task.retry_count + 1
            if retry_count < 2:
                status = ProcessStatus.FAILED
                logger.info(f"{self.MODULE_NAME}: 将重试 ({retry_count}/2)")
            else:
                status = ProcessStatus.FAILED
            
            db.update_module_status(
                hashcode, self.MODULE_NAME,
                status, error_msg, retry_count
            )
            return False, error_msg, status
