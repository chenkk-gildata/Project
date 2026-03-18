"""
增强版AI服务模块 - 解决速率限制和JSON解析问题
"""
import json
import logging
import time
import random
from typing import Dict, Any
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from config import ai_config

logger = logging.getLogger(__name__)


class EnhancedAIService:
    """增强版AI服务封装类"""

    def __init__(self):
        self.client = None
        self.request_count = 0
        self.last_request_time = 0
        self.min_request_interval = 0.5  # 最小请求间隔500ms
        self._initialize_client()

    def _initialize_client(self):
        """初始化AI客户端"""
        if not ai_config.api_key:
            raise ValueError("AI API密钥未配置")

        self.client = OpenAI(
            api_key=ai_config.api_key,
            base_url=ai_config.base_url,
        )

    def _rate_limit_control(self):
        """智能速率控制"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            # 添加随机抖动，避免多个请求同时发送
            sleep_time += random.uniform(0.1, 0.3)
            time.sleep(sleep_time)

        self.last_request_time = time.time()
        self.request_count += 1

    @retry(
        stop=stop_after_attempt(ai_config.max_retries),
        wait=wait_exponential(
            multiplier=2,  # 增加重试间隔倍数
            min=5,  # 最小等待5秒
            max=30  # 最大等待30秒
        ),
        retry=retry_if_exception_type((Exception,)),
        reraise=True
    )
    def extract_data_from_file(self, file_id: str, system_prompt: str) -> Dict[str, Any]:
        """从文件提取数据 - 增强版"""
        try:
            # 应用速率控制
            self._rate_limit_control()


            completion = self.client.chat.completions.create(
                model=ai_config.model,
                messages=[
                    {"role": "system", "content": f"fileid://{file_id}"},
                    {"role": "user", "content": system_prompt},
                ],
                response_format={"type": "json_object"},
                temperature=ai_config.temperature,
                top_p=ai_config.top_p,
                timeout=ai_config.timeout,
                max_tokens=8192  # 增加最大输出长度
            )

            json_response = completion.choices[0].message.content

            # 增强的JSON验证和修复
            return self._parse_and_validate_json(json_response)

        except Exception as e:
            logger.error(f"AI服务调用失败: {e}")
            # 如果是速率限制错误，增加等待时间
            if "429" in str(e) or "rate_limit" in str(e).lower():
                self.min_request_interval = min(self.min_request_interval * 1.5, 5.0)
                logger.warning(f"检测到速率限制，调整请求间隔为 {self.min_request_interval:.2f}秒")
            raise

    def _parse_and_validate_json(self, json_response: str) -> Dict[str, Any]:
        """解析和验证JSON响应 - 增强版"""
        try:
            # 首先尝试直接解析
            return json.loads(json_response)
        except json.JSONDecodeError as e:
            logger.warning(f"JSON解析失败，尝试修复: {e}")

            # 尝试修复常见的JSON问题
            fixed_json = self._fix_json_issues(json_response)

            try:
                return json.loads(fixed_json)
            except json.JSONDecodeError as e2:
                logger.error(f"JSON修复失败: {e2}")
                logger.error(f"原始响应: {json_response[:500]}...")
                logger.error(f"修复后响应: {fixed_json[:500]}...")
                raise ValueError(f"AI返回的JSON格式无效且无法修复: {e2}")

    def _fix_json_issues(self, json_str: str) -> str:
        """修复常见的JSON问题"""
        try:
            # 1. 移除可能的BOM标记
            if json_str.startswith('\ufeff'):
                json_str = json_str[1:]

            # 2. 处理未闭合的字符串
            json_str = self._fix_unterminated_strings(json_str)

            # 3. 处理多余的逗号
            json_str = self._fix_trailing_commas(json_str)

            # 4. 确保JSON对象完整性
            json_str = self._ensure_json_completeness(json_str)

            return json_str
        except Exception as e:
            logger.warning(f"JSON修复过程中出错: {e}")
            return json_str

    def _fix_unterminated_strings(self, json_str: str) -> str:
        """修复未闭合的字符串"""
        lines = json_str.split('\n')
        fixed_lines = []

        for line in lines:
            # 检查是否有未闭合的字符串
            if '"' in line and not line.strip().endswith('"') and not line.strip().endswith(','):
                # 尝试找到最后一个完整的键值对
                last_comma = line.rfind(',')
                if last_comma > 0:
                    # 截断到最后一个完整的键值对
                    line = line[:last_comma + 1]
                else:
                    # 如果整行都不完整，跳过这一行
                    continue
            fixed_lines.append(line)

        return '\n'.join(fixed_lines)

    def _fix_trailing_commas(self, json_str: str) -> str:
        """修复多余的逗号"""
        import re
        # 移除对象和数组中多余的逗号
        json_str = re.sub(r',\s*}', '}', json_str)
        json_str = re.sub(r',\s*]', ']', json_str)
        return json_str

    def _ensure_json_completeness(self, json_str: str) -> str:
        """确保JSON对象完整性"""
        # 如果JSON被截断，尝试补全
        if not json_str.strip().endswith('}'):
            # 计算大括号的平衡
            open_braces = json_str.count('{')
            close_braces = json_str.count('}')

            if open_braces > close_braces:
                # 补全缺失的闭合大括号
                json_str += '}' * (open_braces - close_braces)

        return json_str

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(
            multiplier=1.5,
            min=2,
            max=10
        ),
        retry=retry_if_exception_type((Exception,)),
        reraise=True
    )
    def upload_file(self, file_path: str) -> str:
        """上传文件到AI服务 - 增强版"""
        try:
            # 应用速率控制
            self._rate_limit_control()

            with open(file_path, 'rb') as f:
                file_object = self.client.files.create(
                    file=f,
                    purpose="file-extract"
                )
            return file_object.id
        except Exception as e:
            logger.error(f"文件上传失败: {e}")
            # 如果是速率限制错误，增加等待时间
            if "429" in str(e) or "rate_limit" in str(e).lower():
                self.min_request_interval = min(self.min_request_interval * 1.5, 5.0)
                logger.warning(f"检测到速率限制，调整请求间隔为 {self.min_request_interval:.2f}秒")
            raise

    def delete_file(self, file_id: str) -> bool:
        """删除上传的文件"""
        try:
            self.client.files.delete(file_id)
            return True
        except Exception as e:
            logger.warning(f"删除文件失败 {file_id}: {e}")
            return False



# 全局增强版AI服务实例
enhanced_ai_service = EnhancedAIService()
