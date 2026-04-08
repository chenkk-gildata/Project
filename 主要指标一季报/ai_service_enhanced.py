"""
增强版AI服务模块 - 解决速率限制和JSON解析问题
"""
import json
import logging
import time
import random
import os
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
        self.min_request_interval = 0.5
        self._initialize_client()

        self.rate_limit_errors = 0
        self.last_rate_limit_error_time = 0
        self.adaptive_interval = 0.5
        self.request_history = []
        self.max_history_size = 20

    def _initialize_client(self):
        """初始化AI客户端"""
        if not ai_config.api_key:
            raise ValueError("AI API密钥未配置")

        self.client = OpenAI(
            api_key=ai_config.api_key,
            base_url=ai_config.base_url,
        )

    def _rate_limit_control(self):
        """智能速率控制 - 优化版，自适应调整间隔"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time

        self.request_history.append(current_time)
        if len(self.request_history) > self.max_history_size:
            self.request_history.pop(0)

        if len(self.request_history) > 5:
            recent_requests = self.request_history[-5:]
            recent_interval = (recent_requests[-1] - recent_requests[0]) / (len(recent_requests) - 1)
            if recent_interval < self.adaptive_interval * 0.8:
                self.adaptive_interval = min(self.adaptive_interval * 1.1, 2.0)
            elif recent_interval > self.adaptive_interval * 1.5:
                self.adaptive_interval = max(self.adaptive_interval * 0.95, 0.3)

        if self.rate_limit_errors > 0 and (current_time - self.last_rate_limit_error_time) < 60:
            error_penalty = min(self.rate_limit_errors * 0.2, 1.0)
            self.adaptive_interval = min(self.adaptive_interval * (1 + error_penalty), 3.0)
        elif self.rate_limit_errors > 0 and (current_time - self.last_rate_limit_error_time) > 120:
            self.rate_limit_errors = max(0, self.rate_limit_errors - 1)

        if time_since_last < self.adaptive_interval:
            sleep_time = self.adaptive_interval - time_since_last
            sleep_time += random.uniform(0.05, 0.15)
            time.sleep(sleep_time)

        self.last_request_time = time.time()
        self.request_count += 1

        logger.debug(
            f"请求间隔: {time_since_last:.2f}s, 自适应间隔: {self.adaptive_interval:.2f}s, 速率限制错误: {self.rate_limit_errors}")

    @retry(
        stop=stop_after_attempt(ai_config.max_retries),
        wait=wait_exponential(
            multiplier=2,
            min=5,
            max=30
        ),
        retry=retry_if_exception_type((Exception,)),
        reraise=True
    )
    def extract_data_from_file(self, file_id: str, system_prompt: str) -> Dict[str, Any]:
        """从文件提取数据 - 增强版，优化速率限制处理"""
        try:
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
                max_tokens=8192
            )

            json_response = completion.choices[0].message.content

            return self._parse_and_validate_json(json_response)

        except Exception as e:
            logger.error(f"AI服务调用失败: {e}")
            if "429" in str(e) or "rate_limit" in str(e).lower():
                self.rate_limit_errors += 1
                self.last_rate_limit_error_time = time.time()
                self.adaptive_interval = min(self.adaptive_interval * 1.5, 3.0)
                logger.warning(
                    f"检测到速率限制， 调整请求间隔为 {self.adaptive_interval:.2f}秒")
            raise

    def _parse_and_validate_json(self, json_response: str) -> Dict[str, Any]:
        """解析和验证JSON响应 - 增强版"""
        try:
            return json.loads(json_response)
        except json.JSONDecodeError as e:
            logger.warning(f"JSON解析失败，尝试修复: {e}")

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
            if json_str.startswith('\ufeff'):
                json_str = json_str[1:]

            json_str = self._fix_unterminated_strings(json_str)

            json_str = self._fix_trailing_commas(json_str)

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
            if '"' in line and not line.strip().endswith('"') and not line.strip().endswith(','):
                last_comma = line.rfind(',')
                if last_comma > 0:
                    line = line[:last_comma + 1]
                else:
                    continue
            fixed_lines.append(line)

        return '\n'.join(fixed_lines)

    def _fix_trailing_commas(self, json_str: str) -> str:
        """修复多余的逗号"""
        import re
        json_str = re.sub(r',\s*}', '}', json_str)
        json_str = re.sub(r',\s*]', ']', json_str)
        return json_str

    def _ensure_json_completeness(self, json_str: str) -> str:
        """确保JSON对象完整性"""
        if not json_str.strip().endswith('}'):
            open_braces = json_str.count('{')
            close_braces = json_str.count('}')

            if open_braces > close_braces:
                json_str += '}' * (open_braces - close_braces)

        return json_str

    def upload_file(self, file_path: str) -> str:
        """上传文件到AI服务 - 增强版，添加超时控制和重试机制"""
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            executor = None
            future = None

            try:
                self._rate_limit_control()

                file_size = os.path.getsize(file_path)
                if file_size > 50 * 1024 * 1024:
                    logger.warning(f"文件过大，可能上传缓慢: {file_path} ({file_size / 1024 / 1024:.2f}MB)")

                import concurrent.futures

                executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
                future = executor.submit(self._upload_file_task, file_path)

                try:
                    file_id = future.result(timeout=ai_config.file_upload_timeout)
                    logger.info(f"文件上传成功: {file_path} (文件ID: {file_id})")
                    return file_id
                except concurrent.futures.TimeoutError:
                    logger.error(f"文件上传超时(第{retry_count + 1}次尝试): {file_path}")
                    if future:
                        future.cancel()
                        try:
                            future.result(timeout=1)
                        except:
                            pass

                    retry_count += 1

                    if retry_count < max_retries:
                        wait_time = min(2 ** retry_count, 5)
                        logger.info(f"等待{wait_time}秒后重试上传...")
                        time.sleep(wait_time)
                    else:
                        raise TimeoutError(f"文件上传超时，已重试{max_retries}次: {file_path}")

            except Exception as e:
                if "429" in str(e) or "rate_limit" in str(e).lower():
                    self.rate_limit_errors += 1
                    self.last_rate_limit_error_time = time.time()
                    self.adaptive_interval = min(self.adaptive_interval * 1.5, 3.0)
                    logger.warning(
                        f"上传时检测到速率限制，错误计数: {self.rate_limit_errors}, 调整请求间隔为 {self.adaptive_interval:.2f}秒")

                retry_count += 1
                if retry_count < max_retries:
                    wait_time = min(2 ** retry_count, 5)
                    logger.warning(f"上传失败(第{retry_count}次尝试): {file_path} - {e}")
                    logger.info(f"等待{wait_time}秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"文件上传失败，已重试{max_retries}次: {file_path} - {e}")
                    raise Exception(f"文件上传失败，已重试{max_retries}次: {e}")
            finally:
                if future and not future.done():
                    future.cancel()
                if executor:
                    executor.shutdown(wait=False)

    def _upload_file_task(self, file_path: str) -> str:
        """实际执行文件上传的任务"""
        with open(file_path, 'rb') as f:
            file_object = self.client.files.create(
                file=f,
                purpose="file-extract",
                timeout=ai_config.file_upload_timeout
            )
            return file_object.id

    def delete_file(self, file_id: str) -> bool:
        """删除上传的文件"""
        try:
            self.client.files.delete(file_id)
            return True
        except Exception as e:
            logger.warning(f"删除文件失败 {file_id}: {e}")
            time.sleep(3)
            self.client.files.delete(file_id)
            return False


enhanced_ai_service = EnhancedAIService()
