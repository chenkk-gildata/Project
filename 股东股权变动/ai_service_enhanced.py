"""
增强版AI服务模块 - 解决速率限制和JSON解析问题
"""
import json
import time
import random
import os
from typing import Dict, Any
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, retry_if_not_exception_type, retry_if_exception
from config_ShareTransfer import ai_config
from logger_config import get_logger


class UnrecoverableJSONError(ValueError):
    """JSON格式无效且无法修复的异常"""
    pass

# 延迟初始化日志记录器
logger = None

def get_ai_logger():
    """获取日志记录器，延迟初始化"""
    global logger
    if logger is None:
        logger = get_logger(__name__)
    return logger


class EnhancedAIService:
    """增强版AI服务封装类"""

    def __init__(self):
        self.client = None
        self.request_count = 0
        self.last_request_time = 0
        self.min_request_interval = 0.5  # 最小请求间隔500ms
        self._initialize_client()

        # 添加速率限制监控变量
        self.rate_limit_errors = 0  # 速率限制错误计数
        self.last_rate_limit_error_time = 0  # 最后一次速率限制错误时间
        self.adaptive_interval = 0.5  # 自适应间隔
        self.request_history = []  # 请求历史记录
        self.max_history_size = 20  # 最大历史记录数

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

        # 记录请求历史
        self.request_history.append(current_time)
        if len(self.request_history) > self.max_history_size:
            self.request_history.pop(0)

        # 计算最近的请求频率
        if len(self.request_history) > 5:
            recent_requests = self.request_history[-5:]
            recent_interval = (recent_requests[-1] - recent_requests[0]) / (len(recent_requests) - 1)
            # 如果最近请求过于频繁，增加间隔
            if recent_interval < self.adaptive_interval * 0.8:
                self.adaptive_interval = min(self.adaptive_interval * 1.1, 2.0)
            # 如果最近请求较为稀疏，可以减少间隔
            elif recent_interval > self.adaptive_interval * 1.5:
                self.adaptive_interval = max(self.adaptive_interval * 0.95, 0.3)

        # 检查是否最近有速率限制错误
        if self.rate_limit_errors > 0 and (current_time - self.last_rate_limit_error_time) < 60:
            # 如果最近有速率限制错误，增加间隔
            error_penalty = min(self.rate_limit_errors * 0.2, 1.0)
            self.adaptive_interval = min(self.adaptive_interval * (1 + error_penalty), 3.0)
        elif self.rate_limit_errors > 0 and (current_time - self.last_rate_limit_error_time) > 120:
            # 如果距离上次速率限制错误超过2分钟，逐渐减少错误计数
            self.rate_limit_errors = max(0, self.rate_limit_errors - 1)

        # 使用自适应间隔
        if time_since_last < self.adaptive_interval:
            sleep_time = self.adaptive_interval - time_since_last
            # 添加随机抖动，避免多个请求同时发送
            sleep_time += random.uniform(0.05, 0.15)
            time.sleep(sleep_time)

        self.last_request_time = time.time()
        self.request_count += 1

        # 记录请求间隔
        get_ai_logger().debug(
            f"请求间隔: {time_since_last:.2f}s, 自适应间隔: {self.adaptive_interval:.2f}s, 速率限制错误: {self.rate_limit_errors}")

    @retry(
        stop=stop_after_attempt(ai_config.max_retries),
        wait=wait_exponential(
            multiplier=2,  # 增加重试间隔倍数
            min=5,  # 最小等待5秒
            max=30  # 最大等待30秒
        ),
        retry=retry_if_not_exception_type(UnrecoverableJSONError),
        reraise=True
    )
    def extract_data_from_file(self, file_id: str, system_prompt: str) -> Dict[str, Any]:
        """从文件提取数据 - 增强版，优化速率限制处理"""
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
            get_ai_logger().error(f"AI服务调用失败: {e}")
            # 如果是速率限制错误，增加错误计数并调整间隔
            if "429" in str(e) or "rate_limit" in str(e).lower():
                self.rate_limit_errors += 1
                self.last_rate_limit_error_time = time.time()
                # 立即增加请求间隔
                self.adaptive_interval = min(self.adaptive_interval * 1.5, 3.0)
                get_ai_logger().warning(
                    f"检测到速率限制， 调整请求间隔为 {self.adaptive_interval:.2f}秒")
            raise

    def _parse_and_validate_json(self, json_response: str) -> Dict[str, Any]:
        """解析和验证JSON响应 - 增强版"""
        try:
            # 首先尝试直接解析
            return json.loads(json_response)
        except json.JSONDecodeError as e:
            get_ai_logger().warning(f"JSON解析失败，尝试修复: {e}")

            try:
                # 尝试修复常见的JSON问题
                fixed_json = self._fix_json_issues(json_response)

                try:
                    return json.loads(fixed_json)
                except json.JSONDecodeError as e2:
                    get_ai_logger().error(f"JSON修复失败: {e2}")
                    get_ai_logger().error(f"原始响应完整内容: {json_response[-100:]}")
                    get_ai_logger().error(f"修复后响应: {fixed_json[-100:]}")
                    # 抛出特定异常，用于标识JSON无法修复的情况
                    raise UnrecoverableJSONError(f"AI返回的JSON格式无效且无法修复: {e2}")
            except Exception as e3:
                # 如果修复过程中出错，直接抛出UnrecoverableJSONError
                get_ai_logger().error(f"JSON修复过程出错: {e3}")
                raise UnrecoverableJSONError(f"AI返回的JSON格式无效且修复过程出错: {e3}")

    def _fix_json_issues(self, json_str: str) -> str:
        """修复常见的JSON问题"""
        try:
            # 1. 移除可能的BOM标记
            if json_str.startswith('\ufeff'):
                json_str = json_str[1:]

            # 2. 处理未闭合的字符串
            json_str = self._fix_unterminated_strings(json_str)

            # 3. 确保JSON对象完整性
            json_str = self._ensure_json_completeness(json_str)

            # 4. 处理多余的逗号
            json_str = self._fix_trailing_commas(json_str)

            return json_str
        except Exception as e:
            get_ai_logger().warning(f"JSON修复过程中出错: {e}")
            # 返回原始字符串，让上层方法处理
            return json_str

    def _fix_unterminated_strings(self, json_str: str) -> str:
        """修复未闭合的字符串"""
        lines = json_str.split('\n')
        fixed_lines = []

        for line in lines:
            # 检查是否有未闭合的字符串
            if '"' in line and not line.strip().endswith('"') and not line.strip().endswith(','):
                # 特殊处理：如果是数组或对象开始标记，不跳过
                stripped_line = line.strip()
                if any(keyword in stripped_line for keyword in ['"extracted_data": [', ':[', ': {', ':{', ':[{']):
                    # 保留包含数组或对象开始标记的行，避免丢失结构信息
                    fixed_lines.append(line)
                    continue
                    
                # 尝试找到最后一个完整的键值对
                last_comma = line.rfind(',')
                if last_comma > 0:
                    # 截断到最后一个完整的键值对
                    line = line[:last_comma + 1]
                else:
                    # 如果整行都不完整，跳过这一行
                    continue
            fixed_lines.append(line)

        fixed_json = '\n'.join(fixed_lines)

        return fixed_json

    def _fix_trailing_commas(self, json_str: str) -> str:
        """修复多余的逗号"""
        import re
        # 移除对象和数组中多余的逗号
        json_str = re.sub(r',\s*}', '}', json_str)
        json_str = re.sub(r',\s*]', ']', json_str)
        return json_str

    def _ensure_json_completeness(self, json_str: str) -> str:
        """确保JSON对象完整性"""
        # 计算大括号的平衡
        open_braces = json_str.count('{')
        close_braces = json_str.count('}')
        # 计算方括号的平衡
        open_brackets = json_str.count('[')
        close_brackets = json_str.count(']')
        
        # 补全缺失的闭合大括号和方括号
        if open_brackets > close_brackets and open_braces > close_braces:
            if open_braces - close_braces == 1:
                json_str += ']}'
            elif open_braces - close_braces == 2:
                json_str += '}]}'
        elif open_braces > close_braces:
            json_str += '}' * (open_braces - close_braces)
            
        return json_str

    def upload_file(self, file_path: str) -> str:
        """上传文件到AI服务 - 增强版，添加超时控制和重试机制"""
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            # 初始化变量，确保在异常情况下也能正确清理资源
            executor = None
            future = None

            try:
                # 应用速率控制
                self._rate_limit_control()

                # 添加文件大小检查
                file_size = os.path.getsize(file_path)
                if file_size > 50 * 1024 * 1024:  # 50MB限制
                    get_ai_logger().warning(f"文件过大，可能上传缓慢: {file_path} ({file_size / 1024 / 1024:.2f}MB)")

                # 使用concurrent.futures实现超时控制和任务取消
                import concurrent.futures

                executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
                # 提交上传任务
                future = executor.submit(self._upload_file_task, file_path)

                try:
                    # 等待任务完成，使用文件上传专用超时时间
                    file_id = future.result(timeout=ai_config.file_upload_timeout)
                    get_ai_logger().info(f"文件上传成功: {file_path} (文件ID: {file_id})")
                    return file_id
                except concurrent.futures.TimeoutError:
                    # 超时处理
                    get_ai_logger().error(f"文件上传超时(第{retry_count + 1}次尝试): {file_path}")
                    # 取消任务
                    if future:
                        future.cancel()
                        # 等待任务真正取消
                        try:
                            future.result(timeout=1)  # 短暂等待确保任务被取消
                        except:
                            pass  # 忽略取消过程中的任何错误

                    retry_count += 1

                    if retry_count < max_retries:
                        wait_time = min(2 ** retry_count, 5)  # 指数退避，最大5秒
                        get_ai_logger().info(f"等待{wait_time}秒后重试上传...")
                        time.sleep(wait_time)
                    else:
                        raise TimeoutError(f"文件上传超时，已重试{max_retries}次: {file_path}")

            except Exception as e:
                # 如果是速率限制错误，增加错误计数并调整间隔
                if "429" in str(e) or "rate_limit" in str(e).lower():
                    self.rate_limit_errors += 1
                    self.last_rate_limit_error_time = time.time()
                    # 立即增加请求间隔
                    self.adaptive_interval = min(self.adaptive_interval * 1.5, 3.0)
                    get_ai_logger().warning(
                        f"上传时检测到速率限制，错误计数: {self.rate_limit_errors}, 调整请求间隔为 {self.adaptive_interval:.2f}秒")

                retry_count += 1
                if retry_count < max_retries:
                    wait_time = min(2 ** retry_count, 5)  # 指数退避，最大5秒
                    get_ai_logger().warning(f"上传失败(第{retry_count}次尝试): {file_path} - {e}")
                    get_ai_logger().info(f"等待{wait_time}秒后重试...")
                    time.sleep(wait_time)
                else:
                    get_ai_logger().error(f"文件上传失败，已重试{max_retries}次: {file_path} - {e}")
                    raise Exception(f"文件上传失败，已重试{max_retries}次: {e}")
            finally:
                # 确保资源被正确清理
                if future and not future.done():
                    future.cancel()
                if executor:
                    executor.shutdown(wait=False)  # 不等待正在执行的任务完成，强制关闭
                # file_handle已在_upload_file_task中通过with语句自动管理，无需手动关闭

    def _upload_file_task(self, file_path: str) -> str:
        """实际执行文件上传的任务"""
        with open(file_path, 'rb') as f:
            file_object = self.client.files.create(
                file=f,
                purpose="file-extract",
                timeout=ai_config.file_upload_timeout  # 使用文件上传专用超时时间
            )
            return file_object.id

    def delete_file(self, file_id: str) -> bool:
        """删除上传的文件"""
        try:
            self.client.files.delete(file_id)
            return True
        except Exception as e:
            get_ai_logger().warning(f"删除文件失败 {file_id}: {e}")
            time.sleep(3)
            self.client.files.delete(file_id)
            return False


# 全局增强版AI服务实例
enhanced_ai_service = EnhancedAIService()
