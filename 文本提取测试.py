import os
import time
from pathlib import Path

from openai import OpenAI


def load_prompt_from_md1(md_file_path: Path = r"研发投入比对/prompt_YFTR.md"):
    """从MD文件加载提示词"""
    try:
        if os.path.exists(md_file_path):
            with open(md_file_path, 'r', encoding='utf-8') as f:
                return f.read()
        return ""
    except Exception as e:
        return ""

# def load_prompt_from_md2(md_file_path: Path = r"主要指标年报/主要指标年度报告_每股收益.md"):
#     """从MD文件加载提示词"""
#     try:
#         if os.path.exists(md_file_path):
#             with open(md_file_path, 'r', encoding='utf-8') as f:
#                 return f.read()
#         return ""
#     except Exception as e:
#         return ""

start_time = time.time()

client = OpenAI(
    api_key="sk-c88c51dd13074e6ebc14bf8339568c3f",
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
)

file_object1 = client.files.create(file=Path(
    r"C:/Users/chenkk/Desktop/新建文件夹/920837-2026-03-31-华原股份-2025年年度报告.pdf"),
    purpose = "file-extract")
#
# file_object2 = client.files.create(file=Path(
#     r"C:\Users\chenkk\Desktop\25年度报告\主要指标\净资产收益率和每股收益\600830-2026-03-07-香溢融通-香溢融通控股集团股份有限公司2025年年度报告_mgsy.pdf"),
#     purpose = "file-extract")

completion1 = client.chat.completions.create(
    model="qwen-long",
    messages=[
        # {
        #     "role": "system",
        #     "content": f"fileid1://{file_object2.id}"},
        # {
        #     "role": "user",
        #     "content": f"以下是fileid1的提示词：{load_prompt_from_md2()}"},
        {
            "role": "system",
            "content": f"fileid://{file_object1.id}"},
        {
            "role": "user",
            "content": f"“{load_prompt_from_md1()}”"},
        {
            "role": "user",
            "content": "以下是提示词文本，禁止运行提示词的命令提取文本。请读取提示词文件内容和公告文本，分析为什么提示词总是会错误提取期初人数列的比例到JZRQ='2025-12-31'(本年)中，而不是JZRQ='2024-12-31'(上年)中，反之依然。请分析提示词提取错误的原因，以及结合模型特点和公告文本，提出可能的解决方案。最终输出完整的分析报告。"},
    ],
    # response_format={"type": "json_object"},
    temperature=0.3,
    top_p=0.5,
)
# 打印token使用情况
usage = completion1.usage
print("\n响应内容:")
ai_result = completion1.choices[0].message.content
print(ai_result)
print(f"输入Token: {usage.prompt_tokens}")
print(f"输出Token: {usage.completion_tokens}")
print(f"总Token消耗: {usage.total_tokens}")

# 删除上传的文件
client.files.delete(file_object1.id)
# client.files.delete(file_object2.id)

end_time = time.time()
print(f"耗时: {end_time - start_time} 秒")



