import os
import time
from pathlib import Path

from openai import OpenAI


def load_prompt_from_md(md_file_path: Path = r"业绩预告比对/业绩预告优化.md"):
    """从MD文件加载提示词"""
    try:
        if os.path.exists(md_file_path):
            with open(md_file_path, 'r', encoding='utf-8') as f:
                return f.read()
        return ""
    except Exception as e:
        return ""

start_time = time.time()

client = OpenAI(
    api_key="sk-c88c51dd13074e6ebc14bf8339568c3f",
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
)

file_object1 = client.files.create(file=Path(
    # r"C:\Users\chenkk\Desktop\test\688577-2026-01-29-浙海德曼-浙海德曼2025年年度业绩预告.PDF"),
    r"Z:\特别组\9.外包\AI比对\业绩预告\业绩预告小程序比对\files\20260129_225222\000301-2026-01-30-东方盛虹-2025年度业绩预告.PDF"),
    purpose = "file-extract")
#
# file_object2 = client.files.create(file=Path(
#     r"C:\Users\chenkk\Desktop\test\688690-2026-01-16-纳微科技-苏州纳微科技股份有限公司2025年年度业绩预告的自愿性披露公告.PDF"),
#     purpose = "file-extract")

completion1 = client.chat.completions.create(
    model="qwen-long",
    messages=[
        {
            "role": "system",
            "content": f"fileid://{file_object1.id}"},
        {
            "role": "user",
            "content": '''我上传了一个pdf文件，现在我希望你帮我读取这个文件里的内容并提取相关信息，为了方便查找，我将pdf文件里的关键字高亮显示了，请问这对于你提取信息是否有帮助？是否会影响提取的结果？
            '''},
        # {
        #     "role": "user",
        #     "content": f"以下是提示词：{load_prompt_from_md()}"},
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



