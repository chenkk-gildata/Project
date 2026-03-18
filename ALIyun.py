from datetime import datetime, timezone, timedelta
from time import sleep

from openai import OpenAI

client = OpenAI(
    # 若没有配置环境变量，请用阿里云百炼API Key将下行替换为：api_key="sk-xxx",
    # 新加坡和北京地域的API Key不同。获取API Key：https://help.aliyun.com/zh/model-studio/get-api-key
    api_key="sk-c88c51dd13074e6ebc14bf8339568c3f",
    # 以下是北京地域base_url，如果使用新加坡地域的模型，需要将base_url替换为：https://dashscope-intl.aliyuncs.com/compatible-mode/v1
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
)

# 获取当前时间
now = datetime.now()
# 计算3天前的时间
three_days_ago = (now - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

# 获取文件列表
file_list = client.files.list()
print(f"总文件数量: {len(file_list.data)}")

#  打印文件列表
# for file in file_list:
#     file_name = file.filename
#     print(file_name)

# 删除3天前创建的文件
deleted_count = 0
for file in file_list.data:
    # 将时间戳转换为datetime对象
    file_time = datetime.fromtimestamp(file.created_at, timezone.utc)
    formatted_file_time = file_time.strftime("%Y-%m-%d %H:%M:%S")
    # 如果文件创建时间早于3天前，则删除
    if formatted_file_time < three_days_ago:
        try:
            client.files.delete(file.id)
            print(f"已删除文件: {file.filename} (ID: {file.id}, 创建时间: {file_time})")
            deleted_count += 1
        except Exception as e:
            print(f"删除文件失败: {file.filename} (ID: {file.id} - {e})")
            continue

        sleep(0.1)

print(f"总共删除了 {deleted_count} 个文件")

# 再次检查剩余文件数量
remaining_files = client.files.list()
print(f"剩余文件数量: {len(remaining_files.data)}")


#
#
# file_list = client.files.list()
# print(file_list)
# # 删除文件名包含"股东"的所有文件
# for file in file_list.data:
#     client.files.delete(file.id)
#     print(f"已删除文件: {file.filename} (ID: {file.id})")
# file_list = client.files.list()
# files = list(file_list)
# file_count = len(files)
# print(file_count)
