import pandas as pd
import os

# 获取用户输入的文件名
input_file = input("请输入Excel文件路径（含文件名）：").strip()

# 检查文件是否存在
if not os.path.isfile(input_file):
    print("❌ 文件不存在，请检查路径是否正确！")
    exit()

# 生成输出文件名
file_dir, file_name = os.path.split(input_file)
file_base, file_ext = os.path.splitext(file_name)
output_file = os.path.join(file_dir, f"{file_base}_处理结果{file_ext}")

print(f"📄 原始文件: {input_file}")
print(f"📄 处理后输出文件: {output_file}")

# 加载所有工作表名
excel_file = pd.ExcelFile(input_file)
sheet_names = excel_file.sheet_names

print(f"🔍 检测到 {len(sheet_names)} 个工作表：{sheet_names}")

df_list = []

with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    for sheet_name in sheet_names:
        print(f"👉 正在处理工作表：{sheet_name}")
        df = pd.read_excel(input_file, sheet_name=sheet_name)
        df['备注2'] = sheet_name
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        df_list.append(df)

    print("🧩 正在合并所有工作表生成汇总...")
    merged_df = pd.concat(df_list, ignore_index=True)
    merged_df.to_excel(writer, sheet_name='汇总', index=False)

print("✅ 全部处理完成！结果已保存至：", output_file)
