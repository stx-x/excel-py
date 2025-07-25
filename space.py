import pandas as pd
import os

# 1. 从终端读取输入路径
input_file = input("请输入 Excel 文件路径（例如 D:/data/原始.xlsx）：").strip()

# 2. 校验文件是否存在
if not os.path.isfile(input_file):
    print("❌ 文件不存在，请检查路径是否正确。")
    exit()

# 3. 读取 Excel 文件
df = pd.read_excel(input_file)

# 4. 清洗“身份证号”和“姓名”列
for col in ['身份证号', '姓名']:
    if col in df.columns:
        df[col] = df[col].astype(str).str.replace(r'\s+', '', regex=True)

# 5. 构建输出文件路径（在原文件后缀前添加 _clean）
base, ext = os.path.splitext(input_file)
output_file = f"{base}_clean{ext}"

# 6. 写入清洗后的 Excel 文件
df.to_excel(output_file, index=False)
print(f"✅ 清洗完成，已保存为：{output_file}")
