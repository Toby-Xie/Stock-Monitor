import pandas as pd

# ===== 文件路径 =====
file1 = r"C:\Users\TobyXie\Downloads\Default23_04_2026_16_21_21.xlsx"
file2 = r"C:\Users\TobyXie\Downloads\Default23_04_2026_16_23_18.xlsx"
output_file = r"C:\Users\TobyXie\Downloads\output.xlsx"

# ===== 读取数据 =====
# 注意：Excel列是从0开始计数
# H列 = 第7列, AC列 = 第28列
df1 = pd.read_excel(file1, usecols=[7, 28], names=["H", "AC"])

# A列 = 第0列, C列 = 第2列
df2 = pd.read_excel(file2, usecols=[0, 2], names=["A", "C"])

# ===== 建立映射关系（A -> C）=====
mapping_dict = dict(zip(df2["A"], df2["C"]))

# ===== 匹配并生成新列 =====
df1["匹配C列"] = df1["H"].map(mapping_dict)

# ===== 生成输出结果 =====
result = pd.DataFrame({
    "来自文件2的C列": df1["匹配C列"],
    "来自文件1的AC列": df1["AC"]
})

# ===== 写入Excel =====
result.to_excel(output_file, index=False)

print("处理完成，已输出到:", output_file)