import pandas as pd

df1 = pd.DataFrame([['Snow', 'M', 22], ['Tyrion', 'M', 32], ['Sansa', 'F', 18], ['Arya', 'F', 14]],
                   columns=['name', 'gender', 'age'])
new = pd.DataFrame([{'name': 'lisa', 'gender': 'F', 'age': ''}
                       , {'name': 'tsy', 'gender': 'M', 'age': ''}], index=[0, 1])
df1 = df1.append(new, ignore_index=True)

print("--------更换单个值----------")
# loc和iloc 可以更换单行、单列、多行、多列的值
df1.loc[0, 'age'] = 25  # 思路：先用loc找到要更改的值，再用赋值（=）的方法实现更换值
df1.iloc[0, 2] = 25  # iloc：用索引位置来查找

# at 、iat只能更换单个值
df1.at[0, 'age'] = 25  # iat 用来取某个单值,参数只能用数字索引
df1.iat[0, 2] = 25  # at 用来取某个单值,参数只能用index和columns索引名称
print(df1)

df2 = df1[df1.age == '']
for index in df2.index:
    df1.at[index, 'age'] = 35

print(df1)
