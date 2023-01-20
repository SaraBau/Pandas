import pandas as pd
  
data = [['#1','tom', 10], ['#2','nick', 15], ['#3','juli', 14]]
  
df = pd.DataFrame(data, columns=['Id','Name', 'Age'])

df1 = df.set_index('Id')
print(df1)

print(df1.loc['#1'])  