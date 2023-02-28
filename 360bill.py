import numpy as np
import pandas as pd

df = pd.read_csv('/Users/lijing/Desktop/我的工作/360/ecsv_2_2023.csv')
df.head()

# bill_pivot=pd.pivot_table(df[df['RecordType'] == 'LinkedLineItem'], index=['ProductName'], values=['TotalCost'], aggfunc=np.sum)

bill_pivot=pd.pivot_table(df[df['RecordType'] == 'LinkedLineItem'], index=['ProductName'], values=['TotalCost'], aggfunc=np.sum)

sorted_df_pivot = bill_pivot.sort_values(by=['TotalCost'], ascending=False)

print(sorted_df_pivot)