import sys
# want to provide parameters to the pipeline 
# we get whole year of data and want to process only month 12
sys.argv

print('arguments', sys.argv)
month = int(sys.argv[1])
print(f'hello pipeline, month {month}')

# what is pandas: the default way of processing data in python 
import pandas as pd 


df = pd.DataFrame({"day": [1, 2], "num_passengers": [3, 4]})
df["month"] = month
print(df.head())

df.to_parquet(f"output_{month}.parquet")
