
import pandas as pd

data = pd.read_csv('erepo.tabbed.txt', sep='\t', header=0)
print(list(data))

for index, row in data.iterrows():
    print(row)
    for i in range(len(row)):
        print(row[i])

    sys.exit