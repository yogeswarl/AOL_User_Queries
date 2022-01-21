import pandas as pd
import numpy as np

#the size of dataset len(dataset)=3558412
from datetime import datetime
Q = pd.read_csv(f'DS/user-ct-test-collection-01.txt', sep='\t', header=0)
# print(Q.head(5))
# if Q['QueryTime'][0]!=Q['QueryTime'][1]:
#       print((Q['QueryTime'][2]), (Q['QueryTime'][3]))

x= pd.Timedelta(abs(pd.Timestamp(Q['QueryTime'][2])- pd.Timestamp(Q['QueryTime'][3]))).seconds
# print(str(x))
x= 4
y=6
if pd.notnull(Q['ItemRank'][0]):
      print("yes")

# df = pd.DataFrame({'id': np.arange(1,6,1),
#                    'val': list('ABCDE')})
#print(Q['AnonID'])
# df = pd.DataFrame(columns=['index','AnonId', 'Query', 'Query change', 'Query Date', 'Query Time'])
i=0
group_config = Q.groupby('AnonID')
for k,gp in group_config:
    # if(k == 142):
    filterWord = gp[gp["ClickURL"].notnull()]
    processedData = filterWord["QueryTime"].astype('datetime64[ns]').to_frame().reset_index()
    processedData.columns = ['index', 'QueryTime']
    l=0
    for l in processedData.index:
        # config_var = gp.index[l] if k == 142 else gp.index[l-1]
        config_var = gp.index[l]
        prev_var_time =gp.loc[config_var]['QueryTime']
        IntervalTimer = pd.Timedelta(abs(pd.Timestamp(processedData['QueryTime'][l]) - pd.Timestamp(prev_var_time))).seconds
        if IntervalTimer >60:
            print('('+str(k) + ','+gp.loc[filterWord.index[l]]['Query'] +',' +'NULL, [(1,' + gp.loc[filterWord.index[l]]['ClickURL']+')])')
            # (142, westchester.gov, NULL, [(1, http: // www.westchestergov.com)])
        else:
            continue

        # IntervalTimer = pd.Timedelta(abs(pd.Timestamp(processedData['QueryTime'][l+1]) - pd.Timestamp(processedData['QueryTime'][l]))).seconds
        # if IntervalTimer <=60:






