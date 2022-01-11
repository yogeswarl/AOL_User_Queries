# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import pandas as pd
import numpy as np
#the size of dataset len(dataset)=3558412
from datetime import datetime
#dataset = open("DS/user-ct-test-collection-01.txt", "r").readlines()
Q = pd.read_csv(f'DS/user-ct-test-collection-01.txt', sep='\t', header=0)
print(Q.head(5))
if Q['QueryTime'][0]!=Q['QueryTime'][1]:
      print((Q['QueryTime'][2]), (Q['QueryTime'][3]))

x= pd.Timedelta(abs(pd.Timestamp(Q['QueryTime'][2])- pd.Timestamp(Q['QueryTime'][3]))).seconds
print(str(x))
x= 4
y=6
if pd.notnull(Q['ItemRank'][0]):
      print("yes")

df = pd.DataFrame({'id': np.arange(1,6,1),
                   'val': list('ABCDE')})
#print(Q['AnonID'])
df = pd.DataFrame(columns=['AnonId', 'Query', 'Query change', 'Query Date', 'Query Time'])
i=100
iterator = 0
list=[]
while i <130:
    #print("hiiii",i)
    diff={}
    diff["time"]=[]
    l = Q['Query'][i]
    ltime = Q['AnonID'][i]
    user_name=[]
    user_name.append(Q['Query'][i])
    user_name.append(Q['QueryTime'][i])
    #user_name[i]=[]
    for j in range(i+1,130):

        if (Q['AnonID'][i]==Q['AnonID'][j]):
              x = pd.Timedelta(abs(pd.Timestamp(Q['QueryTime'][i]) - pd.Timestamp(Q['QueryTime'][j]))).seconds
              if x<=60:
               # customDF = pd.DataFrame({'AnonID':Q['AnonID'][i],'Query':  Q['Query'][i],'Query change':Q['Query'][j],'Query Date':Q['QueryTime'][i], 'Query Time': Q['QueryTime'][j]},index=[0])
                # newDF = pd.DataFrame({'AnonID':Q['AnonID'][i],'Query':  Q['Query'][i],'Query change':Q['Query'][j]}, index=[])
                # df.to_csv(r'DS/final.txt', columns=["AnonID", "Query", "Query change"],index=False, sep=' ', mode='a')
                l2=Q['Query'][j]+Q['QueryTime'][j]
                if (Q['Query'][i] ==Q['Query'][j]):
                   #user_name = []
                   diff["time"].append(Q['QueryTime'][j])
                   user_name.append(diff)
                else:
                    user_name.append(Q['Query'][j])
                    diff["time"].append(Q['QueryTime'][j])
                    user_name.append(diff)
                    i = j
               # df = df.append(customDF)
                iterator += 1
                #i = j
              else:
                  print(user_name)
                  user_name=[]
                  diff["time"]=[]
                  # pass
    if (len(user_name)!=0):
        #customDF = pd.DataFrame(user_name)#({'AnonID': Q['AnonID'][i], 'Query': Q['Query'][i], 'Query change': Q['Query'][j], 'Query Date': Q['QueryTime'][i], 'Query Time': Q['QueryTime'][j]}, index=[0])
        print("user_name")
        #df = df.append(customDF)
    i=i+1

#df.to_csv(f'dataset/final.txt', index=False,sep=' ')