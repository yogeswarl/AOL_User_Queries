import pandas as pd
import numpy as np
import sys
from pandas.core.dtypes.missing import isna

dataset_name = sys.argv[1]
time_interval = sys.argv[2]


def read_ds(ds):
    data = pd.read_csv(f'DS/{ds}.txt', sep='\t')
    return data

def store_result(ds,result_list):
    output_store = pd.DataFrame(np.asarray(result_list), columns=["AnonID", "Query", "new Query", "Generated URLs"])
    output_store.to_csv(f'./output/{ds}-out.csv', index=False)

def generate_query(ds_name, interval):
    interval = int(interval)
    groups = read_ds(ds_name).groupby("AnonID")
    results = []
    for group_id, group in groups:
        group_reverse = group.iloc[::-1]

        groupLength = group_reverse.shape[0]
        skip = -1
        for row in range(groupLength):
            if row <= skip:
                continue
            currentRow = group_reverse.iloc[row]
            resultURLs = []
            userQuery = []
            correctQuery = []
            updatedQuery = []
            previousRowIndex = row + 1

            if previousRowIndex >= groupLength:
                if not isna(currentRow["ClickURL"]):
                    result = (
                        currentRow["AnonID"], currentRow["Query"], "NULL",
                        [(currentRow["ItemRank"], currentRow["ClickURL"])])
                    results.append(result)
                    print(currentRow["QueryTime"], result)
                continue

            previousRow = group_reverse.iloc[previousRowIndex]

            timeDiff = pd.Timedelta(
                abs(pd.Timestamp(currentRow["QueryTime"]) - pd.Timestamp(previousRow["QueryTime"]))).seconds

            if timeDiff > interval and not isna(currentRow["ClickURL"]):
                result = (
                    currentRow["AnonID"], currentRow["Query"], "NULL",
                    [(currentRow["ItemRank"], currentRow["ClickURL"])])
                results.append(result)
                print(currentRow["QueryTime"], result)
            else:
                concurrentRowIndex = 0
                for concurrentRowIndex in range(row, groupLength):
                    concurrentRow = group_reverse.iloc[concurrentRowIndex]
                    timeDiff = pd.Timedelta(
                        abs(pd.Timestamp(currentRow["QueryTime"]) - pd.Timestamp(concurrentRow["QueryTime"]))).seconds

                    if interval >= timeDiff >= 0:
                        if not isna(concurrentRow["ClickURL"]):
                            resultURLs.append((concurrentRow["ItemRank"], concurrentRow["ClickURL"]))
                            if concurrentRow["Query"] == currentRow["Query"]:
                                updatedQuery.append("NULL")
                            else:
                                updatedQuery.append(currentRow["Query"])
                            if concurrentRow["QueryTime"] == currentRow["QueryTime"] and concurrentRow.name != currentRow.name:
                                correctQuery.append(concurrentRow)
                        if isna(concurrentRow["ClickURL"]):
                            userQuery.append(concurrentRow)
                            updatedQuery.append(currentRow["Query"])
                    else:
                        break

                if resultURLs:
                    if userQuery:
                        for i, query in enumerate(userQuery):
                            result = (query["AnonID"], query["Query"], updatedQuery[i], resultURLs)
                            if i > 0:
                                if userQuery[i]["Query"] != userQuery[i - 1]["Query"] and pd.Timedelta(abs(pd.Timestamp(userQuery[i]["QueryTime"]) - pd.Timestamp(userQuery[i-1]["QueryTime"]))).seconds >60:
                                    print(query["QueryTime"], len(userQuery), result)
                                    results.append(result)
                            else:
                                print(query["QueryTime"], len(userQuery), result)
                                results.append(result)
                    elif correctQuery:
                        for i, query in enumerate(correctQuery):
                            result = (query["AnonID"], query["Query"], updatedQuery[i], resultURLs)
                            if i > 0:
                                if correctQuery[i]["Query"] != correctQuery[i - 1]["Query"] or pd.Timedelta(abs(pd.Timestamp(correctQuery[i]["QueryTime"]) - pd.Timestamp(correctQuery[i-1]["QueryTime"]))).seconds >60:
                                    results.append(result)
                                    print(query["QueryTime"], len(userQuery), result)

                            else:
                                results.append(result)
                                print(query["QueryTime"], len(userQuery), result)
                        if concurrentRowIndex == groupLength - 1 or concurrentRowIndex == groupLength:
                            print(f"{concurrentRow['QueryTime']} ({concurrentRow['AnonID']},'{concurrentRow['Query']}','NULL',[({concurrentRow['ItemRank']},'{concurrentRow['ClickURL']})])'")
                skip = concurrentRowIndex
    store_result(dataset_name,results)
generate_query(dataset_name,time_interval)