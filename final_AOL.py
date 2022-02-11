import pandas as pd
from pandas.core.dtypes.missing import isna

data = pd.read_csv(f'DS/aolchanged1.txt', sep='\t')
groups = data.groupby("AnonID")

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

        if timeDiff > 60 and not isna(currentRow["ClickURL"]):
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

                if 60 >= timeDiff >= 0:
                    if not isna(concurrentRow["ClickURL"]):
                        resultURLs.append((concurrentRow["ItemRank"], concurrentRow["ClickURL"]))
                        if concurrentRow["Query"] == currentRow["Query"]:
                            updatedQuery.append("NULL")
                        else:
                            updatedQuery.append(currentRow["Query"])
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
                        results.append(result)
                        print(query["QueryTime"], len(userQuery), result)
                elif correctQuery:
                    for i, query in enumerate(correctQuery):
                        result = (query["AnonID"], query["Query"], updatedQuery[i], resultURLs)
                        results.append(result)
                        print(query["QueryTime"], len(userQuery), result)
                    if (concurrentRowIndex == groupLength - 1 or concurrentRowIndex == groupLength):
                        print(f"{concurrentRow['QueryTime']} ({concurrentRow['AnonID']},'{concurrentRow['Query']}','NULL',[({concurrentRow['ItemRank']},'{concurrentRow['ClickURL']})])'")


            skip = concurrentRowIndex + 1
