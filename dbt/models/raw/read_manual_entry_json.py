import glob
import json

import numpy as np
import pandas as pd


def model(dbt, session):
    filePath = dbt.config.get("manual_entry_json_file_name")
    print("filePath", filePath)

    def read_manual_entry_lines(filename):
        lines = []
        with open(filename) as file:
            for line in file:
                print(line)
                if line[0] != "#" and "," in line:
                    json_data = "{" + line.replace("'", '"') + "}"
                    try:
                        parsed_json = json.loads(json_data)
                        parsed_json["rownum"] = len(lines) + 1
                        parsed_json["filename"] = filename
                        lines.append(parsed_json)
                    except json.decoder.JSONDecodeError as ve:
                        # logger.error(ve)
                        # logger.error(json_data)
                        raise  # pass
        print("filename", filename, "lines", lines)
        return lines

    listData = []
    # Iterate files matching pattern in variable filename
    files = glob.glob(filePath, recursive=False)
    for filename in files:
        print("filename", filename)
        listData.extend(read_manual_entry_lines(filename))

    print("listData, len", len(listData), listData)
    import sys

    sys.path.append("..")

    from glrules import glrule_logic

    dictRules = glrule_logic.load_rules()
    # print('Rules', dictRules)
    transactions = []
    print("Vientejä", len(listData), listData)
    for data in listData:
        iType = data["Type"]
        if iType not in dictRules:
            print(f"Rule not found with type {iType} {json.dumps(data)}")
        r = dictRules[iType].ProcessRule(data)
        # logger.debug(f"Result {r}")
        if r is not None:
            transactions.extend(r)

    df = pd.DataFrame(transactions)
    print(df)
    df["date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["amount"] = pd.to_numeric(df["Amount"])
    df["account"] = df["Account"].astype(str)
    df["memo"] = df["Memo"].astype(str)
    df["dim1"] = df["Dim1"]
    df["tositelaji"] = np.where(
        df["type"] == 10,
        "ML",
        np.where(
            df["type"] == 21,
            "PLK",
            np.where(df["type"] == 30, "OL", np.where(df["type"] == 50, "KÄT2", "XX")),
        ),
    )
    df = df[
        [
            "rownum",
            "date",
            "amount",
            "account",
            "memo",
            "dim1",
            "type",
            "tositelaji",
            "filename",
        ]
    ]

    print("manual entries df", df)
    df.info()

    return df
