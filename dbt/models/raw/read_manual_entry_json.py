import json

import pandas as pd


def model(dbt, session):
    filename = dbt.config.get("manual_entry_json_file_name")
    print("filename", filename)
    listData = []
    with open(filename) as file:
        for line in file:
            print(line)
            if line[0] != "#" and "," in line:
                json_data = "{" + line.replace("'", '"') + "}"
                try:
                    parsed_json = json.loads(json_data)
                    parsed_json["rownum"] = len(listData) + 1
                    listData.append(parsed_json)
                except json.decoder.JSONDecodeError as ve:
                    # logger.error(ve)
                    # logger.error(json_data)
                    raise  # pass

    import sys

    sys.path.append("..")

    from glrules import glrule_logic

    dictRules = glrule_logic.load_rules()
    # print('Rules', dictRules)
    csv_lines = []
    print("Vientejä", len(listData), listData)
    for data in listData:
        iType = data["Type"]
        if iType not in dictRules:
            print(f"Rule not found with type {iType} {json.dumps(data)}")
        r = dictRules[iType].ProcessRule(data)
        # logger.debug(f"Result {r}")
        if r is not None:
            csv_lines.extend(r)

    df = pd.DataFrame(csv_lines)
    print(df)
    df["date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["amount"] = pd.to_numeric(df["Amount"])
    df["account"] = df["Account"].astype(str)
    df["memo"] = df["Memo"].astype(str)
    df["dim1"] = df["Dim1"]

    df = df[["rownum", "date", "amount", "account", "memo", "dim1"]]

    print("manual entries df", df)
    df.info()

    return df
