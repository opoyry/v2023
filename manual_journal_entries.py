import json
import logging
import os.path

import pandas as pd
from dotenv import load_dotenv

# This script reads coded journal entries from Excel sheet and converts them to 2-n GL debit/credit entries in a parquet file.
# The conversion is done using logic in glrules package and rules stored in a yaml file.

load_dotenv()  # take environment variables from .env.

logger = logging.getLogger("manual_journal_entries")

vuosi = os.environ["vuosi"]
dirname = os.path.expanduser(os.environ["manual_journal.inputDir"])
inputFile = os.environ["manual_journal.inputFile"]
inputSheet = os.environ["manual_journal.inputSheet"]
outputFile = os.environ["manual_journal.outputFile"]


def KäteisViennit():
    df = pd.read_excel(f"{dirname}/{inputFile}", sheet_name=inputSheet)
    listData = []
    for index, row in df.iterrows():
        if not pd.isna(row["Vienti"]):
            line = str(row["Vienti"])
            print(line)
            if line[0] != "#" and "," in line:
                json_data = "{" + line.replace("'", '"') + "}"
                try:
                    parsed_json = json.loads(json_data)
                    listData.append(parsed_json)
                except json.decoder.JSONDecodeError as ve:
                    logger.error(ve)
                    logger.error(json_data)
                    raise  # pass

    from glrules import glrule_logic

    dictRules = glrule_logic.load_rules()
    # print('Rules', dictRules)
    csv_lines = []
    print("Vientejä", len(listData), listData)
    for data in listData:
        iType = data["Type"]
        if iType not in dictRules:
            logger.warn(f"Rule not found with type {iType} {json.dumps(data)}")
        r = dictRules[iType].ProcessRule(data)
        # logger.debug(f"Result {r}")
        if r is not None:
            csv_lines.extend(r)

    df = pd.DataFrame(csv_lines)
    print(df)
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")  # .astype(str)
    df.to_parquet(outputFile, compression=None)


KäteisViennit()
