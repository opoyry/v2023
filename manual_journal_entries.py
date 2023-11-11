import pandas as pd
import json
import logging

from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env.

logger = logging.getLogger('manual_journal_entries')

import os.path
vuosi = os.environ['vuosi']
dirname = os.path.expanduser(os.environ['manual_journal.inputDir'])
inputFile = os.environ['manual_journal.inputFile']
inputSheet = os.environ['manual_journal.inputSheet']
outputFile = os.environ['manual_journal.outputFile']

def KäteisViennit():
    df = pd.read_excel(f'{dirname}/{inputFile}', sheet_name= inputSheet)
    listData = []
    for index, row in df.iterrows():
        if not pd.isna(row['Vienti']):
            line = str(row['Vienti'])
            print(line)
            if line[0] != "#" and "," in line:
                json_data = "{" + line.replace("'", '"') + "}"
                try:
                    parsed_json = (json.loads(json_data))
                    listData.append(parsed_json)
                except json.decoder.JSONDecodeError as ve:
                    logger.error(ve)
                    logger.error(json_data)
                    raise # pass

    from glrules import glrule_logic
    dictRules = glrule_logic.load_rules()
    #print('Rules', dictRules)
    csv_lines = []
    print('Vientejä', len(listData), listData)
    for data in listData:
        iType = data["Type"]
        if iType not in dictRules:
            logger.warn(f"Rule not found with type {iType} {json.dumps(data)}")
        r = dictRules[iType].ProcessRule(data)
        #logger.debug(f"Result {r}")
        if r is not None:
            csv_lines.extend(r)

    df = pd.DataFrame(csv_lines)
    print(df)
    df.to_parquet(outputFile, compression=None)

KäteisViennit()
