import glob
import logging
import os
import xml.etree.cElementTree as ET
from datetime import datetime

import pandas as pd


def model(dbt, session):
    dbt.config(materialized="incremental", incremental_strategy="append")
    logger = logging.getLogger("python_logger")
    # df = dbt.ref("upstream_table").to_dataframe(session)
    # dbt_df = dbt.ref("read_bank_statement")
    # pandas_df = dbt_df.df()

    # only new rows compared to max in current table
    if dbt.is_incremental:
        max_from_this = f"select max(date) from {dbt.this}"
        max_date_was = session.sql(max_from_this).df(date_as_object=True).iloc[0, 0]
        print("max_date_was", max_from_this, max_date_was)
        logger.info(f"Logging from Python module. max_date_was {max_date_was}")

    def chooseLatestFile():
        file_name_pattern = dbt.config.get("file_name_pattern")
        print("file_name_pattern", file_name_pattern)
        files = glob.glob(file_name_pattern, recursive=False)
        return max(files, key=os.path.basename)  # getctime)

    ns = {"doc": "urn:schemas-microsoft-com:office:spreadsheet"}
    fileName = chooseLatestFile()
    print("fileName", fileName)
    tree = ET.parse(fileName)
    root = tree.getroot()

    def getvalueofnode(node):
        """return node text or None"""
        return node.text if node is not None else None

    colNames = []
    data = []
    for i, node in enumerate(root.findall(".//doc:Row", ns)):
        if i == 0:
            colNames.extend(
                getvalueofnode(cell.find("doc:Data", ns))
                for cell in node.findall("doc:Cell", ns)
            )
        else:
            row = {"Rivino": i}
            for j, cell in enumerate(node.findall("doc:Cell", ns)):
                row[colNames[j]] = getvalueofnode(cell.find("doc:Data", ns))
            data.append(row)
    df = pd.DataFrame(data)

    df["date"] = pd.to_datetime(df["Kirjauspäivä"], format="%d.%m.%Y").dt.date

    print(df)

    if dbt.is_incremental and max_date_was is not None:
        print("Filtering df to only rows with date >= ", max_date_was)
        df = df[(df["date"] > max_date_was)]  # df.filter(df.date >= max_date_was)
        print("Filtered", df)

    # def convert_date_if_exists(x):
    #    return None if x is None else datetime.strptime(x, "%d.%m.%Y").dt.date
    # df["date2"] = df["Arvopäivä"].apply(convert_date_if_exists)

    df["date2"] = df["Arvopäivä"].apply(
        lambda x: datetime.strptime(x, "%d.%m.%Y").date() if x is not None else None
    )
    df["amount"] = pd.to_numeric(df["Määrä EUR"])
    df["ref"] = df["Viite/viesti"].astype(str)
    df["ref"] = df["ref"].apply(lambda x: x.replace("\n", " "))
    df["status"] = df["Tila"].astype(str)
    df["rownum"] = df["Rivino"]
    df["inserted_at"] = datetime.now()

    print("bank statement df", df)
    df.info()
    df = df[["date", "date2", "amount", "ref", "rownum", "status", "inserted_at"]]
    print("bank statement df", df)
    # df.to_parquet(outputFile, compression="gzip")
    return df
