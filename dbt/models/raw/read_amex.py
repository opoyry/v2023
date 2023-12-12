import glob
import os

import pandas as pd


def model(dbt, session):
    dbt.config(materialized="table")

    def chooseLatestFile():
        homeDir = os.path.expanduser("~")
        filePath = os.path.join(homeDir, "Downloads") + "/tapahtumat*xlsx"
        print("filePath", filePath)
        files = glob.glob(filePath, recursive=False)
        downloadedFileWithPath = max(files, key=os.path.getctime)
        print("downloadedFileWithPath", downloadedFileWithPath)
        return downloadedFileWithPath

    df = pd.read_excel(chooseLatestFile(), sheet_name="Tapahtuman tiedot", header=6)

    df["Päivämäärä"] = pd.to_datetime(df["Päivämäärä"]).dt.date  # , format="%d/%m/%Y")
    df["Summa"] = pd.to_numeric(df["Summa"])
    df["Tili"] = df["Tili"].astype(str)

    print("amex df", df)
    df.info()
    # df.to_parquet(outputFile, compression="gzip")
    return df
