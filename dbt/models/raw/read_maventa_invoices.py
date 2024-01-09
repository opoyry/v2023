import glob
import os

import pandas as pd


def model(dbt, session):
    def chooseLatestFile():
        file_name_pattern = dbt.config.get("maventa_invoices_excel_file_name")
        print("file_name_pattern", file_name_pattern)
        files = glob.glob(file_name_pattern, recursive=False)
        return max(files, key=os.path.getctime)

    filename = chooseLatestFile()
    print("filename", filename)

    df = pd.read_excel(filename, dtype={"Laskunumero": str, "Viitenumero": str})
    print(df)

    try:
        df["date"] = pd.to_datetime(df["Laskun päivä"], format="%d.%m.%Y").dt.date
        df["due_date"] = pd.to_datetime(df["Eräpäivä"], format="%d.%m.%Y").dt.date
        df["received_at"] = pd.to_datetime(df["Saapunut"], format="%d.%m.%Y %H:%M")
        df["amount"] = pd.to_numeric(df["Verollinen summa"])
        df["amount_net"] = pd.to_numeric(df["Veroton summa"])
        # vat = amount - amount_net
        # df["vat"] = df.apply(lambda row: row["amount"] - row["amount_net"], axis=1)
        df["sender"] = df["Lähettäjä"].astype(str)
        df["ref"] = df["Viitenumero"]
        df.drop(
            [
                "Laskun päivä",
                "Eräpäivä",
                "Saapunut",
                "Verollinen summa",
                "Veroton summa",
                "Lähettäjä",
                "Viitenumero",
            ],
            axis=1,
            inplace=True,
        )

        # df.dropna(subset=["date", "amount", "account"], inplace=True)
        # df = df[df["amount"] != 0]

        # df = df[["date", "amount", "vat", "amount_net", "account", "memo"]]

        # df.to_parquet(outputFile, compression="gzip")
        print("manual entries df", df)
        df.info()

    except Exception as error:
        print("Error:", error)
        raise error

    return df
