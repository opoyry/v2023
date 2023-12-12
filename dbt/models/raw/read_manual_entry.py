import pandas as pd


def model(dbt, session):
    filename = dbt.config.get("manual_entry_file_name")
    print("filename", filename)

    df = pd.read_excel(filename, dtype={"Tili": str, "Selite": str})

    df["date"] = pd.to_datetime(df["Pvm"], format="%d.%m.%Y").dt.date
    df["amount"] = pd.to_numeric(df["Yht"])
    df["vat"] = pd.to_numeric(df["ALV"])
    df["amount_net"] = pd.to_numeric(df["Netto"])
    df["account"] = df["Tili"].astype(str)
    df["memo"] = df["Selite"]  # .astype(str)

    df.dropna(subset=["date", "amount", "account"], inplace=True)
    df = df[df["amount"] != 0]

    df = df[["date", "amount", "vat", "amount_net", "account", "memo"]]

    # df.to_parquet(outputFile, compression="gzip")
    print("manual entries df", df)
    df.info()

    return df
