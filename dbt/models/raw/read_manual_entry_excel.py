import pandas as pd


def model(dbt, session):
    filename = dbt.config.get("manual_entry_file_name")
    sheetname = "Tilinpäätösviennit"
    print("filename", filename)

    df = pd.read_excel(
        filename,
        sheet_name=sheetname,
        skiprows=15,
        dtype={"Tili": str, "Selite": str, "Debet": float, "Kredit": float},
    )
    df.dropna(subset=["Tili"], inplace=True)
    # print("df", df)

    df["date"] = pd.to_datetime(df["Päiväys"], format="%d.%m.%Y").dt.date
    df["debet"] = pd.to_numeric(df["Debet"])
    df["credit"] = pd.to_numeric(df["Kredit"])

    # Split account number from name
    df["account"] = df.apply(lambda x: x["Tili"].split(" ")[0], axis=1)
    # df["Tili"].astype(str).split(" ")[0]
    df["memo"] = df["Selite"]
    df["rownum"] = 0
    pvm = ""
    # df = df[["date", "amount", "vat", "amount_net", "account", "memo"]]
    for ind in df.index:
        # print(df['Name'][ind], df['Stream'][ind])
        df["rownum"][ind] = ind + 1
        if pd.isna(df["date"][ind]):
            df["date"][ind] = pvm
        else:
            pvm = df["date"][ind]

    df.drop(
        ["Päiväys", "Debet", "Kredit", "Unnamed: 2", "Tili", "Selite"],
        axis=1,
        inplace=True,
    )

    # df.to_parquet(outputFile, compression="gzip")
    print(df)
    # df.info()

    return df
