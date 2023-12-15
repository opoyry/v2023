import csv
import re

import pandas as pd


def model(dbt, session):
    dbt_df = dbt.ref("gl_csv")
    pandas_df = dbt_df.df()

    output_file_name = dbt.config.get("output_file_name")

    print("output_file_name", output_file_name, "df", pandas_df)
    pandas_df["Selite"] = pandas_df["Selite"].apply(
        lambda x: re.sub(r"\s+", " ", x) if x is not None else ""
    )
    pandas_df["Debet"] = pandas_df["Debet"].apply(
        lambda x: round(x, 2) if x is not None else None
    )
    pandas_df["Kredit"] = pandas_df["Kredit"].apply(
        lambda x: round(x, 2) if x is not None else None
    )
    pandas_df.to_csv(
        output_file_name, index=False, quoting=csv.QUOTE_ALL, date_format="%d.%m.%Y"
    )

    # Print also each entry type into own file
    for entry_type in pandas_df["source"].unique():
        print("entry_type", entry_type)
        pandas_df[pandas_df["source"] == entry_type].to_csv(
            output_file_name.replace(".csv", f"_{entry_type}.csv"),
            index=False,
            quoting=csv.QUOTE_ALL,
            date_format="%d.%m.%Y",
        )

    pandas_df.info()
    return dbt_df
