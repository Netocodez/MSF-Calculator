# msf_common.py

import pandas as pd
import numpy as np

OLD_AGE_BANDS = [
    "<1","1-4","5-9","10-14","15-19","20-24","25-29",
    "30-34","35-39","40-44","45-49","50-54","55-59",
    "60-64","65+"
]

NEW_AGE_BANDS = [
    "<1",
    "1-4",
    "5-9",
    "10-14",
    "15-19",
    "20-24",
    "25-49",
    "50+"
]


OLD_BINS = [
    -np.inf,1,4,9,14,19,24,29,
    34,39,44,49,54,59,64,np.inf
]

NEW_BINS = [
    -np.inf,
    1,
    4,
    9,
    14,
    19,
    24,
    49,
    np.inf
]


def add_agebands(df):

    df["Age Band"] = pd.cut(
        df["Age"],
        bins=OLD_BINS,
        labels=OLD_AGE_BANDS,
        right=True
    )

    df["Age Band New"] = pd.cut(
        df["Age"],
        bins=NEW_BINS,
        labels=NEW_AGE_BANDS,
        right=True
    )

    return df


def standardize_pivot(df, bands):

    df = df.reindex(columns=bands, fill_value=0)
    df = df.reindex(["M","F"], fill_value=0)
    df.rename(
        index={
            "M":"Male",
            "F":"Female"
        },
        inplace=True
    )

    df["Total"] = df.sum(axis=1)

    return df