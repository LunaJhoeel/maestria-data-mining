import polars as pl

df = pl.read_parquet("talkingdata-adtracking-fraud-detection/train.parquet")

print(df)