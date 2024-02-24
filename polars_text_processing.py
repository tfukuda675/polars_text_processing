import sys
import os
import polars as pl


df = pl.read_csv("sample.dspf",separator="\n",has_header=False,new_columns=["dspf"])

with pl.Config() as cfg:
    cfg.set_tbl_rows(100)
    cfg.set_fmt_str_lengths(100)
    print(df)

df = df.with_row_count()

with pl.Config() as cfg:
    cfg.set_tbl_rows(100)
    cfg.set_fmt_str_lengths(100)
    print(df)

df = df.with_columns(pl.col("dspf").str.split(by=" ").alias("splited_dspf"))

with pl.Config() as cfg:
    cfg.set_tbl_rows(100)
    cfg.set_fmt_str_lengths(100)
    print(df)

df = df.drop("dspf").explode(pl.exclude('row_nr'))

with pl.Config() as cfg:
    cfg.set_tbl_rows(100)
    cfg.set_fmt_str_lengths(100)
    print(df)


df = df.with_columns(pl.col("splited_dspf").str.replace_all("regcontrol_top","regcontrol_fukuda_top"))

with pl.Config() as cfg:
    cfg.set_tbl_rows(100)
    cfg.set_fmt_str_lengths(100)
    print(df)

df = df.group_by("row_nr", maintain_order=True).agg(pl.col(["splited_dspf"]))

with pl.Config() as cfg:
    cfg.set_tbl_rows(100)
    cfg.set_fmt_str_lengths(100)
    print(df)

df = df.with_columns(pl.col("splited_dspf").list.join(" "))

with pl.Config() as cfg:
    cfg.set_tbl_rows(100)
    cfg.set_fmt_str_lengths(100)
    print(df)


#####
#####
#####


df = pl.read_csv("sample.dspf",separator="\n",has_header=False,new_columns=["dspf"])

df = df.lazy()
df = df.with_row_count()
df = df.with_columns(pl.col("dspf").str.split(by=" ").alias("splited_dspf"))
df = df.drop("dspf").explode(pl.exclude('row_nr'))
df = df.with_columns(pl.col("splited_dspf").str.replace_all("regcontrol_top","regcontrol_fukuda_top"))
df = df.group_by("row_nr", maintain_order=True).agg(pl.col(["splited_dspf"]))
df = df.with_columns(pl.col("splited_dspf").list.join(" "))
df = df.collect()

with pl.Config() as cfg:
    cfg.set_tbl_rows(100)
    cfg.set_fmt_str_lengths(100)
    print(df)

