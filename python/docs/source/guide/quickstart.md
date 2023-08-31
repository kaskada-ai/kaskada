---
file_format: mystnb
kernelspec:
  name: python3
  display_name: Python 3
mystnb:
  execution_mode: cache
---

# Quick Start

This shows the bare minimum needed to get started with Kaskada.

## Install

Install the latest version.
This uses `kaskdaa>=0.6.0-a.0` to ensure the pre-release version is installed.

```
pip install kaskada>=0.6.0-a.0
```

See the section on [installation](./installation.md) to learn more about installing Kaskada.

## Write a query

The following Python code imports the Kaskada library, creates a session, and loads some CSV data.
It then runs a query to produce a Pandas DataFrame.

```{code-cell}
import kaskada as kd
kd.init_session()
content = "\n".join(
    [
        "time,key,m,n",
        "1996-12-19T16:39:57,A,5,10",
        "1996-12-19T16:39:58,B,24,3",
        "1996-12-19T16:39:59,A,17,6",
        "1996-12-19T16:40:00,A,,9",
        "1996-12-19T16:40:01,A,12,",
        "1996-12-19T16:40:02,A,,",
    ]
)
source = kd.sources.CsvString(content, time_column="time", key_column="key")
source.select("m", "n").extend({"sum_m": source.col("m").sum() }).to_pandas()
```