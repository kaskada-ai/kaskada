---
file_format: mystnb
kernelspec:
  name: python3
  display_name: Python 3
mystnb:
  execution_mode: cache
---

# Quick Start

```{todo}

Write the quick start.
```

```{code-cell}
import kaskada as kd
kt.init_session()
content = "\n".join(
    [
        "time,key,m,n",
        "1996-12-19T16:39:57-08:00,A,5,10",
        "1996-12-19T16:39:58-08:00,B,24,3",
        "1996-12-19T16:39:59-08:00,A,17,6",
        "1996-12-19T16:40:00-08:00,A,,9",
        "1996-12-19T16:40:01-08:00,A,12,",
        "1996-12-19T16:40:02-08:00,A,,",
    ]
)
source = kt.sources.CsvString(content, time_column_name="time", key_column_name="key")
source.run().to_pandas()
```