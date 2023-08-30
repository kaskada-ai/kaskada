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
source.run().to_pandas()
```