"""Render timestreams using Plotly."""

try:
    import plotly.graph_objects as go
    import plotly.io as pio
    from plotly.subplots import make_subplots

    def _require_plotly() -> None:
        pass

except ImportError:

    def _require_plotly() -> None:
        raise ImportError(
            "Plotly is not installed. Install it with `pip install plotly`."
        )


import itertools
from dataclasses import dataclass
from typing import Optional

from ._timestream import Timestream


@dataclass
class Plot(object):
    """Configuration for a single plot to render.

    Attributes:
        stream: The Timestream to render.
        name: The name of the plot to render. Defaults to `Result` if not set.
    """

    stream: Timestream
    name: Optional[str] = None


def render(
    *args: Plot, theme: Optional[str] = None, title_text: Optional[str] = None
) -> None:
    """Render one or more plots."""
    _require_plotly()

    if theme is None:
        theme = "plotly"

    template = pio.templates[theme]

    fig = make_subplots(rows=len(args), cols=1)

    next_color = itertools.cycle(template.layout.colorway)
    color_map = {}

    for row, plot in enumerate(args, 1):
        data = plot.stream.to_pandas()
        name = plot.name or f"Result {row}"

        for key in data["_key"].unique():
            # TODO: Change markers when colorway cycles.
            # TODO: Use different markers for different entity types.
            # TODO: Render categorical values.
            # TODO: Warn if number of points / keys exceeds some threshold.
            new = False
            if key not in color_map:
                new = True
                color_map[key] = next(next_color)
            color = color_map[key]

            points = data[data["_key"] == key]

            # TODO: Render arrows for continuous timestreams.
            # TODO: Render discontinuity at window boundaries.
            fig.append_trace(
                go.Scatter(
                    x=points["_time"],
                    y=points["result"],
                    mode="markers+lines" if plot.stream.is_continuous else "markers",
                    marker=dict(
                        color=color,
                    ),
                    line={"shape": "hv"},
                    legendgroup=key,
                    showlegend=new,
                    name=str(key),
                ),
                row=row,
                col=1,
            )
        fig["layout"][f"xaxis{row}"]["title"] = "Time"
        fig["layout"][f"yaxis{row}"]["title"] = name

    fig.update_layout(height=300 * len(args), width=600, title_text=title_text)

    fig.show()


def _assign_colors(next_color, color_map, keys):
    colors = []
    for key in keys:
        if key not in color_map:
            color_map[key] = next(next_color)
        colors.append(color_map[key])
    return colors
