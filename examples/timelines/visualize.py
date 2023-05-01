from bokeh.models import CategoricalAxis, ColumnDataSource, Range1d, Span, Segment, NormalHead, Arrow
from bokeh.models.tools import HoverTool
from bokeh.palettes import Category10_3
from bokeh.plotting import figure, output_file, show
from bokeh.transform import factor_cmap, factor_mark
from bokeh.layouts import column
import pandas as pd
import numpy as np

PALETTE = Category10_3

class Data:
    def __init__(self, df, time = None, entity = None):
        self.df = df
        self.time = time or '_time'
        self.entity = entity or '_key'
        self.df_by_entity = df.groupby([self.entity])
        self.prev_names = {}

        self.entities = df[self.entity].unique()

        self.min_time = df[self.time].min()
        self.max_time = df[self.time].max()
        self.source = None

    def prev(self, name):
        if name in self.prev_names:
            return self.prev_names[name]
        else:
            # TODO: Handle the case where `prev_foo` is already in the dataframe?
            prev_name = 'prev_{}'.format(name)
            self.prev_names[name] = prev_name
            self.df[prev_name] = self.df_by_entity[name].shift(1)
            return prev_name

    def last_value(self, name):
        return pd.DataFrame({
            'entity': self.df_by_entity[self.entity].tail(1),
            'time': self.df_by_entity[self.time].tail(1),
            'value': self.df_by_entity[name].tail(1),
        })

    def get_or_create_source(self):
        if not self.source:
            self.source = ColumnDataSource(self.df)
        return self.source

class Timeline:
    def __init__(self, name, data = None, label = None, continuous = False):
        """Create a timeline.

        Parameters:
          - name: The name of the column containing the values.
          - data: (Optional) the data for the timeline. If not set, defaults to
                  the data for the plots.
          - label: The label to show for the expression. Defaults to the name.
          - continuous: Whether the timeline is continuous. Defaults to false.
        """
        self.name = name
        if isinstance(data, pd.DataFrame):
            data = Data(data)
        self.data = data
        self.label = label or name
        self.continuous = continuous

    def __repr__(self):
        return f"Timeline({self.name}, {self.data}, {self.label}, {self.continuous})"

    def plot(self, data, x_range, markers, height, width):
        data = self.data or data
        if not data:
            raise TypeError('missing data for timeline "{}"'.format(self.name))

        plot = None
        if data.df[self.name].dtype == 'object':
            values = data.df[self.name].unique()
            values.sort()

            plot = figure(title=self.label, width=width, height=height, x_axis_type="datetime", x_range=x_range, y_range=values)
        else:
            plot = figure(title=self.label, width=width, height=height, x_axis_type="datetime", x_range=x_range)

        source = data.get_or_create_source()
        plot.scatter(
            x=data.time,
            y=self.name,
            source=source,
            legend_group=data.entity,
            marker=factor_mark(data.entity, markers, data.entities),
            color=factor_cmap(data.entity, PALETTE, data.entities),
        )

        plot.add_tools(HoverTool(
             tooltips=[
                ('entity', f'(@{data.time}{{%F}}, @{data.entity}, @{self.name})')
            ],
            formatters={
                '@{}'.format(data.time): 'datetime', # use 'datetime' formatter for 'time' field
            },
            # Would like to use `vline` to show all intersected points.
            # However, it has problems with the vertical segments (and other things) and ends
            # up showing *many* hovers. Leaving off for now.
            #
            # https://github.com/bokeh/bokeh/issues/9087 - May be able to fix once this issue
            # is addressed, by providing a custom "filter".
            mode = "mouse"))
        plot.legend.location = 'top_left'

        if self.continuous:
            last_time = data.prev(data.time)
            last_value = data.prev(self.name)
            # Draw horizontal lines between the last change time and the next change time.
            plot.add_glyph(source, Segment(
                x0=last_time, x1=data.time, y0=last_value, y1=last_value,
                line_color=factor_cmap(data.entity, PALETTE, data.entities)))
            # Draw vertical dashed lines between last value and the new value (at new time).
            plot.add_glyph(source, Segment(
                x0=data.time, x1=data.time, y0=last_value, y1=self.name,
                line_dash='dotted',
                line_color=factor_cmap(data.entity, PALETTE, data.entities)))

            # Print the arrows for continuity.
            entities = list(data.entities)
            ends = data.last_value(self.name)
            for index, row in ends.iterrows():
                index = entities.index(row['entity'])
                color = PALETTE[index % len(PALETTE)]
                plot.add_layout(Arrow(
                    x_start=row['time'], y_start=row['value'], x_end=x_range.end, y_end=row['value'],
                    end=NormalHead(line_color=color, fill_color=color, size=5),
                    line_color=color
                ))
        return plot


def plot_timelines(timelines, data = None, width = None, height = None):
    """Plot 1 or more timelines.

    Parameters:
        - timelines: The timelines to plot.
        - data: (Optional) the data to use for timelines without specific data.
          Defaults to None.
        - width: (Optional) the width of each plot. Defaults to 600px.
        - height: (Optional) the height of each plot. Defaults to 250px.
    """
    width = width or 600
    height = height or 250

    max_entities = None
    min_time = None
    max_time = None
    if isinstance(data, pd.DataFrame):
       data = Data(data)
    if isinstance(data, Data):
        max_entities = len(data.entities)
        min_time = data.min_time
        max_time = data.max_time
    for timeline in timelines:
        if isinstance(timeline.data, Data):
            max_entities = max(max_entities, len(timeline.data.entities))
            if min_time:
                min_time = timeline.data.min_time
            else:
                min_time = min(min_time, timeline.data.min_time)
            max_time = max(max_time, timeline.data.max_time)

        # Make sure the "previous" values are defined before we create the source.
        timeline_data = timeline.data or data
        if timeline.continuous:
            timeline_data.prev(timeline_data.time)
            timeline_data.prev(timeline.name)

    MARKERS = ['circle', 'triangle', 'diamond', 'square'] * int(max_entities / 4 + 1)
    x_range_padding = 0.03 * (max_time - min_time)
    x_range = Range1d(min_time - x_range_padding, max_time + x_range_padding)

    plots = []
    for timeline in timelines:
        plot = timeline.plot(data, x_range, MARKERS, height, width)
        plots.append(plot)

    return column(plots)