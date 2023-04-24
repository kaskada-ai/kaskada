from bokeh.plotting import figure, output_file, show
from bokeh.models import ColumnDataSource, Span, Segment
from bokeh.models.tools import HoverTool, CrosshairTool
from bokeh.transform import factor_cmap, factor_mark
from bokeh.layouts import column

class Timeline:
    def __init__(self, name, label = None, continuous = None):
        self.name = name
        self.label = label or name
        self.continuous = continuous or False

def plot_timelines(df, time, entity, timelines, width = None, height = None):
    df_by_entity = df.groupby([entity])
    df['last_{}'.format(time)] = df_by_entity[time].shift(1)
    for timeline in timelines:
        if timeline.continuous:
            df['last_{}'.format(timeline.name)] = df_by_entity[timeline.name].shift(1)

    ENTITIES = df[entity].unique()
    MARKERS = ['hex', 'circle_x', 'triangle'] * int(len(ENTITIES) / 3 + 1)

    source = ColumnDataSource(df)

    width = width or 600
    height = height or 250
    plots = []
    x_range = None
    for timeline in timelines:
        plot = figure(title=timeline.label, width=width, height=height, x_axis_type="datetime")
        if x_range:
            plot.x_range = x_range
        else:
            x_range = plot.x_range

        plot.scatter(
            x=time,
            y=timeline.name,
            source=source,
            legend_group=entity,
            marker=factor_mark(entity, MARKERS, ENTITIES),
            color=factor_cmap(entity, 'Category10_3', ENTITIES),
        )
        plot.add_tools(HoverTool(
             tooltips=[
                (entity, '@{}'.format(entity)),
                ('time', '@{}{{%F}}'.format(time)),
                ('value', '@{}'.format(timeline.name)),
            ],
            formatters={
                '@{}'.format(time): 'datetime', # use 'datetime' formatter for 'time' field
            }))
        plot.legend.location = 'top_left'

        if timeline.continuous:
            last_time = 'last_{}'.format(time)
            last_value = 'last_{}'.format(timeline.name)
            plot.add_glyph(source, Segment(
                x0=last_time, x1=time, y0=last_value, y1=last_value,
                line_color=factor_cmap(entity, 'Category10_3', ENTITIES)))
            plot.add_glyph(source, Segment(
                x0=time, x1=time, y0=last_value, y1=timeline.name,
                line_dash='dotted',
                line_color=factor_cmap(entity, 'Category10_3', ENTITIES)))
        plots.append(plot)

    return column(plots)