import operator
import pprint

from domonic.html import pre, table, td, th, tr
from domonic.utils import Utils

import kaskada.kaskada.v1alpha.view_service_pb2 as view_pb

from .formatters_helpers import (
    appendChildren,
    appendHtmlObjTableRowIfAttrExists,
    convert_fenl_datatype,
    get_datetime,
    get_properties,
    get_schema_dataframe,
    html_obj_table_row,
    html_table_row,
)

"""
NOTES:

Use `hasattr(obj, prop) to test if an object has a property`
Use `obj.HasField(prop) to test if an object has a specific one-of`
"""


def appendTimes(parent, obj):
    if hasattr(obj, "create_time"):
        create_time = get_datetime(getattr(obj, "create_time")).isoformat()
        parent.appendChild(html_obj_table_row("create_time", create_time))
    if hasattr(obj, "update_time"):
        update_time = get_datetime(getattr(obj, "update_time")).isoformat()
        parent.appendChild(html_obj_table_row("update_time", update_time))


def get_analysis_and_diagnostics_tables(obj):
    can_execute = (
        obj.analysis.can_execute if hasattr(obj, "analysis") else obj.can_execute
    )
    details = table(
        html_obj_table_row("can_execute", str(can_execute)), _class="kda_table"
    )
    diagnostics = None
    has_errors = False
    if (
        hasattr(obj, "fenl_diagnostics")
        and len(obj.fenl_diagnostics.fenl_diagnostics) > 0
    ):
        details.appendChild(
            html_obj_table_row("fenl_diagnostics", "(see Diagnostics tab)")
        )
        diagnostics, has_errors = get_fenl_diagnostics_html_and_has_errors(
            obj.fenl_diagnostics
        )
    return can_execute, has_errors, details, diagnostics


def get_fenl_diagnostics_html_and_has_errors(obj):
    diagnostics = table(_class="kda_table")
    for i in range(len(obj.fenl_diagnostics)):
        fenl_diag = obj.fenl_diagnostics[i]
        diagnostics.appendChild(html_obj_table_row(i, fenl_diag.formatted))
    return diagnostics, obj.num_errors > 0


def get_generic_object_html_and_schema_df(obj):
    props = get_properties(obj)
    obj_type = type(obj).__name__.lower()

    # try to pop off obj_id and obj_name first
    obj_name_key = f"{obj_type}_name"
    obj_name = props.pop(obj_name_key, None)

    # also remove create & update time from props
    props.pop("create_time", None)
    props.pop("update_time", None)

    rows = [
        # first row is item name
        html_obj_table_row(obj_name_key, obj_name),
        # stop showing item ids since they aren't actionable by the user
        # html_obj_table_row(obj_id_key, obj_id),
    ]

    # add remainder of fields
    for key in props:
        rows.append(html_obj_table_row(key, props[key]))

    schema_df = get_schema_dataframe(obj)
    details = table(_class="kda_table")
    appendChildren(details, rows)
    if schema_df is not None:
        details.appendChild(html_obj_table_row("schema", "(see Schema tab)"))
    appendTimes(details, obj)
    return details, schema_df


def get_materialization_html_and_schema_df(obj):
    schema_df = get_schema_dataframe(obj)
    details = table(_class="kda_table")
    appendHtmlObjTableRowIfAttrExists(details, obj, "materialization_name")
    appendHtmlObjTableRowIfAttrExists(details, obj, "query")

    if hasattr(obj, "destination") and obj.HasField("destination"):
        destination = table(_class="kda_table")
        details.appendChild(html_table_row("destination", destination))
        if hasattr(obj.destination, "redis_a_i") and obj.destination.HasField(
            "redis_a_i"
        ):
            redis_a_i = table(_class="kda_table")
            appendHtmlObjTableRowIfAttrExists(
                redis_a_i, obj.destination.redis_a_i, "host"
            )
            appendHtmlObjTableRowIfAttrExists(
                redis_a_i, obj.destination.redis_a_i, "port"
            )
            appendHtmlObjTableRowIfAttrExists(
                redis_a_i, obj.destination.redis_a_i, "db"
            )
            destination.appendChild(html_table_row("redis_a_i", redis_a_i))

    if hasattr(obj, "slice"):
        details.appendChild(html_table_row("slice", get_slice_request_html(obj.slice)))

    if schema_df is not None:
        details.appendChild(html_obj_table_row("schema", "(see Schema tab)"))

    appendTimes(details, obj)
    return details, schema_df


def get_metrics_html(obj):
    metrics = table(_class="kda_table")

    time_preparing = (
        obj.time_preparing.ToMilliseconds() if hasattr(obj, "time_preparing") else 0
    )
    metrics.appendChild(html_table_row("time_preparing", f"{time_preparing / 1000}s"))

    time_computing = (
        obj.time_computing.ToMilliseconds() if hasattr(obj, "time_computing") else 0
    )
    metrics.appendChild(html_table_row("time_computing", f"{time_computing / 1000}s"))

    metrics.appendChild(html_table_row("output_files", obj.output_files))
    return metrics


def get_raw_html(obj):
    return pre(pprint.pformat(obj).replace("\\n", ""))


def get_request_details_table_row_if_exists(obj):
    if hasattr(obj, "request_details") and hasattr(obj.request_details, "request_id"):
        nested_table = table(
            html_obj_table_row("request_id", obj.request_details.request_id),
            _class="kda_table",
        )
        return html_table_row("request_details", nested_table)
    return None


def get_result_type_and_schema_df(obj):
    result_type = Utils.escape(convert_fenl_datatype(obj))
    schema_df = None
    if obj.HasField("struct"):
        schema_df = get_schema_dataframe(obj.struct)
        if schema_df is not None:
            result_type += " (see Schema tab)"
    return result_type, schema_df


def get_slice_request_html(obj):
    details = table(_class="kda_table")
    if obj is not None and obj.HasField("percent"):
        details.appendChild(html_obj_table_row("percent", obj.percent.percent))
    else:
        details.appendChild(html_obj_table_row("None", "(full dataset used for query)"))
    return details


def get_table_html_and_schema_df(obj):
    schema_df = get_schema_dataframe(obj)
    details = table(_class="kda_table")
    appendHtmlObjTableRowIfAttrExists(details, obj, "table_name")
    appendHtmlObjTableRowIfAttrExists(details, obj, "entity_key_column_name")
    appendHtmlObjTableRowIfAttrExists(details, obj, "time_column_name")
    appendHtmlObjTableRowIfAttrExists(details, obj, "grouping_id")
    if obj.HasField("subsort_column_name"):
        details.appendChild(
            html_obj_table_row("subsort_column_name", obj.subsort_column_name.value)
        )

    if hasattr(obj, "table_source") and obj.HasField("table_source"):
        if hasattr(obj.table_source, "iceberg") and obj.table_source.HasField(
            "iceberg"
        ):
            iceberg = table(_class="kda_table")
            appendHtmlObjTableRowIfAttrExists(
                iceberg, obj.table_source.iceberg, "table_name"
            )
            appendHtmlObjTableRowIfAttrExists(
                iceberg, obj.table_source.iceberg, "namespace"
            )
            if hasattr(
                obj.table_source.iceberg, "config"
            ) and obj.table_source.iceberg.HasField("config"):
                config = table(_class="kda_table")
                if hasattr(
                    obj.table_source.iceberg.config, "glue"
                ) and obj.table_source.iceberg.config.HasField("glue"):
                    glue = table(_class="kda_table")
                    if hasattr(
                        obj.table_source.iceberg.config.glue, "warehouse"
                    ) and obj.table_source.iceberg.config.glue.HasField("warehouse"):
                        warehouse = table(_class="kda_table")
                        appendHtmlObjTableRowIfAttrExists(
                            warehouse,
                            obj.table_source.iceberg.config.glue.warehouse,
                            "bucket",
                        )
                        appendHtmlObjTableRowIfAttrExists(
                            warehouse,
                            obj.table_source.iceberg.config.glue.warehouse,
                            "region",
                        )
                        appendHtmlObjTableRowIfAttrExists(
                            warehouse,
                            obj.table_source.iceberg.config.glue.warehouse,
                            "prefix",
                        )
                        glue.appendChild(html_table_row("warehouse", warehouse))
                    config.appendChild(html_table_row("glue", glue))
                iceberg.appendChild(html_table_row("config", config))
            details.appendChild(html_table_row("iceberg", iceberg))

    appendHtmlObjTableRowIfAttrExists(details, obj, "version")
    if schema_df is not None:
        details.appendChild(html_obj_table_row("schema", "(see Schema tab)"))
    appendTimes(details, obj)
    return details, schema_df


def get_query_html(obj):
    details = table(_class="kda_table")
    appendHtmlObjTableRowIfAttrExists(details, obj, "query_id")
    details.appendChild(html_table_row("create_time", obj.create_time.ToJsonString()))
    appendHtmlObjTableRowIfAttrExists(details, obj, "expression")
    if hasattr(obj, "data_token_id"):
        details.appendChild(html_table_row("data_token", obj.data_token_id.value))
    if hasattr(obj, "views"):
        views = table(_class="kda_table")
        views.appendChild(tr(th("name"), th("expression")))
        obj.views.sort(key=operator.attrgetter("view_name"))
        for view in obj.views:
            views.appendChild(tr(td(view.view_name), td(view.expression)))
        details.appendChild(html_table_row("views", views))
    if hasattr(obj, "result_behavior"):
        result_behavior = "Unspecified"
        if obj.result_behavior == 1:
            result_behavior = "All Results"
        elif obj.result_behavior == 2:
            result_behavior = "Final Results"
        elif obj.result_behavior == 3:
            result_behavior = "Final Results at Time"
        details.appendChild(html_table_row("result_behavior", result_behavior))
    if hasattr(obj, "slice"):
        details.appendChild(html_table_row("slice", obj.slice))
    if hasattr(obj, "limits"):
        details.appendChild(html_table_row("limits", obj.limits))

    return details


def get_view_html_and_schema_df(obj: view_pb.View):
    details = table(_class="kda_table")
    appendHtmlObjTableRowIfAttrExists(details, obj, "view_name")
    appendHtmlObjTableRowIfAttrExists(details, obj, "expression")
    schema_df = None
    result_type, schema_df = get_result_type_and_schema_df(obj.result_type)
    if result_type is not None and result_type != "":
        details.appendChild(html_obj_table_row("result_type", result_type))

    appendTimes(details, obj)
    return details, schema_df
