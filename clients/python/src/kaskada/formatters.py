# pylint: skip-file

import builtins
import inspect
import io
import logging
import pprint
import sys

import html5lib
import pkg_resources
from domonic.ext.html5lib_ import getTreeBuilder
from domonic.html import a, button, div, pre, script, style, table, td, th, tr
from domonic.utils import Utils

import kaskada.formatters_shared as shared
from kaskada.formatters_helpers import (
    appendChildIfNotNone,
    appendHtmlObjTableRowIfAttrExists,
    get_classname,
    get_properties,
    get_schema_dataframe,
    html_obj_id_row,
    html_obj_table_row,
    html_table_row,
)

_LOGGER = logging.getLogger(__name__)


def in_ipython() -> bool:
    """
    Checks to see if running in an iPython environment
    """
    return getattr(builtins, "__IPYTHON__", False)


def get_query_response_content(resp_obj):
    resultsExist = False
    details = table(_class="kda_table")

    state_enum_descriptor = resp_obj.DESCRIPTOR.enum_types_by_name.get("State")
    state = state_enum_descriptor.values_by_number.get(resp_obj.state).name
    details.appendChild(html_table_row("state", state.replace("STATE_", "")))

    if hasattr(resp_obj, "query_id"):
        details.appendChild(html_table_row("query_id", resp_obj.query_id))

    if hasattr(resp_obj, "metrics") and resp_obj.state > 1:
        metrics = shared.get_metrics_html(resp_obj.metrics)
        details.appendChild(html_table_row("metrics", metrics))

    if hasattr(resp_obj, "file_results"):
        nested_table = table(_class="kda_table")
        details.appendChild(html_table_row("file_results", nested_table))
        nested_table.appendChild(tr(th("index"), th("path")))
        for i in range(len(resp_obj.file_results.paths)):
            path = resp_obj.file_results.paths[i]
            appendChildIfNotNone(nested_table, tr(td(pre(i)), td(a(path, _href=path))))
        resultsExist = True
    elif hasattr(resp_obj, "redis_bulk"):
        nested_table = table(_class="kda_table")
        details.appendChild(html_table_row("redis_bulk", nested_table))
        nested_table.appendChild(tr(th("index"), th("path")))
        for i in range(len(resp_obj.redis_bulk.paths)):
            path = resp_obj.redis_bulk.paths[i]
            appendChildIfNotNone(nested_table, tr(td(pre(i)), td(a(path, _href=path))))
        resultsExist = True

    (
        can_execute,
        has_errors,
        nested_details,
        diagnostics,
    ) = shared.get_analysis_and_diagnostics_tables(resp_obj)
    details.appendChild(html_table_row("analysis", nested_details))

    schema = None
    if resp_obj.HasField("analysis"):
        schema_df = get_schema_dataframe(resp_obj.analysis)
        if schema_df is not None:
            schema = convert_df_to_domonic(schema_df)
            details.appendChild(html_obj_table_row("schema", "(see Schema tab)"))

    if resultsExist and hasattr(resp_obj, "config"):
        appendHtmlObjTableRowIfAttrExists(details, resp_obj.config, "data_token_id")
        if resp_obj.config.HasField("slice_request"):
            details.appendChild(
                html_table_row(
                    "slice_request",
                    shared.get_slice_request_html(resp_obj.config.slice_request),
                )
            )
        else:
            details.appendChild(
                html_table_row("slice_request", shared.get_slice_request_html(None))
            )

    appendChildIfNotNone(
        details, shared.get_request_details_table_row_if_exists(resp_obj)
    )

    return has_errors, {
        "Details": details,
        "Schema": schema,
        "Diagnostics": diagnostics,
    }


# handles Create and Get responses for Table, View, Materialization
# also handles LoadDataResponse
def generic_response_html_formatter(obj):
    output_custom_css_and_javascript_if_output_wrapped_in_iframe()

    details = table(_class="kda_table")
    schema_df = None
    if hasattr(obj, "table") and obj.HasField("table"):
        table_details, schema_df = shared.get_table_html_and_schema_df(obj.table)
        details.appendChild(html_table_row("table", table_details))
    elif hasattr(obj, "query"):
        query_details = shared.get_query_html(obj.query)
        details.appendChild(html_table_row("query", query_details))
    elif hasattr(obj, "view") and obj.HasField("view"):
        view_details, schema_df = shared.get_view_html_and_schema_df(obj.view)
        details.appendChild(html_table_row("view", view_details))
    elif hasattr(obj, "materialization") and obj.HasField("materialization"):
        mat_details, schema_df = shared.get_materialization_html_and_schema_df(
            obj.materialization
        )
        details.appendChild(html_table_row("materialization", mat_details))
    else:
        appendHtmlObjTableRowIfAttrExists(details, obj, "data_token_id")

    # pull of analysis
    diagnostics = None
    has_errors = False
    if hasattr(obj, "analysis"):
        (
            can_execute,
            has_errors,
            nested_details,
            diagnostics,
        ) = shared.get_analysis_and_diagnostics_tables(obj.analysis)
        details.appendChild(html_table_row("analysis", nested_details))

    appendChildIfNotNone(details, shared.get_request_details_table_row_if_exists(obj))

    schema = convert_df_to_domonic(schema_df) if schema_df is not None else None
    title_to_set_active = "Diagnostics" if has_errors else "Details"
    return str(
        tab_panel(
            [
                ("Details", details),
                ("Schema", schema),
                ("Diagnostics", diagnostics),
                ("Raw", shared.get_raw_html(obj)),
            ],
            title_to_set_active=title_to_set_active,
        )
    )


def query_resource_response_html_formatter(obj):
    output_custom_css_and_javascript_if_output_wrapped_in_iframe()

    details = table(_class="kda_table")
    query_details = shared.get_query_html(obj.query)
    details.appendChild(html_table_row("query", query_details))

    appendChildIfNotNone(details, shared.get_request_details_table_row_if_exists(obj))

    return str(
        tab_panel(
            [
                ("Details", details),
                ("Raw", shared.get_raw_html(obj.query)),
            ],
            title_to_set_active="Details",
        )
    )


# handles Table, View, Materialization, and other generic objects
def generic_object_html_formatter(obj):
    output_custom_css_and_javascript_if_output_wrapped_in_iframe()

    if get_classname(type(obj)) == "kaskada.kaskada.v1alpha.table_service_pb2.Table":
        details, schema_df = shared.get_table_html_and_schema_df(obj)
    elif get_classname(type(obj)) == "kaskada.kaskada.v1alpha.view_service_pb2.View":
        details, schema_df = shared.get_view_html_and_schema_df(obj)
    elif (
        get_classname(type(obj))
        == "kaskada.kaskada.v1alpha.materialization_service_pb2.Materialization"
    ):
        details, schema_df = shared.get_materialization_html_and_schema_df(obj)
    else:
        details, schema_df = shared.get_generic_object_html_and_schema_df(obj)

    schema = convert_df_to_domonic(schema_df) if schema_df is not None else None
    return str(
        tab_panel(
            [
                ("Details", details),
                ("Schema", schema),
                ("Raw", shared.get_raw_html(obj)),
            ]
        )
    )


# handles the Schema object
def schema_html_formatter(obj):
    output_custom_css_and_javascript_if_output_wrapped_in_iframe()

    schema_df = get_schema_dataframe(obj)
    schema = convert_df_to_domonic(schema_df) if schema_df is not None else None

    return str(tab_panel([("Schema", schema), ("Raw", shared.get_raw_html(obj))]))


# handles the DataType object
def data_type_html_formatter(obj):
    output_custom_css_and_javascript_if_output_wrapped_in_iframe()

    result_type, schema_df = shared.get_result_type_and_schema_df(obj)
    title_to_set_active = "Schema" if schema_df is not None else ""
    schema = convert_df_to_domonic(schema_df) if schema_df is not None else None

    return str(
        tab_panel(
            [
                ("Result Type", pre(result_type)),
                ("Schema", schema),
                ("Raw", shared.get_raw_html(obj)),
            ],
            title_to_set_active,
        )
    )


# handles Delete responses for Table, View, Materialization
def response_delete_html_formatter(del_resp):
    output_custom_css_and_javascript_if_output_wrapped_in_iframe()

    request_details = shared.get_request_details_table_row_if_exists(del_resp)

    del_resp_type = type(del_resp).__name__
    del_obj = del_resp_type.replace("Response", "")

    details = table(_class="kda_table")
    details.appendChild(html_obj_table_row(del_obj, "success"))
    details.appendChild(request_details)

    return str(
        tab_panel([("Details", details), ("Raw", pre(pprint.pformat(del_resp)))])
    )


# handles List responses for Tables, Views, Materializations
def response_list_html_formatter(list_resp):
    output_custom_css_and_javascript_if_output_wrapped_in_iframe()

    list_resp_type = type(list_resp).__name__
    list_key = list_resp_type.replace("Response", "").replace("List", "").lower()

    details = table(_class="kda_table")

    # after pulling the request_id, only the item list should remain
    props = get_properties(list_resp)
    if "request_details" in props:
        props.pop("request_details")
    if len(props) == 1:
        list_value = getattr(list_resp, list_key)
        list_props = props[list_key]
        list_type = type(list_value)
        nested_table = table(_class="kda_table")
        details.appendChild(html_table_row(list_key, nested_table))
        nested_table.appendChild(tr(th("index"), th("name")))
        if (
            get_classname(list_type)
            == "google._upb._message.RepeatedCompositeContainer"
        ):
            for i in range(len(list_value)):
                appendChildIfNotNone(
                    nested_table, html_obj_id_row(str(i), list_value[i], list_props[i])
                )
    else:  # when list is empty
        details.appendChild(html_table_row(list_key, Utils.escape("<empty>")))

    appendChildIfNotNone(
        details, shared.get_request_details_table_row_if_exists(list_resp)
    )

    return str(
        tab_panel([("Details", details), ("Raw", shared.get_raw_html(list_resp))])
    )


def response_list_queries_html_formatter(list_resp):
    output_custom_css_and_javascript_if_output_wrapped_in_iframe()

    list_resp_type = type(list_resp).__name__
    list_key = list_resp_type.replace("Response", "").replace("List", "").lower()

    details = table(_class="kda_table")

    # after pulling the request_id, only the item list should remain
    props = get_properties(list_resp)
    if "request_details" in props:
        props.pop("request_details")
    if len(props) == 1:
        list_value = getattr(list_resp, list_key)
        list_type = type(list_value)
        details.appendChild(tr(th("id"), th("query")))
        if (
            get_classname(list_type)
            == "google._upb._message.RepeatedCompositeContainer"
        ):
            for i in range(len(list_value)):
                details.appendChild(
                    tr(
                        td(str(i)),
                        td(
                            table(
                                tr(td("Query"), td(list_value[i].expression)),
                                tr(
                                    td("Created At"),
                                    td(list_value[i].create_time.ToJsonString()),
                                ),
                                tr(td("Query ID", td(list_value[i].query_id))),
                                tr(
                                    td(
                                        "Data Token",
                                        td(list_value[i].data_token_id.value),
                                    )
                                ),
                            )
                        ),
                    )
                )
    else:  # when list is empty
        details.appendChild(html_table_row(list_key, Utils.escape("<empty>")))

    appendChildIfNotNone(
        details, shared.get_request_details_table_row_if_exists(list_resp)
    )

    return str(
        tab_panel([("Details", details), ("Raw", shared.get_raw_html(list_resp))])
    )


def proto_list_html_formatter(list_resp):
    item_count = len(list_resp)
    if item_count == 0:
        return str(shared.get_raw_html(list_resp))

    output_custom_css_and_javascript_if_output_wrapped_in_iframe()
    details = table(_class="kda_table")

    # peek at the type of items in the list
    item_class = get_classname(type(list_resp[0]))

    if item_class == "kaskada.kaskada.v1alpha.fenl_diagnostics_pb2.FenlDiagnostic":
        for i in range(item_count):
            fenl_diag = list_resp[i]
            details.appendChild(html_obj_table_row(i, fenl_diag.formatted))
    else:
        details.appendChild(tr(th("index"), th("name")))
        for i in range(item_count):
            appendChildIfNotNone(
                details, html_obj_id_row(i, list_resp[i], get_properties(list_resp[i]))
            )

    return str(
        tab_panel([("Details", details), ("Raw", shared.get_raw_html(list_resp))])
    )


def slice_request_html_formatter(obj):
    output_custom_css_and_javascript_if_output_wrapped_in_iframe()
    return str(
        tab_panel(
            [
                ("Details", shared.get_slice_request_html(obj)),
                ("Raw", shared.get_raw_html(obj)),
            ]
        )
    )


def query_metrics_html_formatter(obj):
    output_custom_css_and_javascript_if_output_wrapped_in_iframe()
    return str(
        tab_panel(
            [
                ("Details", shared.get_metrics_html(obj)),
                ("Raw", shared.get_raw_html(obj)),
            ]
        )
    )


def query_response_html_formatter(obj):
    output_custom_css_and_javascript_if_output_wrapped_in_iframe()

    has_errors, content = get_query_response_content(obj)
    title_to_set_active = "Diagnostics" if has_errors else "Details"
    return str(
        tab_panel(
            [
                ("Details", content["Details"]),
                ("Schema", content["Schema"]),
                ("Diagnostics", content["Diagnostics"]),
                ("Raw", shared.get_raw_html(obj)),
            ],
            title_to_set_active=title_to_set_active,
        )
    )


def query_v2_response_html_formatter(obj):
    output_custom_css_and_javascript_if_output_wrapped_in_iframe()

    has_errors, content = get_query_response_content(obj)
    title_to_set_active = "Diagnostics" if has_errors else "Details"
    return str(
        tab_panel(
            [
                ("Details", content["Details"]),
                ("Schema", content["Schema"]),
                ("Diagnostics", content["Diagnostics"]),
                ("Raw", shared.get_raw_html(obj)),
            ],
            title_to_set_active=title_to_set_active,
        )
    )


def fenlmagic_query_result_html_formatter(obj):
    output_custom_css_and_javascript_if_output_wrapped_in_iframe()

    has_errors, content = get_query_response_content(obj.query_response)
    details_table = content["Details"]
    details_table.appendChild(html_obj_table_row("query", obj.query))

    raw = {
        "query": obj.query,
        "query_response": obj.query_response,
    }

    dataframe = None
    if obj.dataframe is not None:
        buffer = io.StringIO()
        obj.dataframe.info(buf=buffer, verbose=False)
        raw["dataframe"] = buffer.getvalue().replace(
            "<class 'pandas.core.frame.DataFrame'>\n", ""
        )
        dataframe = convert_df_to_domonic(obj.dataframe, 10, True)

    title_to_set_active = "Details"
    if has_errors:
        title_to_set_active = "Diagnostics"
    elif dataframe is not None:
        title_to_set_active = "Dataframe"

    return str(
        tab_panel(
            [
                ("Dataframe", dataframe),
                ("Details", content["Details"]),
                ("Schema", content["Schema"]),
                ("Diagnostics", content["Diagnostics"]),
                ("Raw", shared.get_raw_html(raw)),
            ],
            title_to_set_active=title_to_set_active,
        )
    )


def entity_filter_html_formatter(obj):
    output_custom_css_and_javascript_if_output_wrapped_in_iframe()

    details = table(_class="kda_table")
    filter_type = type(obj).__name__
    if filter_type == "EntityPercentFilter":
        details.appendChild(html_obj_table_row("percent", obj.percent))

    return str(tab_panel([("Details", details), ("Raw", shared.get_raw_html(obj))]))


def analysis_html_formatter(obj):
    output_custom_css_and_javascript_if_output_wrapped_in_iframe()

    (
        can_execute,
        has_errors,
        details,
        diagnostics,
    ) = shared.get_analysis_and_diagnostics_tables(obj)
    title_to_set_active = "Diagnostics" if has_errors else "Details"
    return str(
        tab_panel(
            [
                ("Details", details),
                ("Diagnostics", diagnostics),
                ("Raw", shared.get_raw_html(obj)),
            ],
            title_to_set_active=title_to_set_active,
        )
    )


def fenl_diagnostics_html_formatter(obj):
    output_custom_css_and_javascript_if_output_wrapped_in_iframe()

    diagnostics, has_errors = shared.get_fenl_diagnostics_html_and_has_errors(obj)
    details = table(html_table_row("fenl_diagnostics", diagnostics))
    return str(tab_panel([("Details", details), ("Raw", shared.get_raw_html(obj))]))


def convert_df_to_domonic(df, max_rows=None, show_dimensions=False):
    parser = html5lib.HTMLParser(tree=getTreeBuilder())
    return parser.parse(
        df.to_html(max_rows=max_rows, notebook=True, show_dimensions=show_dimensions)
    )


def tab_panel(content_list, title_to_set_active=""):
    """
    Creates a tab-panel for the passed content list.

    Args:
        content_list (list of tuple of (str, Any)): A list of tuples, where the the first value
        in the tuple is the tab title, and the second is the tab contents.

        title_to_set_active str: The name of the tab title to set active when the panel loads. If
        unset, the first tab will be set active.

    Returns:
        domonic.html.div: A div html element wrapping the tabs and content.  Pass this value to
        str() to get the string html output.
    """
    if len(content_list) == 0:
        return None

    tab_links = div(_class="kda_tab")
    root_div = div(tab_links)

    for titleContent in content_list:
        title = titleContent[0]
        content = titleContent[1]
        if content is None:
            continue
        if title_to_set_active == "":
            title_to_set_active = title
        lower_title = title.lower()

        tab_button_class = (
            "kda_tablinks kda_active"
            if title_to_set_active == title
            else "kda_tablinks"
        )
        tab_links.appendChild(
            button(
                title,
                _class=tab_button_class,
                _onclick=f"openTab(event, '{lower_title}')",
            )
        )

        scroll_div = div(_class="kda_scroll_container").html(content)
        content_div = div(_class=f"{lower_title} kda_tabcontent").html(scroll_div)
        content_div.style.display = "block" if title_to_set_active == title else "none"
        root_div.appendChild(content_div)

    return root_div


def output_custom_css_and_javascript():
    """
    Outputs our custom css and javascript to the current cell
    """
    import IPython

    css = pkg_resources.resource_string(__name__, "formatters.css").decode("utf-8")
    js = pkg_resources.resource_string(__name__, "formatters.js").decode("utf-8")
    IPython.core.display.display(IPython.core.display.HTML(str(style(css))))
    IPython.core.display.display(
        IPython.core.display.HTML(str(script(js, _type="text/javascript")))
    )


def cell_output_is_wrapped_in_iframe():
    """
    Based on the environment that the code is running inside of, the cell output may be
    automatically wrapped in an iFrame.
    For example, in google colab: "the output of each cell is hosted in a separate iframe
    sandbox with limited access to the global notebook environment"
    In enviroments like these, we need to include our custom css & javascript in every output.

    Returns:
        bool: True if the environment will automatically wrap the cell output in an iFrame, otherwise False.
    """
    from IPython import get_ipython

    if "google.colab" in str(get_ipython()):
        return True
    else:
        return False


def output_custom_css_and_javascript_if_output_wrapped_in_iframe():
    """
    Outputs our custom css and javascript to the cell output if the output for the cell
    will be wrapped in an iFrame.  (i.e. in google colab)
    """
    if cell_output_is_wrapped_in_iframe():
        output_custom_css_and_javascript()


def try_init():
    try:
        from IPython import get_ipython

        # the following command will throw an exception in non-iPython
        # environments
        html_formatter = get_ipython().display_formatter.formatters["text/html"]
        # dynamically assign formatters to kaskada protobuf types
        mods = sys.modules.copy()
        for key in mods:
            if key.endswith("_grpc"):
                continue
            if key.startswith("kaskada.kaskada"):
                for cls in inspect.getmembers(mods[key], inspect.isclass):
                    classname = get_classname(cls[1])
                    if (
                        classname
                        == "kaskada.kaskada.v1alpha.query_service_pb2.CreateQueryResponse"
                    ):
                        html_formatter.for_type(
                            classname, query_v2_response_html_formatter
                        )
                    elif classname == "kaskada.kaskada.v1alpha.schema_pb2.Schema":
                        html_formatter.for_type(classname, schema_html_formatter)
                    elif classname == "kaskada.kaskada.v1alpha.schema_pb2.DataType":
                        html_formatter.for_type(classname, data_type_html_formatter)
                    elif classname == "kaskada.kaskada.v1alpha.slice_pb2.SliceRequest":
                        html_formatter.for_type(classname, slice_request_html_formatter)
                    elif (
                        classname
                        == "kaskada.kaskada.v1alpha.fenl_diagnostics_pb2.FenlDiagnostics"
                    ):
                        html_formatter.for_type(
                            classname, fenl_diagnostics_html_formatter
                        )
                    elif classname == "kaskada.kaskada.v1alpha.shared_pb2.Analysis":
                        html_formatter.for_type(classname, analysis_html_formatter)
                    elif "Delete" in classname and "Response" in classname:
                        html_formatter.for_type(
                            classname, response_delete_html_formatter
                        )  # generic formatter for Delete responses
                    elif "ListQueriesResponse" in classname:
                        html_formatter.for_type(
                            classname, response_list_queries_html_formatter
                        )
                    elif "List" in classname and "Response" in classname:
                        html_formatter.for_type(
                            classname, response_list_html_formatter
                        )  # generic formatter for List responses
                    elif "Response" in classname:
                        html_formatter.for_type(
                            classname, generic_response_html_formatter
                        )  # generic formatter for other responses
                    else:
                        html_formatter.for_type(
                            classname, generic_object_html_formatter
                        )  # generic formatter for all other objects

        # the following types don't normally exist when the library first loads
        html_formatter.for_type(
            # TODO: find the new class name for this
            "kaskada.kaskada.v1alpha.compute_service_pb2.Metrics",
            query_metrics_html_formatter,
        )
        html_formatter.for_type(
            "kaskada.materialization.MaterializationView", generic_object_html_formatter
        )
        html_formatter.for_type(
            "kaskada.materialization.RedisAIDestination", generic_object_html_formatter
        )
        html_formatter.for_type(
            "kaskada.kaskada.v1alpha.materialization_service_pb2.RedisAI",
            generic_object_html_formatter,
        )
        html_formatter.for_type(
            "kaskada.kaskada.v1alpha.materialization_service_pb2.Destination",
            generic_object_html_formatter,
        )
        html_formatter.for_type(
            # TODO: find the new class name for this
            "kaskada.compute.EntityPercentFilter",
            entity_filter_html_formatter,
        )
        html_formatter.for_type(
            "fenlmagic.QueryResult", fenlmagic_query_result_html_formatter
        )
        html_formatter.for_type(
            "kaskada.query.QueryResource", query_resource_response_html_formatter
        )

        # additional non-kaskada types we want to assign formatters to
        html_formatter.for_type(
            "google.protobuf.pyext._message.RepeatedCompositeContainer",
            proto_list_html_formatter,
        )

        if not cell_output_is_wrapped_in_iframe():
            # load our custom css into the page
            output_custom_css_and_javascript()

    except Exception as e:
        _LOGGER.debug("Init exception: {}".format(e))
