from domonic.html import b, pre, table, td, th, tr

import kaskada.formatters_helpers as formatters_helpers
import kaskada.formatters_shared as formatters
import kaskada.kaskada.v1alpha.fenl_diagnostics_pb2 as fenl_pb
import kaskada.kaskada.v1alpha.pulsar_pb2 as pulsar_pb
import kaskada.kaskada.v1alpha.sources_pb2 as sources_pb
import kaskada.kaskada.v1alpha.table_service_pb2 as table_pb


def test_table_html_rendering_default():
    test_table = table_pb.Table()
    render, schema = formatters.get_table_html_and_schema_df(test_table)
    expected = table(_class="kda_table")
    expected.appendChild(tr(td(b("version")), td(pre(0))))
    expected.appendChild(
        tr(
            td(b("create_time")),
            td(
                pre(formatters_helpers.get_datetime(test_table.create_time).isoformat())
            ),
        )
    )
    expected.appendChild(
        tr(
            td(b("update_time")),
            td(
                pre(formatters_helpers.get_datetime(test_table.update_time).isoformat())
            ),
        )
    )
    assert schema is None
    assert str(render) == str(expected)


def test_table_html_minimal_table():
    name = "test_table"
    entity_column = "entity_key_column"
    time_column = "time_column"
    grouping_id = "groupind_id"
    test_table = table_pb.Table(
        **{
            "table_name": name,
            "entity_key_column_name": entity_column,
            "time_column_name": time_column,
            "grouping_id": grouping_id,
        }
    )
    render, schema = formatters.get_table_html_and_schema_df(test_table)

    expected = table(_class="kda_table")
    expected.appendChild(tr(td(b("table_name")), td(pre(name))))
    expected.appendChild(tr(td(b("entity_key_column_name")), td(pre(entity_column))))
    expected.appendChild(tr(td(b("time_column_name")), td(pre(time_column))))
    expected.appendChild(tr(td(b("grouping_id")), td(pre(grouping_id))))
    expected.appendChild(tr(td(b("version")), td(pre(0))))
    expected.appendChild(
        tr(
            td(b("create_time")),
            td(
                pre(formatters_helpers.get_datetime(test_table.create_time).isoformat())
            ),
        )
    )
    expected.appendChild(
        tr(
            td(b("update_time")),
            td(
                pre(formatters_helpers.get_datetime(test_table.update_time).isoformat())
            ),
        )
    )
    assert schema is None
    assert str(render) == str(expected)


def test_table_pulsar_config_html():
    broker_service_url = "pulsar://localhost:6650"
    auth_plugin = "auth-plugin"
    tenant = "test-tenant"
    namespace = "test-namespace"
    topic_name = "my-topic"
    pulsar = pulsar_pb.PulsarConfig(
        **{
            "broker_service_url": broker_service_url,
            "auth_plugin": auth_plugin,
            "tenant": tenant,
            "namespace": namespace,
            "topic_name": topic_name,
        }
    )
    render = formatters.get_table_pulsar_config(pulsar)
    expected = table(_class="kda_table")
    expected.appendChild(tr(td(b("broker_service_url")), td(pre(broker_service_url))))
    expected.appendChild(tr(td(b("auth_plugin")), td(pre(auth_plugin))))
    expected.appendChild(tr(td(b("tenant")), td(pre(tenant))))
    expected.appendChild(tr(td(b("namespace")), td(pre(namespace))))
    expected.appendChild(tr(td(b("topic_name")), td(pre(topic_name))))
    assert str(render) == str(expected)


def test_table_html_pulsar_table():
    name = "test_table"
    entity_column = "entity_key_column"
    time_column = "time_column"
    broker_service_url = "pulsar://localhost:6650"
    auth_plugin = "auth-plugin"
    tenant = "test-tenant"
    namespace = "test-namespace"
    topic_name = "my-topic"
    test_table = table_pb.Table(
        table_name=name,
        time_column_name=time_column,
        entity_key_column_name=entity_column,
        source=sources_pb.Source(
            pulsar=sources_pb.PulsarSource(
                config=pulsar_pb.PulsarConfig(
                    **{
                        "broker_service_url": broker_service_url,
                        "auth_plugin": auth_plugin,
                        "tenant": tenant,
                        "namespace": namespace,
                        "topic_name": topic_name,
                    }
                )
            )
        ),
    )
    render, schema = formatters.get_table_html_and_schema_df(test_table)
    expected = table(_class="kda_table")
    expected.appendChild(tr(td(b("table_name")), td(pre(name))))
    expected.appendChild(tr(td(b("entity_key_column_name")), td(pre(entity_column))))
    expected.appendChild(tr(td(b("time_column_name")), td(pre(time_column))))
    expected.appendChild(
        tr(
            td(b("pulsar")),
            td(formatters.get_table_pulsar_config(test_table.source.pulsar.config)),
        )
    )
    expected.appendChild(tr(td(b("version")), td(pre(0))))
    expected.appendChild(
        tr(
            td(b("create_time")),
            td(
                pre(formatters_helpers.get_datetime(test_table.create_time).isoformat())
            ),
        )
    )
    expected.appendChild(
        tr(
            td(b("update_time")),
            td(
                pre(formatters_helpers.get_datetime(test_table.update_time).isoformat())
            ),
        )
    )
    assert schema is None
    assert str(render) == str(expected)


def test_update_diagnostic_with_link():
    valid_error = fenl_pb.FenlDiagnostic(
        code="E0013",
        formatted="error[E0013]: Invalid output type = Output type must be a record, but was i64",
        web_link="https://kaskada.io/docs-site/kaskada/main/troubleshooting/diagnostic-pages/e0013.html",
    )
    result = formatters.update_diagnostic_with_link(valid_error)
    assert (
        result
        == 'error[<a href="https://kaskada.io/docs-site/kaskada/main/troubleshooting/diagnostic-pages/e0013.html" target="_blank">E0013</a>]: Invalid output type = Output type must be a record, but was i64'
    )
