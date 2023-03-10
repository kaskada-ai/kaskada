from domonic.html import b, pre, table, td, th, tr

import kaskada.formatters_helpers as formatters_helpers
import kaskada.formatters_shared as formatters
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


def test_table_source_pulsar_html():
    protocol_url = "pulsar://localhost:6650"
    topic = "my-topic"
    pulsar = table_pb.Table.PulsarSource(
        **{"protocol_url": protocol_url, "topic": topic}
    )
    render = formatters.get_table_source_pulsar(pulsar)
    expected = table(_class="kda_table")
    expected.appendChild(tr(td(b("protocol_url")), td(pre(protocol_url))))
    expected.appendChild(tr(td(b("topic")), td(pre(topic))))
    assert str(render) == str(expected)


def test_table_html_pulsar_table():
    name = "test_table"
    entity_column = "entity_key_column"
    time_column = "time_column"
    grouping_id = "groupind_id"
    protocol_url = "pulsar://localhost:6650"
    topic = "my-topic"
    test_table = table_pb.Table(
        table_name=name,
        time_column_name=time_column,
        entity_key_column_name=entity_column,
        table_source={
            "pulsar": table_pb.Table.PulsarSource(
                **{"protocol_url": protocol_url, "topic": topic}
            )
        },
    )
    render, schema = formatters.get_table_html_and_schema_df(test_table)
    expected = table(_class="kda_table")
    expected.appendChild(tr(td(b("table_name")), td(pre(name))))
    expected.appendChild(tr(td(b("entity_key_column_name")), td(pre(entity_column))))
    expected.appendChild(tr(td(b("time_column_name")), td(pre(time_column))))
    expected.appendChild(
        tr(
            td(b("pulsar")),
            td(formatters.get_table_source_pulsar(test_table.table_source.pulsar)),
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
