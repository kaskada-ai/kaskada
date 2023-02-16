{%- if example.name -%}
### Example: {{ example.name }}
{% else -%}
### Example
{% endif %}
{%- if example.description -%}
{{ example.description }}
{% endif -%}

#### Query
```
{%- if example.full_expression %}
{{ example.full_expression | trim }}
{% else %}
{{ example.expression }}
{% endif -%}
```
{% if example.input_csv %}
#### Table: Input

* **Name**: `Input`
* **Time Column**: `time`
* **Group Column**: `key`
* **Grouping**: `grouping`

{{ example.input_csv | csv2md | trim }}
{% endif %}
{%- for table in example.tables | default(value=[]) %}
#### Table: {{ table.name }}
* **Name**: `{{ table.name }}`
* **Time Column**: `{{ table.time_column_name }}`
* **Group Column**: `{{ table.group_column_name }}`
* **Grouping**: `{{ table.grouping }}`

{{ table.input_csv | csv2md | trim }}
{% endfor %}
#### Output CSV
{{ example.output_csv | csv2md | trim }}