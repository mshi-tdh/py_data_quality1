{% macro validate_simple_rule(rule_id, rule_configs, rule_binding_id, fully_qualified_table_name, include_reference_columns ) -%}
{% set column_name = rule_configs.get("params").get("rule_binding_arguments").get("p_column_name") -%}
  SELECT
    CURRENT_TIMESTAMP AS execution_ts,
    '{{ rule_binding_id }}'::text AS rule_binding_id,
    '{{ rule_id }}'::text AS rule_id,
    '{{ fully_qualified_table_name }}'::text AS table_id,
    '{{ column_name }}'::text AS column_id,
    data.{{ column_name }} AS column_value,
    {%- for ref_column_name in include_reference_columns -%}
        data.{{ ref_column_name }} AS {{ ref_column_name }},
    {%- endfor -%}
{% if rule_configs.get("dimension") %}
    '{{ rule_configs.get("dimension") }}'::text AS dimension,
{% else %}
    CAST(NULL AS varchar) AS dimension,
{% endif %}
    CASE
{% if rule_id == 'RL_NOT_NULL_CHECK' %}
      WHEN {{ rule_configs.get("rule_sql_expr") }} THEN TRUE
{% else %}
      WHEN {{ column_name }} IS NULL THEN CAST(NULL AS BOOLEAN)
      WHEN {{ rule_configs.get("rule_sql_expr") }} THEN TRUE
{% endif %}
    ELSE
      FALSE
    END AS simple_rule_row_is_valid,
    CAST(NULL AS INT) AS complex_rule_validation_errors_count,
    CAST(NULL AS BOOLEAN) AS complex_rule_validation_success_flag
  FROM
    zero_record
  LEFT JOIN
    data
  ON
    zero_record.rule_binding_id = data.rule_binding_id
{% endmacro -%}

{% macro validate_complex_rule(rule_id, rule_configs, rule_binding_id, fully_qualified_table_name, include_reference_columns,rule_binding_params ) -%}
{% set target_metric_column = rule_binding_params['tgt_tbl_metric_column'] -%}
{% set target_key_columns = rule_binding_params['tgt_tbl_key_column'] -%}
{% set source_metric_column = rule_binding_params['src_tbl_metric_column'] -%}
{% set source_key_column = rule_binding_params['src_tbl_key_column'] -%}
{% set row_limit = rule_binding_params['output_row_limit'] -%}
{% if target_key_columns %}
    {% set target_key_column_list = target_key_columns.split(',') -%}
{% endif %}
  SELECT
    CURRENT_TIMESTAMP AS execution_ts,
    '{{ rule_binding_id }}'::text AS rule_binding_id,
    '{{ rule_id }}'::text AS rule_id,
    '{{ fully_qualified_table_name }}' AS table_id,
{%- if target_metric_column %}
    '{{ target_metric_column }}' AS column_id,
    custom_sql_statement_validation_errors.tgt_metric_value as column_value,
{%- else %}
    CAST('NULL' AS varchar) AS column_id,
    custom_sql_statement_validation_errors.aggregated_result AS column_value,
{% endif %}
{%- if source_metric_column %}
    custom_sql_statement_validation_errors.src_metric_value as src_column_value,
{%- else %}
    NULL AS src_column_value,
{% endif %}
{%- if target_key_column_list %}
    '{'
    {%- for target_key_column in target_key_column_list -%}
    {%- if not loop.last %}
      '"'||'{{ target_key_column }}'||'":"'||custom_sql_statement_validation_errors.{{ target_key_column }}||'",'||
    {%- else %}
      '"'||'{{ target_key_column }}'||'":"'||custom_sql_statement_validation_errors.{{ target_key_column }}||'"'
    {% endif %}
    {%- endfor -%}
    '}' as column_val_json_str,
{%- else %}
    NULL AS column_val_json_str,
{%- endif %}
    {%- for ref_column_name in include_reference_columns %}
        custom_sql_statement_validation_errors.{{ ref_column_name }} AS {{ ref_column_name }},
    {%- endfor -%}
{%- if rule_configs.get("dimension") %}
    '{{ rule_configs.get("dimension") }}'::text AS dimension,
{%- else %}
    CAST(NULL AS varchar) AS dimension,
{% endif %}
    CAST(NULL AS BOOLEAN) AS simple_rule_row_is_valid,
    custom_sql_statement_validation_errors.complex_rule_validation_errors_count AS complex_rule_validation_errors_count,
    CASE
      WHEN custom_sql_statement_validation_errors.complex_rule_validation_errors_count IS NULL THEN CAST(NULL AS BOOLEAN)
      WHEN custom_sql_statement_validation_errors.complex_rule_validation_errors_count = 0 THEN FALSE
      ELSE TRUE
    END AS complex_rule_validation_success_flag
  FROM
    zero_record
  LEFT JOIN
    (
      SELECT
         DISTINCT
         custom_sql.*,
        '{{ rule_binding_id }}' AS _rule_binding_id,
        COUNT(*) OVER() AS complex_rule_validation_errors_count
      FROM (
      {{ rule_configs.get("rule_sql_expr") }}
      ) custom_sql
{%- if row_limit %}
    limit {{ row_limit }}
{%- endif %}
    ) custom_sql_statement_validation_errors
  ON
    zero_record.rule_binding_id = custom_sql_statement_validation_errors._rule_binding_id
{% endmacro -%}

{% macro validate_entity_rule(rule_id, rule_configs, rule_binding_id, fully_qualified_table_name, include_reference_columns ) -%}
{% set column_name = None -%}
{% if rule_configs.get("params") -%}
    {% if rule_configs.get("params").get("rule_binding_arguments") -%}
        {% if rule_configs.get("params").get("rule_binding_arguments").get("p_column_name") -%}
            {% set column_name = rule_configs.get("params").get("rule_binding_arguments").get("p_column_name") -%}
         {% endif -%}
    {% endif -%}
{% endif -%}
  SELECT
    CURRENT_TIMESTAMP AS execution_ts,
    '{{ rule_binding_id }}'::text AS rule_binding_id,
    '{{ rule_id }}'::text AS rule_id,
    '{{ fully_qualified_table_name }}' AS table_id,
    '{{ column_name }}'::text AS column_id,
    NULL AS column_value,
    {%- for ref_column_name in include_reference_columns %}
        custom_sql_statement_validation_errors.{{ ref_column_name }} AS {{ ref_column_name }},
    {% endfor %}
{%- if rule_configs.get("dimension") -%}
    '{{ rule_configs.get("dimension") }}'::text AS dimension,
{%- else -%}
    CAST(NULL AS varchar) AS dimension,
{% endif %}
    CAST(NULL AS BOOLEAN) AS simple_rule_row_is_valid,
    custom_sql_statement_validation_errors.complex_rule_validation_errors_count AS complex_rule_validation_errors_count,
    CASE
      WHEN custom_sql_statement_validation_errors.complex_rule_validation_errors_count IS NULL THEN CAST(NULL AS BOOLEAN)
      WHEN custom_sql_statement_validation_errors.complex_rule_validation_errors_count = 0 THEN FALSE
      ELSE TRUE
    END AS complex_rule_validation_success_flag
  FROM
    zero_record
  LEFT JOIN
    (
      SELECT
         *,
        '{{ rule_binding_id }}' AS rule_binding_id,
        COUNT(*) OVER() AS complex_rule_validation_errors_count,
      FROM (
      {{ rule_configs.get("rule_sql_expr") }}
      ) custom_sql
    ) custom_sql_statement_validation_errors
  ON
    zero_record.rule_binding_id = custom_sql_statement_validation_errors.rule_binding_id
{% endmacro -%}
