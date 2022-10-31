{% from 'macros.sql' import validate_simple_rule -%}
{% from 'macros.sql' import validate_complex_rule -%}
{% from 'macros.sql' import validate_entity_rule -%}
{%- macro create_rule_binding_view(configs, environment, dq_summary_table_name, metadata, configs_hashsum, progress_watermark, dq_summary_table_exists, high_watermark_value, current_timestamp_value, generated_sql_string) -%}
{% set rule_binding_id = configs.get('rule_binding_id') -%}
{% set rule_configs_dict = configs.get('rule_configs_dict') -%}
{% set rule_ids = configs.get('rule_ids') -%}
{% set filter_sql_expr = configs.get('row_filter_configs').get('filter_sql_expr') -%}
{% set entity_configs = configs.get('entity_configs') -%}
{% set partition_fields = entity_configs.get('partition_fields')-%}
{% set instance_name = entity_configs.get('instance_name') -%}
{% set schema_name = entity_configs.get('schema_name') -%}
{% set table_name = entity_configs.get('table_name') -%}
{% set include_reference_columns =  configs.get('include_reference_columns') -%}
{% set incremental_time_filter_column_id = configs.get('incremental_time_filter_column_id') %}
{% set column_config = namespace(target_key_column=[],target_metric_column='',source_metric_column='') %}
{% set rule_binding = namespace(params=dict()) %}
{% if environment and entity_configs.get('environment_override') -%}
  {% set env_override = entity_configs.get('environment_override') %}
  {% if env_override.get(environment|lower) %}
    {% set override_values = env_override.get(environment|lower) %}
    {% if override_values.get('table_name') -%}
        {% set table_name = override_values.get('table_name') -%}
    {% endif -%}
    {% if override_values.get('schema_name') -%}
        {% set schema_name = override_values.get('schema_name') -%}
    {% endif -%}
    {% if override_values.get('instance_name') -%}
        {% set instance_name = override_values.get('instance_name') -%}
    {% endif -%}
  {% endif %}
{% endif -%}
{% set fully_qualified_table_name = "%s.%s" % (schema_name, table_name) -%}
{% set _dummy = metadata.update(configs.get('metadata', '')) -%}
WITH
{%- if configs.get('incremental_time_filter_column') and dq_summary_table_exists -%}
{% set time_column_id = configs.get('incremental_time_filter_column') %}
{% endif -%}
zero_record AS (
    SELECT
        '{{ rule_binding_id }}'::text AS rule_binding_id
),
data AS (
    SELECT
       *,
      '{{ rule_binding_id }}'::text AS rule_binding_id
    FROM {{ fully_qualified_table_name }} d
{%- if configs.get('incremental_time_filter_column') and dq_summary_table_exists %}
    WHERE
      d.{{ time_column_id }}
          BETWEEN CAST('{{ high_watermark_value }}' AS TIMESTAMP) AND CAST('{{ current_timestamp_value }}' AS TIMESTAMP)
      {{ filter_sql_expr }}
{% else %}
    WHERE
      {{ filter_sql_expr }}
{% endif -%}
{%- if partition_fields %}
    {% for field in partition_fields %}
        AND {{ field['name'] }} IS NOT NULL
    {%- endfor -%}
{% endif -%}
),
validation_results AS (
{% for rule_id, rule_configs in rule_configs_dict.items() %}
    {%- if rule_configs.get('rule_type') == "CUSTOM_SQL_STATEMENT" -%}
    {% for rule_runtime_params in rule_ids %}
    {%- if rule_runtime_params.get(rule_id) -%}
    {% set rule_binding.params = rule_runtime_params.get(rule_id) -%}
    {% endif -%}
    {%- endfor -%}
      {{ validate_complex_rule(rule_id, rule_configs, rule_binding_id, fully_qualified_table_name, include_reference_columns,
            rule_binding.params ) }}
    {%- elif rule_configs.get('rule_type') == "ENTITY_LEVEL" -%}
      {{ validate_entity_rule(rule_id, rule_configs, rule_binding_id, fully_qualified_table_name, include_reference_columns) }}
    {%- else -%}
      {{ validate_simple_rule(rule_id, rule_configs, rule_binding_id, fully_qualified_table_name, include_reference_columns) }}
    {%- endif -%}
    {% set column_config.target_key_column = [] -%}
    {% if loop.nextitem is defined %}
    UNION ALL
    {% endif -%}
{%- endfor -%}
),
all_validation_results AS (
  SELECT
    r.execution_ts AS execution_ts
    ,r.rule_binding_id AS rule_binding_id
    ,r.rule_id AS rule_id
    ,r.table_id AS table_id
    ,r.column_id AS column_key
    ,r.column_value AS tgt_column_value
    ,r.src_column_value
    ,r.column_val_json_str
    ,CAST(r.dimension AS varchar) AS dimension
    ,r.simple_rule_row_is_valid AS simple_rule_row_is_valid
    ,r.complex_rule_validation_errors_count AS complex_rule_validation_errors_count
    ,r.complex_rule_validation_success_flag AS complex_rule_validation_success_flag
    {% for ref_column_name in include_reference_columns %}
        ,r.{{ ref_column_name }} AS {{ ref_column_name }}
        {% if loop.last %}
            {{ '\n' }}
        {% endif %}
    {%- endfor -%}
    ,(SELECT COUNT(*) FROM data) AS rows_validated
    ,'{{ metadata|tojson }}' AS metadata_json_string
    ,'{{ configs_hashsum }}' AS configs_hashsum
    ,r.rule_binding_id||'_'||r.rule_id||'_'||r.execution_ts AS dq_run_id
    {%- if progress_watermark %}
      ,{{ progress_watermark|upper }} AS progress_watermark
    {% endif %}
  FROM
    validation_results r
)
SELECT
  *
FROM
  all_validation_results

{%- endmacro -%}

{{-  create_rule_binding_view(configs, environment, dq_summary_table_name, metadata, configs_hashsum, progress_watermark, dq_summary_table_exists, high_watermark_value, current_timestamp_value, generated_sql_string) -}}
