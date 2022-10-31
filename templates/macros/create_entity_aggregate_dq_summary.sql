{% macro create_entity_aggregate_dq_summary(entity_target_rule_binding_configs,failed_queries_configs) -%}

{% set rule_binding_ids_list = entity_target_rule_binding_configs.get('rule_binding_ids_list') %}
{%- for rule_binding_id in rule_binding_ids_list -%}
  SELECT
      execution_ts,
      rule_binding_id,
      rule_id,
      table_id,
      column_id,
      dimension,
      metadata_json_string,
      configs_hashsum,
      dq_run_id,
      '' progress_watermark,
      rows_validated,
      complex_rule_validation_errors_count,
      complex_rule_validation_success_flag,
      '' last_modified,
      skip_null_count,
      CASE 
        WHEN rows_validated = 0 THEN NULL
        WHEN complex_rule_validation_errors_count IS NOT NULL THEN NULL
        ELSE count(case when simple_rule_row_is_valid IS true then 1 else null end)
      END
      AS success_count,
      CASE
        WHEN rows_validated = 0 THEN NULL
        WHEN complex_rule_validation_errors_count IS NOT NULL THEN NULL
        ELSE count(case when simple_rule_row_is_valid IS true then 1 else null end) / rows_validated
      END
      AS success_percentage,
      CASE
        WHEN rows_validated = 0 THEN NULL
        WHEN complex_rule_validation_errors_count IS NOT NULL THEN NULL
        ELSE count(case when simple_rule_row_is_valid IS false then 1 else null end)
      END
      AS failed_count,
      CASE
        WHEN rows_validated = 0 THEN NULL
        WHEN complex_rule_validation_errors_count IS NOT NULL THEN NULL
        ELSE COUNT(case when simple_rule_row_is_valid IS false then 1 else null end) / rows_validated
      END
      AS failed_percentage,
      CASE
        WHEN rows_validated = 0 THEN NULL
        WHEN complex_rule_validation_errors_count IS NOT NULL THEN NULL
        WHEN skip_null_count IS TRUE THEN NULL
        ELSE count(case when simple_rule_row_is_valid IS null then 1 else null end)
      END
      AS null_count,
      CASE
        WHEN rows_validated = 0 THEN NULL
        WHEN complex_rule_validation_errors_count IS NOT NULL THEN NULL
        WHEN skip_null_count IS TRUE THEN NULL
        ELSE count(case when simple_rule_row_is_valid IS null then 1 else null end) / rows_validated
      END
      AS null_percentage,
      null as failed_records_query
  FROM
      {% raw -%}{{ ref('{%- endraw %}{{ rule_binding_id }}{% raw -%}') }}{%- endraw %}
  GROUP BY
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,22

  {% if loop.nextitem is defined %}
  UNION ALL
  {% endif %}
{%- endfor -%}

{%- endmacro -%}

{{  create_entity_aggregate_dq_summary(entity_target_rule_binding_configs, failed_queries_configs) }}
