{% macro warehouse_surrogate_key(parts) -%}
md5(
    jsonb_build_array(
        {%- for part in parts %}
        {{ part }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    )::text
)
{%- endmacro %}
