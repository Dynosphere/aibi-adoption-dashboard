-- tests/sql/test_mv_serving_usage.sql — runs post-deploy against the warehouse.
--
-- mvFactServingUsage is endpoint-grain (endpoint_id, usage_date, workspace_id): cost is driven
-- from system.billing.usage (MODEL_SERVING) so it reconciles to billing, and per-endpoint usage
-- is aggregated across served entities. served_entities is deduped to latest config, so the
-- grain-uniqueness assert below now holds (it was expected-fail under the old served_entity grain).
SELECT assert_true(count(*) = 0, 'mvFactServingUsage grain must be unique') AS grain_ok
FROM (
  SELECT endpoint_id, usage_date, workspace_id, count(*) c
  FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactServingUsage')
  GROUP BY 1,2,3 HAVING count(*) > 1
);

SELECT assert_true(count(*) = 0, 'token/cost columns must be non-negative') AS nonneg_ok
FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactServingUsage')
WHERE request_count < 0 OR input_tokens < 0 OR output_tokens < 0 OR dbus < 0 OR dollars < 0;

-- entity_name must never be NULL: it is the representative served-entity name, falling back to
-- the billing endpoint_name for endpoints absent from system.serving.served_entities, so the
-- dashboard "model views by name" never gets a NULL bucket.
SELECT assert_true(count(*) = 0, 'entity_name must never be NULL') AS name_ok
FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactServingUsage')
WHERE entity_name IS NULL;
