-- VIXION PRIME — consultas operativas de referencia (copiar a psql).
-- Requiere conexión a la misma DB que DATABASE_URL.

-- Resumen de jobs por tipo y estado
SELECT job_type::text, status::text, count(*) AS n
FROM jobs
GROUP BY job_type, status
ORDER BY job_type, status;

-- Antigüedad del pending más viejo por tipo de job (minutos)
SELECT
  job_type::text,
  min(created_at) AS oldest_created_at,
  extract(epoch FROM (now() - min(created_at))) / 60.0 AS oldest_pending_age_min
FROM jobs
WHERE status = 'pending'::job_status
GROUP BY job_type;

-- Últimas ingestas RSS con métricas en stats (jsonb)
SELECT
  ri.id,
  ri.source_id,
  s.config->>'slug' AS feed_slug,
  ri.status::text,
  ri.stats->>'entries_seen' AS entries_seen,
  ri.stats->>'process_item_enqueued_new' AS enqueued_new,
  ri.stats->>'process_item_job_deduped' AS deduped,
  ri.stats->>'entries_skipped_no_link' AS skipped_no_link,
  CASE
    WHEN jsonb_typeof(coalesce(ri.stats->'errors', '[]'::jsonb)) = 'array'
    THEN jsonb_array_length(coalesce(ri.stats->'errors', '[]'::jsonb))
    ELSE 0
  END AS error_n,
  ri.started_at,
  ri.finished_at,
  left(ri.error_message, 200) AS error_message
FROM raw_ingests ri
JOIN sources s ON s.id = ri.source_id
ORDER BY ri.started_at DESC
LIMIT 40;

-- Narrativas recientes (proyección mutable)
SELECT
  nc.narrative_id,
  left(nc.current_title, 100) AS title,
  nc.score,
  nc.state::text,
  nc.item_count,
  nc.updated_at
FROM narrative_current nc
ORDER BY nc.updated_at DESC NULLS LAST
LIMIT 20;
