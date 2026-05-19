# Telemetry / Tracing Options — Ideas

État actuel : seul `service_name` / `service_version` / `host_name` est configurable
via `tracing::set_resource()`. Endpoint hardcodé à `192.168.64.1:4317`. Pas de
sampling, pas de batching configurable, pas de variables d'env.

## Endpoint / transport
- `endpoint` (host:port) — override du `192.168.64.1:4317` hardcodé
- `protocol` — gRPC vs HTTP/protobuf vs HTTP/JSON (uniquement gRPC pour l'instant)
- `headers` — auth tokens, tenant IDs (ex. `Authorization: Bearer ...`)
- `tls` — on/off, custom CA, client cert
- `compression` — gzip (réduit la bande passante de ~5–10×)
- `export_timeout` — timeout par export

## Resource attributes
- `service.name`, `service.version`, `service.instance.id` (partiellement OK)
- `deployment.environment` (prod/staging/dev)
- `host.name`, `host.arch`, `os.type`
- Attributs arbitraires key=value

## Sampling
- `always_on` / `always_off`
- `trace_id_ratio` (ex. 0.01 = 1 % des traces)
- `parent_based` — respecter la décision du parent
- `rate_limited` — N spans/sec max
- Override par nom de span ou attribut

## Batching / queue
- `max_export_batch_size` (défaut OTEL : 512)
- `schedule_delay` — intervalle de flush (défaut : 5 s)
- `max_queue_size` — au-delà, drop des spans (défaut : 2048)
- `max_concurrent_exports`

## Retry / backoff
- `max_retries`
- `initial_backoff` / `max_backoff` exponentiel
- Retry policy par code d'erreur gRPC

## Span limits (norme OTEL)
- `attribute_count_limit` (défaut : 128)
- `event_count_limit` (défaut : 128)
- `link_count_limit` (défaut : 128)
- `attribute_value_length_limit`

## Variables d'environnement standard OTEL
- `OTEL_EXPORTER_OTLP_ENDPOINT`
- `OTEL_EXPORTER_OTLP_HEADERS`
- `OTEL_EXPORTER_OTLP_COMPRESSION`
- `OTEL_SERVICE_NAME`
- `OTEL_RESOURCE_ATTRIBUTES`
- `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG`
- `OTEL_SDK_DISABLED` (kill switch)

## Diagnostics
- Stats internes : spans émis / droppés / en queue, latence d'export
- Self-log si export échoue de manière répétée

## Priorité d'implémentation suggérée
1. `endpoint` configurable + lecture de `OTEL_EXPORTER_OTLP_ENDPOINT`
2. `OTEL_SDK_DISABLED` (kill switch sans recompiler)
3. Sampling `trace_id_ratio` (sinon en prod le collecteur sature)
4. `max_queue_size` (sinon backpressure du collecteur → OOM)
5. `compression` gzip
6. Headers (auth — obligatoire dès que le collecteur n'est pas local)
