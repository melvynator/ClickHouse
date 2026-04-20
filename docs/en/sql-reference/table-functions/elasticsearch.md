---
slug: /sql-reference/table-functions/elasticsearch
sidebar_position: 100
sidebar_label: elasticsearch
title: elasticsearch
---

# elasticsearch Table Function

Reads data from an Elasticsearch 8.x+ or OpenSearch 2.x index. Uses search_after with Point In Time (PIT) for efficient, consistent bulk reads with parallel processing.

## Syntax

``` sql
elasticsearch(host, index, user, password, structure [, query])
```

**Arguments**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| host | String | Yes | Elasticsearch endpoint URL including scheme and port. |
| index | String | Yes | Index name or pattern. Supports wildcards. |
| user | String | Yes | Username for HTTP Basic Authentication. |
| password | String | Yes | Password for HTTP Basic Authentication. |
| structure | String | Yes | ClickHouse column definitions. |
| query | String | No | Elasticsearch Query DSL as JSON string. Defaults to match_all. |

## Examples

Read all documents from an index:

``` sql
SELECT * FROM elasticsearch(
    'http://localhost:9200', 'products', 'admin', 'secret',
    'id UInt64, name String, price Float64')
```

Migrate data from Elasticsearch to ClickHouse:

``` sql
INSERT INTO logs_clickhouse
SELECT * FROM elasticsearch(
    'http://es-cluster:9200', 'logs-2025.03', 'admin', 'secret',
    'timestamp DateTime64(3), message String, level String')
```

Read with Elasticsearch Query DSL filter:

``` sql
SELECT * FROM elasticsearch(
    'http://localhost:9200', 'logs-*', 'admin', 'secret',
    'level String, timestamp DateTime',
    '{"range": {"timestamp": {"gte": "now-7d"}}}')
```

## Named Collections

``` sql
CREATE NAMED COLLECTION es_prod AS
    host = 'http://es-cluster:9200',
    user = 'admin',
    password = 'secret';

SELECT * FROM elasticsearch(es_prod,
    index='logs-*',
    structure='timestamp DateTime64(3), message String')
```

## Type Mapping

The structure argument is authoritative. Each column name must match a field in the Elasticsearch _source document.

| Elasticsearch type | Recommended ClickHouse type |
|---|---|
| long | Int64 |
| integer | Int32 |
| short | Int16 |
| unsigned_long | UInt64 |
| double | Float64 |
| float | Float32 |
| keyword, text | String |
| boolean | UInt8 |
| date | DateTime or DateTime64(3) |
| object, nested | String (serialized as JSON) |
| Missing field | Default value, or NULL if Nullable |

## How It Works

1. Opens a Point In Time (PIT) on the index for a consistent snapshot.
2. Creates N parallel sources (controlled by ClickHouse num_streams), each handling an independent slice.
3. Each source paginates its slice using search_after.
4. Closes the PIT when all sources finish.

## Compatibility

- Elasticsearch 8.x+: Full support.
- OpenSearch 2.x+: Automatically detected. Uses _search/point_in_time API.
- Authentication: HTTP Basic Authentication only.
- Read-only: Write operations (INSERT INTO TABLE FUNCTION) are not supported.

## See Also

- [mysql](mysql.md)
- [mongodb](mongodb.md)
- [postgresql](postgresql.md)
