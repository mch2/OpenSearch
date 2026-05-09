#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-http://localhost:9200}"
INDEX="${INDEX:-object_fields}"

hr() { printf '\n--- %s ---\n' "$*"; }

hr "delete (ignore if missing)"
curl -sS -o /dev/null -w "%{http_code}\n" -XDELETE "$HOST/$INDEX"

hr "create index with object mapping + parquet pluggable format"
curl -sS -XPUT "$HOST/$INDEX" -H 'Content-Type: application/json' -d '{
  "settings": {
    "index.pluggable.dataformat.enabled": true,
    "index.pluggable.dataformat": "composite",
    "index.composite.primary_data_format": "parquet",
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "id": {"type": "keyword"},
      "city": {
        "properties": {
          "name":       {"type": "keyword"},
          "population": {"type": "integer"},
          "location": {
            "properties": {
              "latitude":  {"type": "double"},
              "longitude": {"type": "double"}
            }
          }
        }
      },
      "account": {
        "properties": {
          "owner":   {"type": "keyword"},
          "balance": {"type": "double"}
        }
      }
    }
  }
}' | jq .

hr "bulk ingest"
curl -sS -XPOST "$HOST/$INDEX/_bulk?refresh=true" -H 'Content-Type: application/x-ndjson' --data-binary $'
{"index":{"_id":"1"}}
{"id":"1","city":{"name":"Seattle","population":750000,"location":{"latitude":47.6062,"longitude":-122.3321}},"account":{"owner":"alice","balance":1000.50}}
{"index":{"_id":"2"}}
{"id":"2","city":{"name":"Portland","population":650000,"location":{"latitude":45.5152,"longitude":-122.6784}},"account":{"owner":"bob","balance":2500.00}}
{"index":{"_id":"3"}}
{"id":"3","city":{"name":"Austin","population":980000,"location":{"latitude":30.2672,"longitude":-97.7431}},"account":{"owner":"carol","balance":300.25}}
' | jq '{errors: .errors, item_count: (.items | length)}'

hr "flush so parquet files land on disk"
curl -sS -XPOST "$HOST/$INDEX/_flush?force=true" | jq .

hr "PPL: select single object field (city.name)"
curl -sS -XPOST "$HOST/_plugins/_ppl" -H 'Content-Type: application/json' -d "{
  \"query\": \"source=$INDEX | fields city.name | head 3\"
}" | jq .

hr "PPL: select deeply nested (city.name, city.location.latitude)"
curl -sS -XPOST "$HOST/_plugins/_ppl" -H 'Content-Type: application/json' -d "{
  \"query\": \"source=$INDEX | fields city.name, city.location.latitude | head 3\"
}" | jq .

hr "PPL: min on object field (account.balance)"
curl -sS -XPOST "$HOST/_plugins/_ppl" -H 'Content-Type: application/json' -d "{
  \"query\": \"source=$INDEX | stats min(account.balance)\"
}" | jq .

hr "PPL: max on deeply nested (city.location.latitude)"
curl -sS -XPOST "$HOST/_plugins/_ppl" -H 'Content-Type: application/json' -d "{
  \"query\": \"source=$INDEX | stats max(city.location.latitude)\"
}" | jq .

hr "PPL: filter on object field (where city.name='Seattle')"
curl -sS -XPOST "$HOST/_plugins/_ppl" -H 'Content-Type: application/json' -d "{
  \"query\": \"source=$INDEX | where city.name='Seattle' | fields account.owner\"
}" | jq .

hr "done"
