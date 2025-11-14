#!/bin/bash
# Script to set up sample groups and schemas in BanyanDB
# This helps populate BanyanDB with basic structure so you can start ingesting data

BANYANDB_URL="${BANYANDB_URL:-http://localhost:17913/api}"

echo "Setting up sample BanyanDB groups and schemas..."
echo "BanyanDB URL: $BANYANDB_URL"
echo ""

# Create a default group for streams
echo "Creating 'default' group for streams..."
curl -X POST "$BANYANDB_URL/v1/group/schema" \
  -H "Content-Type: application/json" \
  -d '{
    "group": {
      "metadata": {
        "name": "default"
      },
      "catalog": "CATALOG_STREAM",
      "resource_opts": {
        "shard_num": 2,
        "segment_interval": {
          "unit": "UNIT_DAY",
          "num": 1
        },
        "ttl": {
          "unit": "UNIT_DAY",
          "num": 7
        }
      }
    }
  }' && echo " ✓" || echo " ✗ (may already exist)"

# Create a group for measures
echo "Creating 'sw_metric' group for measures..."
curl -X POST "$BANYANDB_URL/v1/group/schema" \
  -H "Content-Type: application/json" \
  -d '{
    "group": {
      "metadata": {
        "name": "sw_metric"
      },
      "catalog": "CATALOG_MEASURE",
      "resource_opts": {
        "shard_num": 2,
        "segment_interval": {
          "unit": "UNIT_DAY",
          "num": 1
        },
        "ttl": {
          "unit": "UNIT_DAY",
          "num": 7
        }
      }
    }
  }' && echo " ✓" || echo " ✗ (may already exist)"

# Create a simple stream schema
echo "Creating 'sw' stream in 'default' group..."
curl -X POST "$BANYANDB_URL/v1/stream/schema" \
  -H "Content-Type: application/json" \
  -d '{
    "stream": {
      "metadata": {
        "name": "sw",
        "group": "default"
      },
      "tag_families": [
        {
          "name": "searchable",
          "tags": [
            {
              "name": "trace_id",
              "type": "TAG_TYPE_STRING"
            },
            {
              "name": "service_id",
              "type": "TAG_TYPE_STRING"
            }
          ]
        }
      ],
      "entity": {
        "tag_names": ["service_id"]
      }
    }
  }' && echo " ✓" || echo " ✗ (may already exist)"

# Create a simple measure schema
echo "Creating 'service_cpm_minute' measure in 'sw_metric' group..."
curl -X POST "$BANYANDB_URL/v1/measure/schema" \
  -H "Content-Type: application/json" \
  -d '{
    "measure": {
      "metadata": {
        "name": "service_cpm_minute",
        "group": "sw_metric"
      },
      "tag_families": [
        {
          "name": "default",
          "tags": [
            {
              "name": "id",
              "type": "TAG_TYPE_STRING"
            },
            {
              "name": "entity_id",
              "type": "TAG_TYPE_STRING"
            },
            {
              "name": "service_id",
              "type": "TAG_TYPE_STRING"
            }
          ]
        }
      ],
      "fields": [
        {
          "name": "total",
          "field_type": "FIELD_TYPE_INT",
          "encoding_method": "ENCODING_METHOD_GORILLA",
          "compression_method": "COMPRESSION_METHOD_ZSTD"
        },
        {
          "name": "value",
          "field_type": "FIELD_TYPE_INT",
          "encoding_method": "ENCODING_METHOD_GORILLA",
          "compression_method": "COMPRESSION_METHOD_ZSTD"
        }
      ],
      "entity": {
        "tag_names": ["entity_id"]
      },
      "interval": "1m"
    }
  }' && echo " ✓" || echo " ✗ (may already exist)"

echo ""
echo "Setup complete! You can now:"
echo "1. List groups: curl $BANYANDB_URL/v1/group/schema/lists"
echo "2. List streams: curl $BANYANDB_URL/v1/stream/schema/lists/default"
echo "3. List measures: curl $BANYANDB_URL/v1/measure/schema/lists/sw_metric"
echo ""
echo "Note: These are empty schemas. You still need to ingest actual data using:"
echo "- Stream write API: POST $BANYANDB_URL/v1/stream/data"
echo "- Measure write API: POST $BANYANDB_URL/v1/measure/data"
echo "- Or use BydbQL INSERT statements"

