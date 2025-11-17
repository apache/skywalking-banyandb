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

# Extract gRPC address from HTTP URL
# Default gRPC port is 17912 (not 17900)
BANYANDB_GRPC="${BANYANDB_GRPC:-localhost:17912}"
if [[ "$BANYANDB_URL" == http://* ]]; then
  # Extract host and port, convert HTTP port to gRPC port
  BANYANDB_HOST_PORT=$(echo "$BANYANDB_URL" | sed 's|http://||' | sed 's|/api||')
  if [[ "$BANYANDB_HOST_PORT" == *:17913 ]]; then
    BANYANDB_GRPC=$(echo "$BANYANDB_HOST_PORT" | sed 's|:17913|:17912|')
  elif [[ "$BANYANDB_HOST_PORT" == *:* ]]; then
    BANYANDB_GRPC="$BANYANDB_HOST_PORT"
  else
    BANYANDB_GRPC="${BANYANDB_HOST_PORT}:17912"
  fi
fi

# Write sample data to service_cpm_minute measure
echo ""
echo "Writing sample data to 'service_cpm_minute' measure..."

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/write-measure-data.py"

# Try Python script first (most reliable)
if command -v python3 &> /dev/null && [ -f "$PYTHON_SCRIPT" ]; then
  echo "Using Python gRPC client..."
  if python3 "$PYTHON_SCRIPT" --host "$BANYANDB_GRPC" --measure "service_cpm_minute" --group "sw_metric" --points 3; then
    echo "Sample data written successfully!"
  else
    echo "Python script failed. Trying grpcurl as fallback..."
    USE_GRPCURL_FALLBACK=true
  fi
elif command -v grpcurl &> /dev/null; then
  USE_GRPCURL_FALLBACK=true
else
  USE_GRPCURL_FALLBACK=false
fi

# Fallback to grpcurl if Python script is not available or failed
if [ "$USE_GRPCURL_FALLBACK" = true ] && command -v grpcurl &> /dev/null; then
  # Check if proto files are available
  PROTO_BASE="$SCRIPT_DIR/../api/proto"
  USE_PROTO_FILES=false
  if [ -d "$PROTO_BASE" ] && [ -f "$PROTO_BASE/banyandb/measure/v1/rpc.proto" ]; then
    USE_PROTO_FILES=true
  fi

  # Get current time in seconds since epoch
  CURRENT_TIME=$(date +%s)
  
  # Create sample data points (3 data points with 1 minute intervals)
  SUCCESS_COUNT=0
  for i in {0..2}; do
    TIMESTAMP_SEC=$((CURRENT_TIME - (2 - i) * 60))
    MESSAGE_ID=$((CURRENT_TIME * 1000000000 + i))
    
    # Create data point JSON with RFC3339 timestamp format
    TIMESTAMP_ISO=$(date -u -r $TIMESTAMP_SEC +%Y-%m-%dT%H:%M:%SZ)
    
    DATA_POINT=$(cat <<EOF
{
  "metadata": {
    "name": "service_cpm_minute",
    "group": "sw_metric"
  },
  "data_point": {
    "timestamp": "$TIMESTAMP_ISO",
    "tag_families": [
      {
        "tags": [
          {
            "str": {
              "value": "svc-$(printf "%02d" $((i + 1)))"
            }
          },
          {
            "str": {
              "value": "entity-$(printf "%02d" $((i + 1)))"
            }
          },
          {
            "str": {
              "value": "service-$(printf "%02d" $((i + 1)))"
            }
          }
        ]
      }
    ],
    "fields": [
      {
        "int": {
          "value": $((100 + i * 10))
        }
      },
      {
        "int": {
          "value": $((i + 1))
        }
      }
    ],
    "version": $MESSAGE_ID
  },
  "message_id": $MESSAGE_ID
}
EOF
)
    
    # Write data point using grpcurl
    # Note: For streaming endpoints, grpcurl sends the message and waits for response
    echo -n "  Writing data point $((i + 1))/3..."
    
    # Try with proto files if available, otherwise try without (will fail if no reflection)
    # Note: Proto files may have dependency issues, so we try both approaches
    if [ "$USE_PROTO_FILES" = true ]; then
      # Try with proto files first
      RESPONSE=$(echo "$DATA_POINT" | grpcurl -plaintext -max-time 5 \
        -import-path "$PROTO_BASE" -import-path "$SCRIPT_DIR/../include" \
        -proto "$PROTO_BASE/banyandb/measure/v1/rpc.proto" \
        -d @ "$BANYANDB_GRPC" banyandb.measure.v1.MeasureService/Write 2>&1)
      # If proto approach fails, fall back to reflection (which will also likely fail)
      if echo "$RESPONSE" | grep -qi "could not parse\|no such file"; then
        RESPONSE=$(echo "$DATA_POINT" | grpcurl -plaintext -max-time 5 -d @ "$BANYANDB_GRPC" banyandb.measure.v1.MeasureService/Write 2>&1)
      fi
    else
      # Try without proto files (requires reflection API, which may not be enabled)
      RESPONSE=$(echo "$DATA_POINT" | grpcurl -plaintext -max-time 5 -d @ "$BANYANDB_GRPC" banyandb.measure.v1.MeasureService/Write 2>&1)
    fi
    EXIT_CODE=$?
    
    if [ $EXIT_CODE -eq 0 ] && (echo "$RESPONSE" | grep -q "message_id" || echo "$RESPONSE" | grep -q "status"); then
      echo " ✓"
      SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
      if echo "$RESPONSE" | grep -qi "connection\|refused\|timeout\|unavailable"; then
        echo " ✗ (connection error - is BanyanDB running on $BANYANDB_GRPC?)"
        break
      else
        echo " ✗"
        # Show first line of error if available
        ERROR_MSG=$(echo "$RESPONSE" | head -1)
        if [ -n "$ERROR_MSG" ]; then
          echo "    Error: $ERROR_MSG"
        fi
      fi
    fi
    # Small delay between writes
    sleep 0.2
  done
  
  if [ $SUCCESS_COUNT -gt 0 ]; then
    echo "  Successfully wrote $SUCCESS_COUNT data point(s)!"
  fi
else
  echo "  ⚠ grpcurl not found. Skipping data insertion."
  echo ""
  echo "To write sample data, install grpcurl:"
  echo "  macOS:   brew install grpcurl"
  echo "  Linux:   See https://github.com/fullstorydev/grpcurl/releases"
  echo ""
  echo "The Write endpoint is a streaming gRPC service:"
  echo "  Address: $BANYANDB_GRPC"
  echo "  Service: banyandb.measure.v1.MeasureService/Write"
fi

echo ""
echo "Setup complete! You can now:"
echo "1. List groups: curl $BANYANDB_URL/v1/group/schema/lists"
echo "2. List streams: curl $BANYANDB_URL/v1/stream/schema/lists/default"
echo "3. List measures: curl $BANYANDB_URL/v1/measure/schema/lists/sw_metric"
echo "4. Query measure data: curl -X POST $BANYANDB_URL/v1/bydbql/query -H 'Content-Type: application/json' -d '{\"query\": \"SELECT * FROM MEASURE service_cpm_minute IN sw_metric TIME > \\\"-1h\\\"\"}'"
echo ""
echo "Note: Measure write requires gRPC (port 17912). Use grpcurl or a gRPC client."

