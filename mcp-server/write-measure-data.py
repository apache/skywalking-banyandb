#!/usr/bin/env python3
"""
Script to write sample data to service_cpm_minute measure using gRPC.
This script generates Python gRPC stubs from proto files and uses them to write data.
"""
import sys
import os
import subprocess
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Check for required packages
try:
    import grpc
    from google.protobuf import timestamp_pb2
except ImportError as e:
    print(f"Error: Missing required package: {e}")
    print("Install with: pip install grpcio protobuf grpcio-tools")
    sys.exit(1)

def generate_python_stubs(proto_base_dir, output_dir, repo_root):
    """Generate Python gRPC stubs from proto files"""
    try:
        from grpc_tools import protoc
    except ImportError:
        print("Error: grpcio-tools not installed")
        print("Install with: pip install grpcio-tools")
        sys.exit(1)
    
    proto_base = Path(proto_base_dir)
    output = Path(output_dir)
    repo_root = Path(repo_root)
    output.mkdir(parents=True, exist_ok=True)
    
    # Proto files we need (relative to api/proto)
    # We only need write.proto and its dependencies for writing data
    # rpc.proto has HTTP annotations that require google/api which we'll skip
    proto_files = [
        "banyandb/common/v1/common.proto",  # Base, no dependencies on other banyandb protos
        "banyandb/model/v1/common.proto",    # Depends on common
        "banyandb/measure/v1/write.proto",   # Depends on common and model - this is what we need!
    ]
    
    # Import paths - protoc needs paths where proto files can be found
    # Proto files import like "banyandb/common/v1/common.proto"
    # So we need api/proto as import path (not api/)
    api_dir = proto_base.parent  # api/ directory  
    include_dir = repo_root / "include"
    
    import_paths = [str(proto_base)]  # api/proto - where banyandb/ protos are
    if include_dir.exists():
        import_paths.append(str(include_dir))  # For google/protobuf imports
    
    # Create a minimal validate.proto stub if it doesn't exist
    # This is needed because proto files import validate/validate.proto
    # The path structure must be: <import_path>/validate/validate.proto
    temp_validate_base = Path("/tmp/validate_banyandb")
    validate_dir = temp_validate_base / "validate"
    validate_dir.mkdir(parents=True, exist_ok=True)
    validate_stub = validate_dir / "validate.proto"
    if not validate_stub.exists():
        # Create minimal validate.proto stub
        # This is a simplified version - full version would be from protoc-gen-validate
        validate_stub.write_text("""syntax = "proto3";
package validate;
import "google/protobuf/descriptor.proto";
extend google.protobuf.FieldOptions {
    FieldRules rules = 1071;
}
message FieldRules {
    message Timestamp { bool required = 1; }
    message Repeated { int32 min_items = 1; }
    message UInt64 { uint64 gt = 1; }
    message UInt32 { uint32 gt = 1; }
    message Int32 { int32 gt = 1; }
    message String { int32 min_len = 1; }
    message Message { bool required = 1; }
    message Enum { bool defined_only = 1; }
    Timestamp timestamp = 17;
    Repeated repeated = 18;
    UInt64 uint64 = 19;
    UInt32 uint32 = 20;
    Int32 int32 = 21;
    String string = 22;
    Message message = 23;
    Enum enum = 24;
}
extend google.protobuf.MessageOptions {
    bool required = 1071;
}
""")
    import_paths.append(str(temp_validate_base))
    
    # Build protoc command arguments - generate all files together
    args = []
    for imp_path in import_paths:
        args.append(f'--proto_path={imp_path}')
    args.extend([
        f'--python_out={output}',
        f'--grpc_python_out={output}',
    ])
    
    # Add all proto files
    proto_paths = []
    for proto_file in proto_files:
        proto_path = proto_base / proto_file
        if not proto_path.exists():
            print(f"Warning: Proto file not found: {proto_path}")
            continue
        # Proto file path relative to proto_base (api/proto)
        rel_proto_path = proto_path.relative_to(proto_base)
        proto_paths.append(str(rel_proto_path))
    
    if not proto_paths:
        print("Error: No proto files found to generate")
        return False
    
    args.extend(proto_paths)
    
    try:
        # Generate all proto files together so imports resolve correctly
        cmd = [sys.executable, '-m', 'grpc_tools.protoc'] + args
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print("Warning: Failed to generate stubs")
            if result.stderr:
                print(f"  Error: {result.stderr[:500]}")
            return False
        return True
    except Exception as e:
        print(f"Error generating stubs: {e}")
        return False

def write_sample_data(host_port="localhost:17912", measure_name="service_cpm_minute", 
                     group_name="sw_metric", num_points=3):
    """Write sample data points to a measure"""
    
    # Determine script location and proto paths
    script_dir = Path(__file__).parent.absolute()
    repo_root = script_dir.parent
    proto_base = repo_root / "api" / "proto"
    stubs_dir = script_dir / "generated_stubs"
    
    # Check if proto files exist
    if not proto_base.exists():
        print(f"Error: Proto directory not found: {proto_base}")
        print("Make sure you're running this from the mcp-server directory")
        return False
    
    # Generate stubs if they don't exist
    # Also create validate proto stub location
    temp_validate_base = Path("/tmp/validate_banyandb")
    
    if not (stubs_dir / "banyandb" / "measure" / "v1" / "write_pb2.py").exists():
        print("Generating Python gRPC stubs from proto files...")
        if not generate_python_stubs(proto_base, stubs_dir, repo_root):
            print("Failed to generate stubs. Trying to use existing ones...")
            if not (stubs_dir / "banyandb").exists():
                print("Error: No stubs available and generation failed")
                return False
    
    # Generate validate_pb2.py from our validate.proto stub
    validate_proto_path = temp_validate_base / "validate" / "validate.proto"
    if validate_proto_path.exists() and not (stubs_dir / "validate" / "validate_pb2.py").exists():
        validate_output = stubs_dir / "validate"
        validate_output.mkdir(parents=True, exist_ok=True)
        include_dir = repo_root / "include"
        args_validate = [
            f'--proto_path={proto_base}',
            f'--proto_path={temp_validate_base}',
        ]
        if include_dir.exists():
            args_validate.append(f'--proto_path={include_dir}')
        args_validate.extend([
            f'--python_out={stubs_dir}',
            str(validate_proto_path.relative_to(temp_validate_base)),
        ])
        try:
            cmd = [sys.executable, '-m', 'grpc_tools.protoc'] + args_validate
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"Warning: Failed to generate validate_pb2: {result.stderr[:200]}")
        except Exception as e:
            print(f"Warning: Error generating validate_pb2: {e}")
    
    # Add stubs directory to path
    sys.path.insert(0, str(stubs_dir))
    
    try:
        # Import generated modules
        from banyandb.common.v1 import common_pb2
        from banyandb.model.v1 import common_pb2 as model_common_pb2
        from banyandb.measure.v1 import write_pb2
    except ImportError as e:
        print(f"Error importing generated stubs: {e}")
        print("Try regenerating stubs or check proto file paths")
        import traceback
        traceback.print_exc()
        return False
    
    # We need to manually create the gRPC stub since we didn't generate rpc_pb2_grpc
    # We'll use the service name directly
    try:
        channel = grpc.insecure_channel(host_port)
        # Create stub using the service descriptor
        # The service name is: banyandb.measure.v1.MeasureService
        # Method name is: Write
        # We'll call it directly using the channel
        
        # Test connection
        try:
            grpc.channel_ready_future(channel).result(timeout=5)
        except grpc.FutureTimeoutError:
            print(f"Error: Connection timeout to {host_port}")
            print("Make sure BanyanDB is running and accessible")
            return False
        
        # Get current time
        current_time = datetime.now(timezone.utc)
        success_count = 0
        
        # Prepare all write requests first
        write_requests = []
        for i in range(num_points):
            # Calculate timestamp (going back in time, 1 minute intervals)
            # Truncate to minute boundary since measure has interval "1m"
            # Use timestamps that are definitely in the past (at least 2 minutes ago)
            # to avoid any timing/validation issues
            minutes_ago = (num_points - i - 1) + 2
            timestamp_dt = current_time - timedelta(minutes=minutes_ago)
            # Truncate to minute (remove seconds and microseconds)
            timestamp_dt = timestamp_dt.replace(second=0, microsecond=0)
            
            # Create timestamp from Unix milliseconds to ensure millisecond precision
            # BanyanDB requires timestamps to have millisecond precision (nanos % 1000000 == 0)
            unix_ms = int(timestamp_dt.timestamp() * 1000)
            timestamp = timestamp_pb2.Timestamp()
            timestamp.FromMilliseconds(unix_ms)
            
            # Create metadata
            metadata = common_pb2.Metadata()
            metadata.name = measure_name
            metadata.group = group_name
            
            # Create tag family with tags (id, entity_id, service_id)
            tag_family = model_common_pb2.TagFamilyForWrite()
            
            # Tag 1: id
            tag1 = model_common_pb2.TagValue()
            tag1.str.value = f"svc-{i+1:02d}"
            tag_family.tags.append(tag1)
            
            # Tag 2: entity_id
            tag2 = model_common_pb2.TagValue()
            tag2.str.value = f"entity-{i+1:02d}"
            tag_family.tags.append(tag2)
            
            # Tag 3: service_id
            tag3 = model_common_pb2.TagValue()
            tag3.str.value = f"service-{i+1:02d}"
            tag_family.tags.append(tag3)
            
            # Create data point
            data_point = write_pb2.DataPointValue()
            data_point.timestamp.CopyFrom(timestamp)
            data_point.tag_families.append(tag_family)
            
            # Add fields (total, value)
            field1 = model_common_pb2.FieldValue()
            field1.int.value = 100 + i * 10  # total
            data_point.fields.append(field1)
            
            field2 = model_common_pb2.FieldValue()
            field2.int.value = i + 1  # value
            data_point.fields.append(field2)
            
            # Set version
            message_id = int(time.time() * 1000000000) + i
            data_point.version = message_id
            
            # Create write request
            write_request = write_pb2.WriteRequest()
            write_request.metadata.CopyFrom(metadata)
            write_request.data_point.CopyFrom(data_point)
            write_request.message_id = message_id
            write_requests.append(write_request)
        
        # Send requests using streaming gRPC call
        # The Write endpoint is bidirectional streaming - send requests and receive responses
        try:
            print("  Sending data points via streaming gRPC...")
            method = '/banyandb.measure.v1.MeasureService/Write'
            call = channel.stream_stream(method, request_serializer=write_pb2.WriteRequest.SerializeToString,
                                        response_deserializer=write_pb2.WriteResponse.FromString)
            
            # Send requests one by one and wait for responses
            # This ensures proper streaming behavior
            def request_generator():
                for req in write_requests:
                    yield req
            
            response_stream = call(request_generator())
            
            # Read responses as they come
            responses = {}
            try:
                for resp in response_stream:
                    responses[resp.message_id] = resp.status
                    print(f"  Response for message {resp.message_id}: {resp.status}")
            except grpc.RpcError as stream_err:
                print(f"  Stream error: {stream_err.code()} - {stream_err.details()}")
            
            # Check success and print results
            for i, req in enumerate(write_requests):
                status = responses.get(req.message_id, 'NO_RESPONSE')
                print(f"  Data point {i+1}/{num_points}: {status}")
                if 'SUCCEED' in status or status == '':
                    success_count += 1
                elif 'INVALID_TIMESTAMP' in status:
                    # Print timestamp details for debugging
                    ts = req.data_point.timestamp
                    print(f"    Timestamp details: seconds={ts.seconds}, nanos={ts.nanos}, nanos%1000000={ts.nanos % 1000000}")
                    print(f"    Timestamp as datetime: {datetime.fromtimestamp(ts.seconds + ts.nanos/1e9, tz=timezone.utc)}")
            
        except grpc.RpcError as e:
            print(f" ✗")
            print(f"    Error: {e.code()} - {e.details()}")
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                print(f"    Make sure BanyanDB is running on {host_port}")
        except Exception as e:
            print(f" ✗")
            print(f"    Unexpected error: {e}")
            import traceback
            traceback.print_exc()
        
        channel.close()
        
        if success_count > 0:
            print(f"\nSuccessfully wrote {success_count} data point(s)!")
            return True
        else:
            print("\nNo data points were written successfully")
            return False
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Write sample data to BanyanDB measure")
    parser.add_argument("--host", default="localhost:17912", help="BanyanDB gRPC address")
    parser.add_argument("--measure", default="service_cpm_minute", help="Measure name")
    parser.add_argument("--group", default="sw_metric", help="Group name")
    parser.add_argument("--points", type=int, default=3, help="Number of data points to write")
    
    args = parser.parse_args()
    
    success = write_sample_data(args.host, args.measure, args.group, args.points)
    sys.exit(0 if success else 1)
