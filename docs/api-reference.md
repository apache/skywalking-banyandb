# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [banyandb/common/v1/trace.proto](#banyandb_common_v1_trace-proto)
    - [Span](#banyandb-common-v1-Span)
    - [Tag](#banyandb-common-v1-Tag)
    - [Trace](#banyandb-common-v1-Trace)
  
- [banyandb/model/v1/common.proto](#banyandb_model_v1_common-proto)
    - [FieldValue](#banyandb-model-v1-FieldValue)
    - [Float](#banyandb-model-v1-Float)
    - [Int](#banyandb-model-v1-Int)
    - [IntArray](#banyandb-model-v1-IntArray)
    - [Str](#banyandb-model-v1-Str)
    - [StrArray](#banyandb-model-v1-StrArray)
    - [TagFamilyForWrite](#banyandb-model-v1-TagFamilyForWrite)
    - [TagValue](#banyandb-model-v1-TagValue)
  
    - [AggregationFunction](#banyandb-model-v1-AggregationFunction)
  
- [banyandb/model/v1/query.proto](#banyandb_model_v1_query-proto)
    - [Condition](#banyandb-model-v1-Condition)
    - [Condition.MatchOption](#banyandb-model-v1-Condition-MatchOption)
    - [Criteria](#banyandb-model-v1-Criteria)
    - [LogicalExpression](#banyandb-model-v1-LogicalExpression)
    - [QueryOrder](#banyandb-model-v1-QueryOrder)
    - [Tag](#banyandb-model-v1-Tag)
    - [TagFamily](#banyandb-model-v1-TagFamily)
    - [TagProjection](#banyandb-model-v1-TagProjection)
    - [TagProjection.TagFamily](#banyandb-model-v1-TagProjection-TagFamily)
    - [TimeRange](#banyandb-model-v1-TimeRange)
  
    - [Condition.BinaryOp](#banyandb-model-v1-Condition-BinaryOp)
    - [Condition.MatchOption.Operator](#banyandb-model-v1-Condition-MatchOption-Operator)
    - [LogicalExpression.LogicalOp](#banyandb-model-v1-LogicalExpression-LogicalOp)
    - [Sort](#banyandb-model-v1-Sort)
  
- [banyandb/measure/v1/query.proto](#banyandb_measure_v1_query-proto)
    - [DataPoint](#banyandb-measure-v1-DataPoint)
    - [DataPoint.Field](#banyandb-measure-v1-DataPoint-Field)
    - [InternalDataPoint](#banyandb-measure-v1-InternalDataPoint)
    - [InternalQueryRequest](#banyandb-measure-v1-InternalQueryRequest)
    - [InternalQueryResponse](#banyandb-measure-v1-InternalQueryResponse)
    - [QueryRequest](#banyandb-measure-v1-QueryRequest)
    - [QueryRequest.Aggregation](#banyandb-measure-v1-QueryRequest-Aggregation)
    - [QueryRequest.FieldProjection](#banyandb-measure-v1-QueryRequest-FieldProjection)
    - [QueryRequest.GroupBy](#banyandb-measure-v1-QueryRequest-GroupBy)
    - [QueryRequest.Top](#banyandb-measure-v1-QueryRequest-Top)
    - [QueryResponse](#banyandb-measure-v1-QueryResponse)
  
- [banyandb/measure/v1/topn.proto](#banyandb_measure_v1_topn-proto)
    - [TopNList](#banyandb-measure-v1-TopNList)
    - [TopNList.Item](#banyandb-measure-v1-TopNList-Item)
    - [TopNRequest](#banyandb-measure-v1-TopNRequest)
    - [TopNResponse](#banyandb-measure-v1-TopNResponse)
  
- [banyandb/common/v1/common.proto](#banyandb_common_v1_common-proto)
    - [Group](#banyandb-common-v1-Group)
    - [IntervalRule](#banyandb-common-v1-IntervalRule)
    - [LifecycleStage](#banyandb-common-v1-LifecycleStage)
    - [Metadata](#banyandb-common-v1-Metadata)
    - [ResourceOpts](#banyandb-common-v1-ResourceOpts)
  
    - [Catalog](#banyandb-common-v1-Catalog)
    - [IntervalRule.Unit](#banyandb-common-v1-IntervalRule-Unit)
  
- [banyandb/property/v1/property.proto](#banyandb_property_v1_property-proto)
    - [Property](#banyandb-property-v1-Property)
  
- [banyandb/property/v1/rpc.proto](#banyandb_property_v1_rpc-proto)
    - [ApplyRequest](#banyandb-property-v1-ApplyRequest)
    - [ApplyResponse](#banyandb-property-v1-ApplyResponse)
    - [DeleteRequest](#banyandb-property-v1-DeleteRequest)
    - [DeleteResponse](#banyandb-property-v1-DeleteResponse)
    - [InternalDeleteRequest](#banyandb-property-v1-InternalDeleteRequest)
    - [InternalQueryResponse](#banyandb-property-v1-InternalQueryResponse)
    - [InternalRepairRequest](#banyandb-property-v1-InternalRepairRequest)
    - [InternalRepairResponse](#banyandb-property-v1-InternalRepairResponse)
    - [InternalUpdateRequest](#banyandb-property-v1-InternalUpdateRequest)
    - [QueryOrder](#banyandb-property-v1-QueryOrder)
    - [QueryRequest](#banyandb-property-v1-QueryRequest)
    - [QueryResponse](#banyandb-property-v1-QueryResponse)
  
    - [ApplyRequest.Strategy](#banyandb-property-v1-ApplyRequest-Strategy)
  
    - [PropertyService](#banyandb-property-v1-PropertyService)
  
- [banyandb/stream/v1/query.proto](#banyandb_stream_v1_query-proto)
    - [Element](#banyandb-stream-v1-Element)
    - [QueryRequest](#banyandb-stream-v1-QueryRequest)
    - [QueryResponse](#banyandb-stream-v1-QueryResponse)
  
- [banyandb/trace/v1/query.proto](#banyandb_trace_v1_query-proto)
    - [InternalQueryResponse](#banyandb-trace-v1-InternalQueryResponse)
    - [InternalTrace](#banyandb-trace-v1-InternalTrace)
    - [QueryRequest](#banyandb-trace-v1-QueryRequest)
    - [QueryResponse](#banyandb-trace-v1-QueryResponse)
    - [Span](#banyandb-trace-v1-Span)
    - [Trace](#banyandb-trace-v1-Trace)
  
- [banyandb/bydbql/v1/query.proto](#banyandb_bydbql_v1_query-proto)
    - [QueryRequest](#banyandb-bydbql-v1-QueryRequest)
    - [QueryResponse](#banyandb-bydbql-v1-QueryResponse)
  
- [banyandb/bydbql/v1/rpc.proto](#banyandb_bydbql_v1_rpc-proto)
    - [BydbQLService](#banyandb-bydbql-v1-BydbQLService)
  
- [banyandb/model/v1/write.proto](#banyandb_model_v1_write-proto)
    - [Status](#banyandb-model-v1-Status)
  
- [banyandb/cluster/v1/rpc.proto](#banyandb_cluster_v1_rpc-proto)
    - [FileInfo](#banyandb-cluster-v1-FileInfo)
    - [HealthCheckRequest](#banyandb-cluster-v1-HealthCheckRequest)
    - [HealthCheckResponse](#banyandb-cluster-v1-HealthCheckResponse)
    - [PartInfo](#banyandb-cluster-v1-PartInfo)
    - [PartResult](#banyandb-cluster-v1-PartResult)
    - [SendRequest](#banyandb-cluster-v1-SendRequest)
    - [SendResponse](#banyandb-cluster-v1-SendResponse)
    - [SyncCompletion](#banyandb-cluster-v1-SyncCompletion)
    - [SyncMetadata](#banyandb-cluster-v1-SyncMetadata)
    - [SyncPartRequest](#banyandb-cluster-v1-SyncPartRequest)
    - [SyncPartResponse](#banyandb-cluster-v1-SyncPartResponse)
    - [SyncResult](#banyandb-cluster-v1-SyncResult)
    - [VersionCompatibility](#banyandb-cluster-v1-VersionCompatibility)
    - [VersionInfo](#banyandb-cluster-v1-VersionInfo)
  
    - [SyncStatus](#banyandb-cluster-v1-SyncStatus)
  
    - [ChunkedSyncService](#banyandb-cluster-v1-ChunkedSyncService)
    - [Service](#banyandb-cluster-v1-Service)
  
- [banyandb/common/v1/rpc.proto](#banyandb_common_v1_rpc-proto)
    - [APIVersion](#banyandb-common-v1-APIVersion)
    - [GetAPIVersionRequest](#banyandb-common-v1-GetAPIVersionRequest)
    - [GetAPIVersionResponse](#banyandb-common-v1-GetAPIVersionResponse)
  
    - [Service](#banyandb-common-v1-Service)
  
- [banyandb/database/v1/database.proto](#banyandb_database_v1_database-proto)
    - [Node](#banyandb-database-v1-Node)
    - [Node.LabelsEntry](#banyandb-database-v1-Node-LabelsEntry)
    - [Shard](#banyandb-database-v1-Shard)
  
    - [Role](#banyandb-database-v1-Role)
  
- [banyandb/database/v1/schema.proto](#banyandb_database_v1_schema-proto)
    - [Entity](#banyandb-database-v1-Entity)
    - [FieldSpec](#banyandb-database-v1-FieldSpec)
    - [IndexRule](#banyandb-database-v1-IndexRule)
    - [IndexRuleBinding](#banyandb-database-v1-IndexRuleBinding)
    - [Measure](#banyandb-database-v1-Measure)
    - [Property](#banyandb-database-v1-Property)
    - [ShardingKey](#banyandb-database-v1-ShardingKey)
    - [Stream](#banyandb-database-v1-Stream)
    - [Subject](#banyandb-database-v1-Subject)
    - [TagFamilySpec](#banyandb-database-v1-TagFamilySpec)
    - [TagSpec](#banyandb-database-v1-TagSpec)
    - [TopNAggregation](#banyandb-database-v1-TopNAggregation)
    - [Trace](#banyandb-database-v1-Trace)
    - [TraceTagSpec](#banyandb-database-v1-TraceTagSpec)
  
    - [CompressionMethod](#banyandb-database-v1-CompressionMethod)
    - [EncodingMethod](#banyandb-database-v1-EncodingMethod)
    - [FieldType](#banyandb-database-v1-FieldType)
    - [IndexRule.Type](#banyandb-database-v1-IndexRule-Type)
    - [TagType](#banyandb-database-v1-TagType)
  
- [banyandb/database/v1/rpc.proto](#banyandb_database_v1_rpc-proto)
    - [DataInfo](#banyandb-database-v1-DataInfo)
    - [GetClusterStateRequest](#banyandb-database-v1-GetClusterStateRequest)
    - [GetClusterStateResponse](#banyandb-database-v1-GetClusterStateResponse)
    - [GetClusterStateResponse.RouteTablesEntry](#banyandb-database-v1-GetClusterStateResponse-RouteTablesEntry)
    - [GetCurrentNodeRequest](#banyandb-database-v1-GetCurrentNodeRequest)
    - [GetCurrentNodeResponse](#banyandb-database-v1-GetCurrentNodeResponse)
    - [GroupDeletionTask](#banyandb-database-v1-GroupDeletionTask)
    - [GroupDeletionTask.DeletedCountsEntry](#banyandb-database-v1-GroupDeletionTask-DeletedCountsEntry)
    - [GroupDeletionTask.TotalCountsEntry](#banyandb-database-v1-GroupDeletionTask-TotalCountsEntry)
    - [GroupRegistryServiceCreateRequest](#banyandb-database-v1-GroupRegistryServiceCreateRequest)
    - [GroupRegistryServiceCreateResponse](#banyandb-database-v1-GroupRegistryServiceCreateResponse)
    - [GroupRegistryServiceDeleteRequest](#banyandb-database-v1-GroupRegistryServiceDeleteRequest)
    - [GroupRegistryServiceDeleteResponse](#banyandb-database-v1-GroupRegistryServiceDeleteResponse)
    - [GroupRegistryServiceExistRequest](#banyandb-database-v1-GroupRegistryServiceExistRequest)
    - [GroupRegistryServiceExistResponse](#banyandb-database-v1-GroupRegistryServiceExistResponse)
    - [GroupRegistryServiceGetRequest](#banyandb-database-v1-GroupRegistryServiceGetRequest)
    - [GroupRegistryServiceGetResponse](#banyandb-database-v1-GroupRegistryServiceGetResponse)
    - [GroupRegistryServiceInspectRequest](#banyandb-database-v1-GroupRegistryServiceInspectRequest)
    - [GroupRegistryServiceInspectResponse](#banyandb-database-v1-GroupRegistryServiceInspectResponse)
    - [GroupRegistryServiceListRequest](#banyandb-database-v1-GroupRegistryServiceListRequest)
    - [GroupRegistryServiceListResponse](#banyandb-database-v1-GroupRegistryServiceListResponse)
    - [GroupRegistryServiceQueryRequest](#banyandb-database-v1-GroupRegistryServiceQueryRequest)
    - [GroupRegistryServiceQueryResponse](#banyandb-database-v1-GroupRegistryServiceQueryResponse)
    - [GroupRegistryServiceUpdateRequest](#banyandb-database-v1-GroupRegistryServiceUpdateRequest)
    - [GroupRegistryServiceUpdateResponse](#banyandb-database-v1-GroupRegistryServiceUpdateResponse)
    - [IndexRuleBindingRegistryServiceCreateRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceCreateRequest)
    - [IndexRuleBindingRegistryServiceCreateResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceCreateResponse)
    - [IndexRuleBindingRegistryServiceDeleteRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceDeleteRequest)
    - [IndexRuleBindingRegistryServiceDeleteResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceDeleteResponse)
    - [IndexRuleBindingRegistryServiceExistRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceExistRequest)
    - [IndexRuleBindingRegistryServiceExistResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceExistResponse)
    - [IndexRuleBindingRegistryServiceGetRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceGetRequest)
    - [IndexRuleBindingRegistryServiceGetResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceGetResponse)
    - [IndexRuleBindingRegistryServiceListRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceListRequest)
    - [IndexRuleBindingRegistryServiceListResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceListResponse)
    - [IndexRuleBindingRegistryServiceUpdateRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceUpdateRequest)
    - [IndexRuleBindingRegistryServiceUpdateResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceUpdateResponse)
    - [IndexRuleRegistryServiceCreateRequest](#banyandb-database-v1-IndexRuleRegistryServiceCreateRequest)
    - [IndexRuleRegistryServiceCreateResponse](#banyandb-database-v1-IndexRuleRegistryServiceCreateResponse)
    - [IndexRuleRegistryServiceDeleteRequest](#banyandb-database-v1-IndexRuleRegistryServiceDeleteRequest)
    - [IndexRuleRegistryServiceDeleteResponse](#banyandb-database-v1-IndexRuleRegistryServiceDeleteResponse)
    - [IndexRuleRegistryServiceExistRequest](#banyandb-database-v1-IndexRuleRegistryServiceExistRequest)
    - [IndexRuleRegistryServiceExistResponse](#banyandb-database-v1-IndexRuleRegistryServiceExistResponse)
    - [IndexRuleRegistryServiceGetRequest](#banyandb-database-v1-IndexRuleRegistryServiceGetRequest)
    - [IndexRuleRegistryServiceGetResponse](#banyandb-database-v1-IndexRuleRegistryServiceGetResponse)
    - [IndexRuleRegistryServiceListRequest](#banyandb-database-v1-IndexRuleRegistryServiceListRequest)
    - [IndexRuleRegistryServiceListResponse](#banyandb-database-v1-IndexRuleRegistryServiceListResponse)
    - [IndexRuleRegistryServiceUpdateRequest](#banyandb-database-v1-IndexRuleRegistryServiceUpdateRequest)
    - [IndexRuleRegistryServiceUpdateResponse](#banyandb-database-v1-IndexRuleRegistryServiceUpdateResponse)
    - [InvertedIndexInfo](#banyandb-database-v1-InvertedIndexInfo)
    - [LiaisonInfo](#banyandb-database-v1-LiaisonInfo)
    - [MeasureRegistryServiceCreateRequest](#banyandb-database-v1-MeasureRegistryServiceCreateRequest)
    - [MeasureRegistryServiceCreateResponse](#banyandb-database-v1-MeasureRegistryServiceCreateResponse)
    - [MeasureRegistryServiceDeleteRequest](#banyandb-database-v1-MeasureRegistryServiceDeleteRequest)
    - [MeasureRegistryServiceDeleteResponse](#banyandb-database-v1-MeasureRegistryServiceDeleteResponse)
    - [MeasureRegistryServiceExistRequest](#banyandb-database-v1-MeasureRegistryServiceExistRequest)
    - [MeasureRegistryServiceExistResponse](#banyandb-database-v1-MeasureRegistryServiceExistResponse)
    - [MeasureRegistryServiceGetRequest](#banyandb-database-v1-MeasureRegistryServiceGetRequest)
    - [MeasureRegistryServiceGetResponse](#banyandb-database-v1-MeasureRegistryServiceGetResponse)
    - [MeasureRegistryServiceListRequest](#banyandb-database-v1-MeasureRegistryServiceListRequest)
    - [MeasureRegistryServiceListResponse](#banyandb-database-v1-MeasureRegistryServiceListResponse)
    - [MeasureRegistryServiceUpdateRequest](#banyandb-database-v1-MeasureRegistryServiceUpdateRequest)
    - [MeasureRegistryServiceUpdateResponse](#banyandb-database-v1-MeasureRegistryServiceUpdateResponse)
    - [PropertyRegistryServiceCreateRequest](#banyandb-database-v1-PropertyRegistryServiceCreateRequest)
    - [PropertyRegistryServiceCreateResponse](#banyandb-database-v1-PropertyRegistryServiceCreateResponse)
    - [PropertyRegistryServiceDeleteRequest](#banyandb-database-v1-PropertyRegistryServiceDeleteRequest)
    - [PropertyRegistryServiceDeleteResponse](#banyandb-database-v1-PropertyRegistryServiceDeleteResponse)
    - [PropertyRegistryServiceExistRequest](#banyandb-database-v1-PropertyRegistryServiceExistRequest)
    - [PropertyRegistryServiceExistResponse](#banyandb-database-v1-PropertyRegistryServiceExistResponse)
    - [PropertyRegistryServiceGetRequest](#banyandb-database-v1-PropertyRegistryServiceGetRequest)
    - [PropertyRegistryServiceGetResponse](#banyandb-database-v1-PropertyRegistryServiceGetResponse)
    - [PropertyRegistryServiceListRequest](#banyandb-database-v1-PropertyRegistryServiceListRequest)
    - [PropertyRegistryServiceListResponse](#banyandb-database-v1-PropertyRegistryServiceListResponse)
    - [PropertyRegistryServiceUpdateRequest](#banyandb-database-v1-PropertyRegistryServiceUpdateRequest)
    - [PropertyRegistryServiceUpdateResponse](#banyandb-database-v1-PropertyRegistryServiceUpdateResponse)
    - [RouteTable](#banyandb-database-v1-RouteTable)
    - [SIDXInfo](#banyandb-database-v1-SIDXInfo)
    - [SchemaInfo](#banyandb-database-v1-SchemaInfo)
    - [SegmentInfo](#banyandb-database-v1-SegmentInfo)
    - [SeriesIndexInfo](#banyandb-database-v1-SeriesIndexInfo)
    - [ShardInfo](#banyandb-database-v1-ShardInfo)
    - [Snapshot](#banyandb-database-v1-Snapshot)
    - [SnapshotRequest](#banyandb-database-v1-SnapshotRequest)
    - [SnapshotRequest.Group](#banyandb-database-v1-SnapshotRequest-Group)
    - [SnapshotResponse](#banyandb-database-v1-SnapshotResponse)
    - [StreamRegistryServiceCreateRequest](#banyandb-database-v1-StreamRegistryServiceCreateRequest)
    - [StreamRegistryServiceCreateResponse](#banyandb-database-v1-StreamRegistryServiceCreateResponse)
    - [StreamRegistryServiceDeleteRequest](#banyandb-database-v1-StreamRegistryServiceDeleteRequest)
    - [StreamRegistryServiceDeleteResponse](#banyandb-database-v1-StreamRegistryServiceDeleteResponse)
    - [StreamRegistryServiceExistRequest](#banyandb-database-v1-StreamRegistryServiceExistRequest)
    - [StreamRegistryServiceExistResponse](#banyandb-database-v1-StreamRegistryServiceExistResponse)
    - [StreamRegistryServiceGetRequest](#banyandb-database-v1-StreamRegistryServiceGetRequest)
    - [StreamRegistryServiceGetResponse](#banyandb-database-v1-StreamRegistryServiceGetResponse)
    - [StreamRegistryServiceListRequest](#banyandb-database-v1-StreamRegistryServiceListRequest)
    - [StreamRegistryServiceListResponse](#banyandb-database-v1-StreamRegistryServiceListResponse)
    - [StreamRegistryServiceUpdateRequest](#banyandb-database-v1-StreamRegistryServiceUpdateRequest)
    - [StreamRegistryServiceUpdateResponse](#banyandb-database-v1-StreamRegistryServiceUpdateResponse)
    - [TopNAggregationRegistryServiceCreateRequest](#banyandb-database-v1-TopNAggregationRegistryServiceCreateRequest)
    - [TopNAggregationRegistryServiceCreateResponse](#banyandb-database-v1-TopNAggregationRegistryServiceCreateResponse)
    - [TopNAggregationRegistryServiceDeleteRequest](#banyandb-database-v1-TopNAggregationRegistryServiceDeleteRequest)
    - [TopNAggregationRegistryServiceDeleteResponse](#banyandb-database-v1-TopNAggregationRegistryServiceDeleteResponse)
    - [TopNAggregationRegistryServiceExistRequest](#banyandb-database-v1-TopNAggregationRegistryServiceExistRequest)
    - [TopNAggregationRegistryServiceExistResponse](#banyandb-database-v1-TopNAggregationRegistryServiceExistResponse)
    - [TopNAggregationRegistryServiceGetRequest](#banyandb-database-v1-TopNAggregationRegistryServiceGetRequest)
    - [TopNAggregationRegistryServiceGetResponse](#banyandb-database-v1-TopNAggregationRegistryServiceGetResponse)
    - [TopNAggregationRegistryServiceListRequest](#banyandb-database-v1-TopNAggregationRegistryServiceListRequest)
    - [TopNAggregationRegistryServiceListResponse](#banyandb-database-v1-TopNAggregationRegistryServiceListResponse)
    - [TopNAggregationRegistryServiceUpdateRequest](#banyandb-database-v1-TopNAggregationRegistryServiceUpdateRequest)
    - [TopNAggregationRegistryServiceUpdateResponse](#banyandb-database-v1-TopNAggregationRegistryServiceUpdateResponse)
    - [TraceRegistryServiceCreateRequest](#banyandb-database-v1-TraceRegistryServiceCreateRequest)
    - [TraceRegistryServiceCreateResponse](#banyandb-database-v1-TraceRegistryServiceCreateResponse)
    - [TraceRegistryServiceDeleteRequest](#banyandb-database-v1-TraceRegistryServiceDeleteRequest)
    - [TraceRegistryServiceDeleteResponse](#banyandb-database-v1-TraceRegistryServiceDeleteResponse)
    - [TraceRegistryServiceExistRequest](#banyandb-database-v1-TraceRegistryServiceExistRequest)
    - [TraceRegistryServiceExistResponse](#banyandb-database-v1-TraceRegistryServiceExistResponse)
    - [TraceRegistryServiceGetRequest](#banyandb-database-v1-TraceRegistryServiceGetRequest)
    - [TraceRegistryServiceGetResponse](#banyandb-database-v1-TraceRegistryServiceGetResponse)
    - [TraceRegistryServiceListRequest](#banyandb-database-v1-TraceRegistryServiceListRequest)
    - [TraceRegistryServiceListResponse](#banyandb-database-v1-TraceRegistryServiceListResponse)
    - [TraceRegistryServiceUpdateRequest](#banyandb-database-v1-TraceRegistryServiceUpdateRequest)
    - [TraceRegistryServiceUpdateResponse](#banyandb-database-v1-TraceRegistryServiceUpdateResponse)
  
    - [GroupDeletionTask.Phase](#banyandb-database-v1-GroupDeletionTask-Phase)
  
    - [ClusterStateService](#banyandb-database-v1-ClusterStateService)
    - [GroupRegistryService](#banyandb-database-v1-GroupRegistryService)
    - [IndexRuleBindingRegistryService](#banyandb-database-v1-IndexRuleBindingRegistryService)
    - [IndexRuleRegistryService](#banyandb-database-v1-IndexRuleRegistryService)
    - [MeasureRegistryService](#banyandb-database-v1-MeasureRegistryService)
    - [NodeQueryService](#banyandb-database-v1-NodeQueryService)
    - [PropertyRegistryService](#banyandb-database-v1-PropertyRegistryService)
    - [SnapshotService](#banyandb-database-v1-SnapshotService)
    - [StreamRegistryService](#banyandb-database-v1-StreamRegistryService)
    - [TopNAggregationRegistryService](#banyandb-database-v1-TopNAggregationRegistryService)
    - [TraceRegistryService](#banyandb-database-v1-TraceRegistryService)
  
- [banyandb/fodc/v1/rpc.proto](#banyandb_fodc_v1_rpc-proto)
    - [Metric](#banyandb-fodc-v1-Metric)
    - [Metric.LabelsEntry](#banyandb-fodc-v1-Metric-LabelsEntry)
    - [RegisterAgentRequest](#banyandb-fodc-v1-RegisterAgentRequest)
    - [RegisterAgentRequest.LabelsEntry](#banyandb-fodc-v1-RegisterAgentRequest-LabelsEntry)
    - [RegisterAgentResponse](#banyandb-fodc-v1-RegisterAgentResponse)
    - [StreamMetricsRequest](#banyandb-fodc-v1-StreamMetricsRequest)
    - [StreamMetricsResponse](#banyandb-fodc-v1-StreamMetricsResponse)
  
    - [FODCService](#banyandb-fodc-v1-FODCService)
  
- [banyandb/measure/v1/write.proto](#banyandb_measure_v1_write-proto)
    - [DataPointSpec](#banyandb-measure-v1-DataPointSpec)
    - [DataPointValue](#banyandb-measure-v1-DataPointValue)
    - [InternalWriteRequest](#banyandb-measure-v1-InternalWriteRequest)
    - [TagFamilySpec](#banyandb-measure-v1-TagFamilySpec)
    - [WriteRequest](#banyandb-measure-v1-WriteRequest)
    - [WriteResponse](#banyandb-measure-v1-WriteResponse)
  
- [banyandb/measure/v1/rpc.proto](#banyandb_measure_v1_rpc-proto)
    - [DeleteExpiredSegmentsRequest](#banyandb-measure-v1-DeleteExpiredSegmentsRequest)
    - [DeleteExpiredSegmentsResponse](#banyandb-measure-v1-DeleteExpiredSegmentsResponse)
  
    - [MeasureService](#banyandb-measure-v1-MeasureService)
  
- [banyandb/property/v1/gossip.proto](#banyandb_property_v1_gossip-proto)
    - [PropagationContext](#banyandb-property-v1-PropagationContext)
    - [PropagationRequest](#banyandb-property-v1-PropagationRequest)
    - [PropagationResponse](#banyandb-property-v1-PropagationResponse)
    - [PropagationTraceContext](#banyandb-property-v1-PropagationTraceContext)
  
    - [GossipService](#banyandb-property-v1-GossipService)
  
- [banyandb/property/v1/repair.proto](#banyandb_property_v1_repair-proto)
    - [DifferTreeSummary](#banyandb-property-v1-DifferTreeSummary)
    - [PropertyMissing](#banyandb-property-v1-PropertyMissing)
    - [PropertySync](#banyandb-property-v1-PropertySync)
    - [PropertySyncWithFrom](#banyandb-property-v1-PropertySyncWithFrom)
    - [RepairRequest](#banyandb-property-v1-RepairRequest)
    - [RepairResponse](#banyandb-property-v1-RepairResponse)
    - [RootCompare](#banyandb-property-v1-RootCompare)
    - [TreeLeafNode](#banyandb-property-v1-TreeLeafNode)
    - [TreeRoot](#banyandb-property-v1-TreeRoot)
    - [TreeSlotSHA](#banyandb-property-v1-TreeSlotSHA)
    - [TreeSlots](#banyandb-property-v1-TreeSlots)
    - [WaitNextDifferData](#banyandb-property-v1-WaitNextDifferData)
  
    - [PropertySyncFromType](#banyandb-property-v1-PropertySyncFromType)
  
    - [RepairService](#banyandb-property-v1-RepairService)
  
- [banyandb/stream/v1/write.proto](#banyandb_stream_v1_write-proto)
    - [ElementValue](#banyandb-stream-v1-ElementValue)
    - [InternalWriteRequest](#banyandb-stream-v1-InternalWriteRequest)
    - [TagFamilySpec](#banyandb-stream-v1-TagFamilySpec)
    - [WriteRequest](#banyandb-stream-v1-WriteRequest)
    - [WriteResponse](#banyandb-stream-v1-WriteResponse)
  
- [banyandb/stream/v1/rpc.proto](#banyandb_stream_v1_rpc-proto)
    - [DeleteExpiredSegmentsRequest](#banyandb-stream-v1-DeleteExpiredSegmentsRequest)
    - [DeleteExpiredSegmentsResponse](#banyandb-stream-v1-DeleteExpiredSegmentsResponse)
  
    - [StreamService](#banyandb-stream-v1-StreamService)
  
- [banyandb/trace/v1/write.proto](#banyandb_trace_v1_write-proto)
    - [InternalWriteRequest](#banyandb-trace-v1-InternalWriteRequest)
    - [TagSpec](#banyandb-trace-v1-TagSpec)
    - [WriteRequest](#banyandb-trace-v1-WriteRequest)
    - [WriteResponse](#banyandb-trace-v1-WriteResponse)
  
- [banyandb/trace/v1/rpc.proto](#banyandb_trace_v1_rpc-proto)
    - [DeleteExpiredSegmentsRequest](#banyandb-trace-v1-DeleteExpiredSegmentsRequest)
    - [DeleteExpiredSegmentsResponse](#banyandb-trace-v1-DeleteExpiredSegmentsResponse)
  
    - [TraceService](#banyandb-trace-v1-TraceService)
  
- [Scalar Value Types](#scalar-value-types)



<a name="banyandb_common_v1_trace-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/common/v1/trace.proto



<a name="banyandb-common-v1-Span"></a>

### Span
Span is the basic unit of a trace.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| start_time | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | start_time is the start time of the span. |
| end_time | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | end_time is the end time of the span. |
| error | [bool](#bool) |  | error indicates whether the span is an error span. |
| tags | [Tag](#banyandb-common-v1-Tag) | repeated | tags is a list of tags of the span. |
| message | [string](#string) |  | message is the message generated by the span. |
| children | [Span](#banyandb-common-v1-Span) | repeated | children is a list of child spans of the span. |
| duration | [int64](#int64) |  | duration is the duration of the span. |






<a name="banyandb-common-v1-Tag"></a>

### Tag
Tag is the key-value pair of a span.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | key is the key of the tag. |
| value | [string](#string) |  | value is the value of the tag. |






<a name="banyandb-common-v1-Trace"></a>

### Trace
Trace is the top level message of a trace.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| trace_id | [string](#string) |  | trace_id is the unique identifier of the trace. |
| spans | [Span](#banyandb-common-v1-Span) | repeated | spans is a list of spans in the trace. |
| error | [bool](#bool) |  | error indicates whether the trace is an error trace. |





 

 

 

 



<a name="banyandb_model_v1_common-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/model/v1/common.proto



<a name="banyandb-model-v1-FieldValue"></a>

### FieldValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| null | [google.protobuf.NullValue](#google-protobuf-NullValue) |  |  |
| str | [Str](#banyandb-model-v1-Str) |  |  |
| int | [Int](#banyandb-model-v1-Int) |  |  |
| binary_data | [bytes](#bytes) |  |  |
| float | [Float](#banyandb-model-v1-Float) |  |  |






<a name="banyandb-model-v1-Float"></a>

### Float



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [double](#double) |  |  |






<a name="banyandb-model-v1-Int"></a>

### Int



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [int64](#int64) |  |  |






<a name="banyandb-model-v1-IntArray"></a>

### IntArray



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [int64](#int64) | repeated |  |






<a name="banyandb-model-v1-Str"></a>

### Str



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="banyandb-model-v1-StrArray"></a>

### StrArray



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) | repeated |  |






<a name="banyandb-model-v1-TagFamilyForWrite"></a>

### TagFamilyForWrite



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tags | [TagValue](#banyandb-model-v1-TagValue) | repeated |  |






<a name="banyandb-model-v1-TagValue"></a>

### TagValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| null | [google.protobuf.NullValue](#google-protobuf-NullValue) |  |  |
| str | [Str](#banyandb-model-v1-Str) |  |  |
| str_array | [StrArray](#banyandb-model-v1-StrArray) |  |  |
| int | [Int](#banyandb-model-v1-Int) |  |  |
| int_array | [IntArray](#banyandb-model-v1-IntArray) |  |  |
| binary_data | [bytes](#bytes) |  |  |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |





 


<a name="banyandb-model-v1-AggregationFunction"></a>

### AggregationFunction


| Name | Number | Description |
| ---- | ------ | ----------- |
| AGGREGATION_FUNCTION_UNSPECIFIED | 0 |  |
| AGGREGATION_FUNCTION_MEAN | 1 |  |
| AGGREGATION_FUNCTION_MAX | 2 |  |
| AGGREGATION_FUNCTION_MIN | 3 |  |
| AGGREGATION_FUNCTION_COUNT | 4 |  |
| AGGREGATION_FUNCTION_SUM | 5 |  |


 

 

 



<a name="banyandb_model_v1_query-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/model/v1/query.proto



<a name="banyandb-model-v1-Condition"></a>

### Condition
Condition consists of the query condition with a single binary operator to be imposed
For 1:1 BinaryOp, values in condition must be an array with length = 1,
while for 1:N BinaryOp, values can be an array with length &gt;= 1.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| op | [Condition.BinaryOp](#banyandb-model-v1-Condition-BinaryOp) |  |  |
| value | [TagValue](#banyandb-model-v1-TagValue) |  |  |
| match_option | [Condition.MatchOption](#banyandb-model-v1-Condition-MatchOption) |  |  |






<a name="banyandb-model-v1-Condition-MatchOption"></a>

### Condition.MatchOption



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| analyzer | [string](#string) |  |  |
| operator | [Condition.MatchOption.Operator](#banyandb-model-v1-Condition-MatchOption-Operator) |  |  |






<a name="banyandb-model-v1-Criteria"></a>

### Criteria
tag_families are indexed.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| le | [LogicalExpression](#banyandb-model-v1-LogicalExpression) |  |  |
| condition | [Condition](#banyandb-model-v1-Condition) |  |  |






<a name="banyandb-model-v1-LogicalExpression"></a>

### LogicalExpression
LogicalExpression supports logical operation


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| op | [LogicalExpression.LogicalOp](#banyandb-model-v1-LogicalExpression-LogicalOp) |  | op is a logical operation |
| left | [Criteria](#banyandb-model-v1-Criteria) |  |  |
| right | [Criteria](#banyandb-model-v1-Criteria) |  |  |






<a name="banyandb-model-v1-QueryOrder"></a>

### QueryOrder
QueryOrder means a Sort operation to be done for a given index rule.
The index_rule_name refers to the name of a index rule bound to the subject.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index_rule_name | [string](#string) |  |  |
| sort | [Sort](#banyandb-model-v1-Sort) |  |  |






<a name="banyandb-model-v1-Tag"></a>

### Tag
Pair is the building block of a record which is equivalent to a key-value pair.
In the context of Trace, it could be metadata of a trace such as service_name, service_instance, etc.
Besides, other tags are organized in key-value pair in the underlying storage layer.
One should notice that the values can be a multi-value.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [TagValue](#banyandb-model-v1-TagValue) |  |  |






<a name="banyandb-model-v1-TagFamily"></a>

### TagFamily



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| tags | [Tag](#banyandb-model-v1-Tag) | repeated |  |






<a name="banyandb-model-v1-TagProjection"></a>

### TagProjection
TagProjection is used to select the names of keys to be returned.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tag_families | [TagProjection.TagFamily](#banyandb-model-v1-TagProjection-TagFamily) | repeated |  |






<a name="banyandb-model-v1-TagProjection-TagFamily"></a>

### TagProjection.TagFamily



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| tags | [string](#string) | repeated |  |






<a name="banyandb-model-v1-TimeRange"></a>

### TimeRange
TimeRange is a range query for uint64,
the range here follows left-inclusive and right-exclusive rule, i.e. [begin, end) if both edges exist


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| begin | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| end | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |





 


<a name="banyandb-model-v1-Condition-BinaryOp"></a>

### Condition.BinaryOp
BinaryOp specifies the operation imposed to the given query condition
For EQ, NE, LT, GT, LE and GE, only one operand should be given, i.e. one-to-one relationship.
HAVING and NOT_HAVING allow multi-value to be the operand such as array/vector, i.e. one-to-many relationship.
For example, &#34;keyA&#34; contains &#34;valueA&#34; **and** &#34;valueB&#34;
MATCH performances a full-text search if the tag is analyzed.
The string value applies to the same analyzer as the tag, but string array value does not.
Each item in a string array is seen as a token instead of a query expression.

| Name | Number | Description |
| ---- | ------ | ----------- |
| BINARY_OP_UNSPECIFIED | 0 |  |
| BINARY_OP_EQ | 1 |  |
| BINARY_OP_NE | 2 |  |
| BINARY_OP_LT | 3 |  |
| BINARY_OP_GT | 4 |  |
| BINARY_OP_LE | 5 |  |
| BINARY_OP_GE | 6 |  |
| BINARY_OP_HAVING | 7 |  |
| BINARY_OP_NOT_HAVING | 8 |  |
| BINARY_OP_IN | 9 |  |
| BINARY_OP_NOT_IN | 10 |  |
| BINARY_OP_MATCH | 11 |  |



<a name="banyandb-model-v1-Condition-MatchOption-Operator"></a>

### Condition.MatchOption.Operator


| Name | Number | Description |
| ---- | ------ | ----------- |
| OPERATOR_UNSPECIFIED | 0 |  |
| OPERATOR_AND | 1 |  |
| OPERATOR_OR | 2 |  |



<a name="banyandb-model-v1-LogicalExpression-LogicalOp"></a>

### LogicalExpression.LogicalOp


| Name | Number | Description |
| ---- | ------ | ----------- |
| LOGICAL_OP_UNSPECIFIED | 0 |  |
| LOGICAL_OP_AND | 1 |  |
| LOGICAL_OP_OR | 2 |  |



<a name="banyandb-model-v1-Sort"></a>

### Sort


| Name | Number | Description |
| ---- | ------ | ----------- |
| SORT_UNSPECIFIED | 0 |  |
| SORT_DESC | 1 |  |
| SORT_ASC | 2 |  |


 

 

 



<a name="banyandb_measure_v1_query-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/measure/v1/query.proto



<a name="banyandb-measure-v1-DataPoint"></a>

### DataPoint
DataPoint is stored in Measures


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | timestamp is in the timeunit of milliseconds. |
| tag_families | [banyandb.model.v1.TagFamily](#banyandb-model-v1-TagFamily) | repeated | tag_families contains tags selected in the projection |
| fields | [DataPoint.Field](#banyandb-measure-v1-DataPoint-Field) | repeated | fields contains fields selected in the projection |
| sid | [uint64](#uint64) |  | sid is the series id of the data point |
| version | [int64](#int64) |  | version is the version of the data point in a series sid, timestamp and version are used to identify a data point |






<a name="banyandb-measure-v1-DataPoint-Field"></a>

### DataPoint.Field



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| value | [banyandb.model.v1.FieldValue](#banyandb-model-v1-FieldValue) |  |  |






<a name="banyandb-measure-v1-InternalDataPoint"></a>

### InternalDataPoint
InternalDataPoint wraps DataPoint with shard information for internal use.
Used in distributed query to distinguish data from different shards.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data_point | [DataPoint](#banyandb-measure-v1-DataPoint) |  | The actual data point |
| shard_id | [uint32](#uint32) |  | The shard id where this data point comes from |






<a name="banyandb-measure-v1-InternalQueryRequest"></a>

### InternalQueryRequest
InternalQueryRequest is the internal request for distributed query.
Wraps QueryRequest for extensibility.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| request | [QueryRequest](#banyandb-measure-v1-QueryRequest) |  | The actual query request |






<a name="banyandb-measure-v1-InternalQueryResponse"></a>

### InternalQueryResponse
InternalQueryResponse is the internal response for distributed query.
Contains shard information for proper deduplication.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data_points | [InternalDataPoint](#banyandb-measure-v1-InternalDataPoint) | repeated | data_points with shard information |
| trace | [banyandb.common.v1.Trace](#banyandb-common-v1-Trace) |  | trace contains the trace information of the query when trace is enabled |






<a name="banyandb-measure-v1-QueryRequest"></a>

### QueryRequest
QueryRequest is the request contract for query.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| groups | [string](#string) | repeated | groups indicate where the data points are stored. |
| name | [string](#string) |  | name is the identity of a measure. |
| time_range | [banyandb.model.v1.TimeRange](#banyandb-model-v1-TimeRange) |  | time_range is a range query with begin/end time of entities in the timeunit of milliseconds. |
| criteria | [banyandb.model.v1.Criteria](#banyandb-model-v1-Criteria) |  | tag_families are indexed. |
| tag_projection | [banyandb.model.v1.TagProjection](#banyandb-model-v1-TagProjection) |  | tag_projection can be used to select tags of the data points in the response |
| field_projection | [QueryRequest.FieldProjection](#banyandb-measure-v1-QueryRequest-FieldProjection) |  | field_projection can be used to select fields of the data points in the response |
| group_by | [QueryRequest.GroupBy](#banyandb-measure-v1-QueryRequest-GroupBy) |  | group_by groups data points based on their field value for a specific tag and use field_name as the projection name |
| agg | [QueryRequest.Aggregation](#banyandb-measure-v1-QueryRequest-Aggregation) |  | agg aggregates data points based on a field |
| top | [QueryRequest.Top](#banyandb-measure-v1-QueryRequest-Top) |  | top limits the result based on a particular field. If order_by is specified, top sorts the dataset based on order_by&#39;s output |
| offset | [uint32](#uint32) |  | offset is used to support pagination, together with the following limit. If top is specified, offset processes the dataset based on top&#39;s output |
| limit | [uint32](#uint32) |  | limit is used to impose a boundary on the number of records being returned. If top is specified, limit processes the dataset based on top&#39;s output |
| order_by | [banyandb.model.v1.QueryOrder](#banyandb-model-v1-QueryOrder) |  | order_by is given to specify the sort for a tag. |
| trace | [bool](#bool) |  | trace is used to enable trace for the query |
| stages | [string](#string) | repeated | stages is used to specify the stage of the data points in the lifecycle |
| rewrite_agg_top_n_result | [bool](#bool) |  | rewriteAggTopNResult will rewrite agg result to raw data |






<a name="banyandb-measure-v1-QueryRequest-Aggregation"></a>

### QueryRequest.Aggregation



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| function | [banyandb.model.v1.AggregationFunction](#banyandb-model-v1-AggregationFunction) |  |  |
| field_name | [string](#string) |  | field_name must be one of files indicated by the field_projection |






<a name="banyandb-measure-v1-QueryRequest-FieldProjection"></a>

### QueryRequest.FieldProjection



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| names | [string](#string) | repeated |  |






<a name="banyandb-measure-v1-QueryRequest-GroupBy"></a>

### QueryRequest.GroupBy



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tag_projection | [banyandb.model.v1.TagProjection](#banyandb-model-v1-TagProjection) |  | tag_projection must be a subset of the tag_projection of QueryRequest |
| field_name | [string](#string) |  | field_name must be one of fields indicated by field_projection |






<a name="banyandb-measure-v1-QueryRequest-Top"></a>

### QueryRequest.Top



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| number | [int32](#int32) |  | number set the how many items should be returned |
| field_name | [string](#string) |  | field_name must be one of files indicated by the field_projection |
| field_value_sort | [banyandb.model.v1.Sort](#banyandb-model-v1-Sort) |  | field_value_sort indicates how to sort fields ASC: bottomN DESC: topN UNSPECIFIED: topN |






<a name="banyandb-measure-v1-QueryResponse"></a>

### QueryResponse
QueryResponse is the response for a query to the Query module.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data_points | [DataPoint](#banyandb-measure-v1-DataPoint) | repeated | data_points are the actual data returned |
| trace | [banyandb.common.v1.Trace](#banyandb-common-v1-Trace) |  | trace contains the trace information of the query when trace is enabled |





 

 

 

 



<a name="banyandb_measure_v1_topn-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/measure/v1/topn.proto



<a name="banyandb-measure-v1-TopNList"></a>

### TopNList
TopNList contains a series of topN items


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | timestamp is in the timeunit of milliseconds. |
| items | [TopNList.Item](#banyandb-measure-v1-TopNList-Item) | repeated | items contains top-n items in a list |






<a name="banyandb-measure-v1-TopNList-Item"></a>

### TopNList.Item



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entity | [banyandb.model.v1.Tag](#banyandb-model-v1-Tag) | repeated |  |
| value | [banyandb.model.v1.FieldValue](#banyandb-model-v1-FieldValue) |  |  |






<a name="banyandb-measure-v1-TopNRequest"></a>

### TopNRequest
TopNRequest is the request contract for query.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| groups | [string](#string) | repeated | groups indicate where the data points are stored. |
| name | [string](#string) |  | name is the identity of a measure. |
| time_range | [banyandb.model.v1.TimeRange](#banyandb-model-v1-TimeRange) |  | time_range is a range query with begin/end time of entities in the timeunit of milliseconds. |
| top_n | [int32](#int32) |  | top_n set the how many items should be returned in each list. |
| agg | [banyandb.model.v1.AggregationFunction](#banyandb-model-v1-AggregationFunction) |  | agg aggregates lists grouped by field names in the time_range |
| conditions | [banyandb.model.v1.Condition](#banyandb-model-v1-Condition) | repeated | criteria select counters. Only equals are acceptable. |
| field_value_sort | [banyandb.model.v1.Sort](#banyandb-model-v1-Sort) |  | field_value_sort indicates how to sort fields |
| trace | [bool](#bool) |  | trace is used to enable trace for the query |
| stages | [string](#string) | repeated | stages is used to specify the stage of the data points in the lifecycle |






<a name="banyandb-measure-v1-TopNResponse"></a>

### TopNResponse
TopNResponse is the response for a query to the Query module.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| lists | [TopNList](#banyandb-measure-v1-TopNList) | repeated | lists contain a series topN lists ranked by timestamp if agg_func in query request is specified, lists&#39; size should be one. |
| trace | [banyandb.common.v1.Trace](#banyandb-common-v1-Trace) |  | trace contains the trace information of the query when trace is enabled |





 

 

 

 



<a name="banyandb_common_v1_common-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/common/v1/common.proto



<a name="banyandb-common-v1-Group"></a>

### Group
Group is an internal object for Group management


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [Metadata](#banyandb-common-v1-Metadata) |  | metadata define the group&#39;s identity |
| catalog | [Catalog](#banyandb-common-v1-Catalog) |  | catalog denotes which type of data the group contains |
| resource_opts | [ResourceOpts](#banyandb-common-v1-ResourceOpts) |  | resourceOpts indicates the structure of the underlying kv storage |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at indicates when resources of the group are updated |






<a name="banyandb-common-v1-IntervalRule"></a>

### IntervalRule
IntervalRule is a structured duration


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| unit | [IntervalRule.Unit](#banyandb-common-v1-IntervalRule-Unit) |  | unit can only be UNIT_HOUR or UNIT_DAY |
| num | [uint32](#uint32) |  |  |






<a name="banyandb-common-v1-LifecycleStage"></a>

### LifecycleStage



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | The stage name (e.g., &#34;warm&#34;, &#34;cold&#34;). This should be a non-empty string. |
| shard_num | [uint32](#uint32) |  | Number of shards allocated for this stage. Must be greater than zero. |
| segment_interval | [IntervalRule](#banyandb-common-v1-IntervalRule) |  | Defines the interval for data segmentation in this stage. This is a required field and uses the IntervalRule structure. |
| ttl | [IntervalRule](#banyandb-common-v1-IntervalRule) |  | Specifies the time-to-live for data in this stage before moving to the next. This is also a required field using the IntervalRule structure. |
| node_selector | [string](#string) |  | Node selector specifying target nodes for this stage. Optional; if provided, it must be a non-empty string. |
| close | [bool](#bool) |  | Indicates whether segments that are no longer live should be closed. |
| replicas | [uint32](#uint32) |  | replicas is the number of replicas for this stage. This is an optional field and defaults to 0. A value of 0 means no replicas, while a value of 1 means one primary shard and one replica. Higher values indicate more replicas. |






<a name="banyandb-common-v1-Metadata"></a>

### Metadata
Metadata is for multi-tenant, multi-model use


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  | group contains a set of options, like retention policy, max |
| name | [string](#string) |  | name of the entity |
| id | [uint32](#uint32) |  | id is the unique identifier of the entity if id is not set, the system will generate a unique id |
| create_revision | [int64](#int64) |  | readonly. create_revision is the revision of last creation on this key. |
| mod_revision | [int64](#int64) |  | readonly. mod_revision is the revision of last modification on this key. |






<a name="banyandb-common-v1-ResourceOpts"></a>

### ResourceOpts



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| shard_num | [uint32](#uint32) |  | shard_num is the number of shards |
| segment_interval | [IntervalRule](#banyandb-common-v1-IntervalRule) |  | segment_interval indicates the length of a segment |
| ttl | [IntervalRule](#banyandb-common-v1-IntervalRule) |  | ttl indicates time to live, how long the data will be cached |
| stages | [LifecycleStage](#banyandb-common-v1-LifecycleStage) | repeated | stages defines the ordered lifecycle stages. Data progresses through these stages sequentially. |
| default_stages | [string](#string) | repeated | default_stages is the name of the default stage |
| replicas | [uint32](#uint32) |  | replicas is the number of replicas. This is used to ensure high availability and fault tolerance. This is an optional field and defaults to 0. A value of 0 means no replicas, while a value of 1 means one primary shard and one replica. Higher values indicate more replicas. |





 


<a name="banyandb-common-v1-Catalog"></a>

### Catalog


| Name | Number | Description |
| ---- | ------ | ----------- |
| CATALOG_UNSPECIFIED | 0 |  |
| CATALOG_STREAM | 1 |  |
| CATALOG_MEASURE | 2 |  |
| CATALOG_PROPERTY | 3 |  |
| CATALOG_TRACE | 4 |  |



<a name="banyandb-common-v1-IntervalRule-Unit"></a>

### IntervalRule.Unit


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNIT_UNSPECIFIED | 0 |  |
| UNIT_HOUR | 1 |  |
| UNIT_DAY | 2 |  |


 

 

 



<a name="banyandb_property_v1_property-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/property/v1/property.proto



<a name="banyandb-property-v1-Property"></a>

### Property
Property stores the user defined data


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | metadata is the identity of a property |
| id | [string](#string) |  | id is the identity of a property |
| tags | [banyandb.model.v1.Tag](#banyandb-model-v1-Tag) | repeated | tag stores the content of a property |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at indicates when the property is updated |





 

 

 

 



<a name="banyandb_property_v1_rpc-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/property/v1/rpc.proto



<a name="banyandb-property-v1-ApplyRequest"></a>

### ApplyRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| property | [Property](#banyandb-property-v1-Property) |  |  |
| strategy | [ApplyRequest.Strategy](#banyandb-property-v1-ApplyRequest-Strategy) |  | strategy indicates how to update a property. It defaults to STRATEGY_MERGE |






<a name="banyandb-property-v1-ApplyResponse"></a>

### ApplyResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| created | [bool](#bool) |  | created indicates whether the property existed. True: the property is absent. False: the property existed. |
| tags_num | [uint32](#uint32) |  |  |






<a name="banyandb-property-v1-DeleteRequest"></a>

### DeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  | groups indicate where the data points are stored. |
| name | [string](#string) |  | name is the identity of a property. |
| id | [string](#string) |  | id is the identity of item in the property. |






<a name="banyandb-property-v1-DeleteResponse"></a>

### DeleteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |






<a name="banyandb-property-v1-InternalDeleteRequest"></a>

### InternalDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ids | [bytes](#bytes) | repeated |  |






<a name="banyandb-property-v1-InternalQueryResponse"></a>

### InternalQueryResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sources | [bytes](#bytes) | repeated |  |
| trace | [banyandb.common.v1.Trace](#banyandb-common-v1-Trace) |  |  |
| deletes | [int64](#int64) | repeated | deletes indicates the property is deleted timestamps, it&#39;s mapping to the sources in the same order if the value is 0, it means the property is not deleted |
| sorted_values | [bytes](#bytes) | repeated | sorted_values contains pre-extracted sort values from shard searches, mapping to sources in the same order for optimized sorting at liaison layer |






<a name="banyandb-property-v1-InternalRepairRequest"></a>

### InternalRepairRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| shard_id | [uint64](#uint64) |  |  |
| id | [bytes](#bytes) |  |  |
| property | [Property](#banyandb-property-v1-Property) |  |  |
| delete_time | [int64](#int64) |  |  |






<a name="banyandb-property-v1-InternalRepairResponse"></a>

### InternalRepairResponse







<a name="banyandb-property-v1-InternalUpdateRequest"></a>

### InternalUpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [bytes](#bytes) |  |  |
| shard_id | [uint64](#uint64) |  |  |
| property | [Property](#banyandb-property-v1-Property) |  |  |






<a name="banyandb-property-v1-QueryOrder"></a>

### QueryOrder



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tag_name | [string](#string) |  | tag_name is the name of the tag to be ordered. |
| sort | [banyandb.model.v1.Sort](#banyandb-model-v1-Sort) |  | order_by is given to specify the sort for a tag. |






<a name="banyandb-property-v1-QueryRequest"></a>

### QueryRequest
QueryRequest is the request contract for query.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| groups | [string](#string) | repeated | groups indicate where the data points are stored. |
| name | [string](#string) |  | name is created when it receives the first property |
| ids | [string](#string) | repeated | ids is the identities of properties |
| criteria | [banyandb.model.v1.Criteria](#banyandb-model-v1-Criteria) |  | criteria is used to filter properties based on tags |
| tag_projection | [string](#string) | repeated | tag_projection can be used to select tags of the data points in the response |
| limit | [uint32](#uint32) |  |  |
| trace | [bool](#bool) |  | trace is used to enable trace for the query |
| order_by | [QueryOrder](#banyandb-property-v1-QueryOrder) |  | order_by is given to specify the sort for a tag. |






<a name="banyandb-property-v1-QueryResponse"></a>

### QueryResponse
QueryResponse is the response for a query to the Query module.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| properties | [Property](#banyandb-property-v1-Property) | repeated | properties are the actual data returned |
| trace | [banyandb.common.v1.Trace](#banyandb-common-v1-Trace) |  | trace contains the trace information of the query when trace is enabled |





 


<a name="banyandb-property-v1-ApplyRequest-Strategy"></a>

### ApplyRequest.Strategy


| Name | Number | Description |
| ---- | ------ | ----------- |
| STRATEGY_UNSPECIFIED | 0 |  |
| STRATEGY_MERGE | 1 |  |
| STRATEGY_REPLACE | 2 |  |


 

 


<a name="banyandb-property-v1-PropertyService"></a>

### PropertyService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Apply | [ApplyRequest](#banyandb-property-v1-ApplyRequest) | [ApplyResponse](#banyandb-property-v1-ApplyResponse) | Apply creates a property if it&#39;s absent, or update a existed one based on a strategy. |
| Delete | [DeleteRequest](#banyandb-property-v1-DeleteRequest) | [DeleteResponse](#banyandb-property-v1-DeleteResponse) |  |
| Query | [QueryRequest](#banyandb-property-v1-QueryRequest) | [QueryResponse](#banyandb-property-v1-QueryResponse) |  |

 



<a name="banyandb_stream_v1_query-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/stream/v1/query.proto



<a name="banyandb-stream-v1-Element"></a>

### Element
Element represents
(stream context) a Span defined in Google Dapper paper or equivalently a Segment in Skywalking.
(Log context) a log


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element_id | [string](#string) |  | element_id could be span_id of a Span or segment_id of a Segment in the context of stream |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | timestamp represents a millisecond 1) either the start time of a Span/Segment, 2) or the timestamp of a log |
| tag_families | [banyandb.model.v1.TagFamily](#banyandb-model-v1-TagFamily) | repeated | fields contains all indexed Field. Some typical names, - stream_id - duration - service_name - service_instance_id - end_time_milliseconds |






<a name="banyandb-stream-v1-QueryRequest"></a>

### QueryRequest
QueryRequest is the request contract for query.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| groups | [string](#string) | repeated | groups indicate where the elements are stored. |
| name | [string](#string) |  | name is the identity of a stream. |
| time_range | [banyandb.model.v1.TimeRange](#banyandb-model-v1-TimeRange) |  | time_range is a range query with begin/end time of entities in the timeunit of milliseconds. In the context of stream, it represents the range of the `startTime` for spans/segments, while in the context of Log, it means the range of the timestamp(s) for logs. it is always recommended to specify time range for performance reason |
| offset | [uint32](#uint32) |  | offset is used to support pagination, together with the following limit |
| limit | [uint32](#uint32) |  | limit is used to impose a boundary on the number of records being returned |
| order_by | [banyandb.model.v1.QueryOrder](#banyandb-model-v1-QueryOrder) |  | order_by is given to specify the sort for a field. So far, only fields in the type of Integer are supported |
| criteria | [banyandb.model.v1.Criteria](#banyandb-model-v1-Criteria) |  | tag_families are indexed. |
| projection | [banyandb.model.v1.TagProjection](#banyandb-model-v1-TagProjection) |  | projection can be used to select the key names of the element in the response |
| trace | [bool](#bool) |  | trace is used to enable trace for the query |
| stages | [string](#string) | repeated | stage is used to specify the stage of the query in the lifecycle |






<a name="banyandb-stream-v1-QueryResponse"></a>

### QueryResponse
QueryResponse is the response for a query to the Query module.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| elements | [Element](#banyandb-stream-v1-Element) | repeated | elements are the actual data returned |
| trace | [banyandb.common.v1.Trace](#banyandb-common-v1-Trace) |  | trace contains the trace information of the query when trace is enabled |





 

 

 

 



<a name="banyandb_trace_v1_query-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/trace/v1/query.proto



<a name="banyandb-trace-v1-InternalQueryResponse"></a>

### InternalQueryResponse
InternalQueryResponse is the response of an internal query.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| internal_traces | [InternalTrace](#banyandb-trace-v1-InternalTrace) | repeated | internal_traces is a list of internal traces that match the query. |
| trace_query_result | [banyandb.common.v1.Trace](#banyandb-common-v1-Trace) |  | trace_query_result contains the trace of the query execution if tracing is enabled. |






<a name="banyandb-trace-v1-InternalTrace"></a>

### InternalTrace
InternalTrace is the trace that is used for internal use.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| spans | [Span](#banyandb-trace-v1-Span) | repeated | spans are the spans that belong to this trace. |
| trace_id | [string](#string) |  | trace_id is the unique identifier of the trace. |
| key | [int64](#int64) |  | key is used for sorting. |






<a name="banyandb-trace-v1-QueryRequest"></a>

### QueryRequest
QueryRequest is the request contract for query.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| groups | [string](#string) | repeated | groups indicates the physical data location. |
| name | [string](#string) |  | name is the identity of a trace. |
| time_range | [banyandb.model.v1.TimeRange](#banyandb-model-v1-TimeRange) |  | time_range is a range query with begin/end time of entities in the timeunit of milliseconds. In the context of trace, it represents the range of the `startTime` for spans/segments, it is always recommended to specify time range for performance reason |
| offset | [uint32](#uint32) |  | offset is used to support pagination, together with the following limit |
| limit | [uint32](#uint32) |  | limit is used to impose a boundary on the number of spans being returned |
| order_by | [banyandb.model.v1.QueryOrder](#banyandb-model-v1-QueryOrder) |  | order_by is given to specify the sort for a tag. So far, only tags in the type of Integer are supported |
| criteria | [banyandb.model.v1.Criteria](#banyandb-model-v1-Criteria) |  | criteria is the filter criteria. |
| tag_projection | [string](#string) | repeated | projection can be used to select the names of the tags in the response |
| trace | [bool](#bool) |  | trace is used to enable trace for the query |
| stages | [string](#string) | repeated | stage is used to specify the stage of the query in the lifecycle |






<a name="banyandb-trace-v1-QueryResponse"></a>

### QueryResponse
QueryResponse is the response of a query.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| traces | [Trace](#banyandb-trace-v1-Trace) | repeated | traces is a list of traces that match the query, with spans grouped by trace ID. |
| trace_query_result | [banyandb.common.v1.Trace](#banyandb-common-v1-Trace) |  | trace_query_result contains the trace of the query execution if tracing is enabled. |






<a name="banyandb-trace-v1-Span"></a>

### Span
Span is a single operation within a trace.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tags | [banyandb.model.v1.Tag](#banyandb-model-v1-Tag) | repeated | tags are the indexed tags of the span. |
| span | [bytes](#bytes) |  | span is the raw span data. |
| span_id | [string](#string) |  | span_id is the unique identifier of the span. |






<a name="banyandb-trace-v1-Trace"></a>

### Trace
Trace contains all spans that belong to a single trace ID.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| spans | [Span](#banyandb-trace-v1-Span) | repeated | spans is the list of spans that belong to this trace. |
| trace_id | [string](#string) |  | trace_id is the unique identifier of the trace. |





 

 

 

 



<a name="banyandb_bydbql_v1_query-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/bydbql/v1/query.proto



<a name="banyandb-bydbql-v1-QueryRequest"></a>

### QueryRequest
QueryRequest is the main request message for BydbQL queries


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| query | [string](#string) |  | query is the BydbQL query string |






<a name="banyandb-bydbql-v1-QueryResponse"></a>

### QueryResponse
QueryResponse contains the result of a BydbQL query


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stream_result | [banyandb.stream.v1.QueryResponse](#banyandb-stream-v1-QueryResponse) |  | stream_result is returned for stream queries |
| measure_result | [banyandb.measure.v1.QueryResponse](#banyandb-measure-v1-QueryResponse) |  | measure_result is returned for measure queries |
| property_result | [banyandb.property.v1.QueryResponse](#banyandb-property-v1-QueryResponse) |  | property_result is returned for property queries |
| trace_result | [banyandb.trace.v1.QueryResponse](#banyandb-trace-v1-QueryResponse) |  | trace_result is returned for trace queries |
| topn_result | [banyandb.measure.v1.TopNResponse](#banyandb-measure-v1-TopNResponse) |  | topn_result is returned for TopN queries |





 

 

 

 



<a name="banyandb_bydbql_v1_rpc-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/bydbql/v1/rpc.proto


 

 

 


<a name="banyandb-bydbql-v1-BydbQLService"></a>

### BydbQLService
BydbQLService provides query interface for BanyanDB Query Language (BydbQL)

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Query | [QueryRequest](#banyandb-bydbql-v1-QueryRequest) | [QueryResponse](#banyandb-bydbql-v1-QueryResponse) | Query executes a generic BydbQL query with explicit FROM clause This endpoint requires the query to specify the resource type and name in the FROM clause (e.g., &#34;FROM STREAM sw&#34;, &#34;FROM MEASURE metrics&#34;) |

 



<a name="banyandb_model_v1_write-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/model/v1/write.proto


 


<a name="banyandb-model-v1-Status"></a>

### Status
Status is the response status for write

| Name | Number | Description |
| ---- | ------ | ----------- |
| STATUS_UNSPECIFIED | 0 |  |
| STATUS_SUCCEED | 1 |  |
| STATUS_INVALID_TIMESTAMP | 2 |  |
| STATUS_NOT_FOUND | 3 |  |
| STATUS_EXPIRED_SCHEMA | 4 |  |
| STATUS_INTERNAL_ERROR | 5 |  |
| STATUS_DISK_FULL | 6 |  |
| STATUS_VERSION_UNSUPPORTED | 7 | Client version not supported |
| STATUS_VERSION_DEPRECATED | 8 | Client version deprecated but still supported |
| STATUS_METADATA_REQUIRED | 9 | Metadata is required for the first request |


 

 

 



<a name="banyandb_cluster_v1_rpc-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/cluster/v1/rpc.proto



<a name="banyandb-cluster-v1-FileInfo"></a>

### FileInfo
Information about an individual file within a part.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | File identifier (e.g., &#34;primary&#34;, &#34;timestamps&#34;, &#34;tagFamilies:seriesId&#34;). |
| offset | [uint32](#uint32) |  | Byte offset within the part where this file starts. |
| size | [uint32](#uint32) |  | Size of this file in bytes. |






<a name="banyandb-cluster-v1-HealthCheckRequest"></a>

### HealthCheckRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| service_name | [string](#string) |  |  |






<a name="banyandb-cluster-v1-HealthCheckResponse"></a>

### HealthCheckResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| service_name | [string](#string) |  |  |
| status | [banyandb.model.v1.Status](#banyandb-model-v1-Status) |  |  |
| error | [string](#string) |  |  |






<a name="banyandb-cluster-v1-PartInfo"></a>

### PartInfo
Information about a part contained within a chunk.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [uint64](#uint64) |  | Unique identifier for this part. |
| files | [FileInfo](#banyandb-cluster-v1-FileInfo) | repeated | Information about individual files within this part. |
| compressed_size_bytes | [uint64](#uint64) |  | Compressed size in bytes from partMetadata. |
| uncompressed_size_bytes | [uint64](#uint64) |  | Uncompressed size in bytes from partMetadata. |
| total_count | [uint64](#uint64) |  | Total count from partMetadata. |
| blocks_count | [uint64](#uint64) |  | Blocks count from partMetadata. |
| min_timestamp | [int64](#int64) |  | Minimum timestamp from partMetadata. |
| max_timestamp | [int64](#int64) |  | Maximum timestamp from partMetadata. |
| min_key | [int64](#int64) |  | Minimum user-provided key for sidx. |
| max_key | [int64](#int64) |  | Maximum user-provided key for sidx. |
| part_type | [string](#string) |  | Part type. |






<a name="banyandb-cluster-v1-PartResult"></a>

### PartResult
PartResult contains the result for individual parts.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | [bool](#bool) |  | Whether this part was processed successfully. |
| error | [string](#string) |  | Error message if processing failed. |
| bytes_processed | [uint32](#uint32) |  | Number of bytes processed for this part. |






<a name="banyandb-cluster-v1-SendRequest"></a>

### SendRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| topic | [string](#string) |  |  |
| message_id | [uint64](#uint64) |  |  |
| body | [bytes](#bytes) |  |  |
| batch_mod | [bool](#bool) |  |  |
| version_info | [VersionInfo](#banyandb-cluster-v1-VersionInfo) |  | version_info contains version information |






<a name="banyandb-cluster-v1-SendResponse"></a>

### SendResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message_id | [uint64](#uint64) |  |  |
| error | [string](#string) |  |  |
| body | [bytes](#bytes) |  |  |
| status | [banyandb.model.v1.Status](#banyandb-model-v1-Status) |  |  |
| version_compatibility | [VersionCompatibility](#banyandb-cluster-v1-VersionCompatibility) |  | version_compatibility contains version compatibility information when status indicates version issues |






<a name="banyandb-cluster-v1-SyncCompletion"></a>

### SyncCompletion
SyncCompletion contains completion information for the sync operation.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| total_bytes_sent | [uint64](#uint64) |  | Total bytes sent for validation. |
| total_parts_sent | [uint32](#uint32) |  | Total number of parts sent. |
| total_chunks | [uint32](#uint32) |  | Total number of chunks in this sync. |






<a name="banyandb-cluster-v1-SyncMetadata"></a>

### SyncMetadata
SyncMetadata contains metadata for the sync operation.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  | Group name (stream/measure). |
| shard_id | [uint32](#uint32) |  | Shard identifier. |
| topic | [string](#string) |  | Sync topic (stream-part-sync or measure-part-sync). |
| timestamp | [int64](#int64) |  | Timestamp when sync started. |
| total_parts | [uint32](#uint32) |  | Total number of parts being synced. |






<a name="banyandb-cluster-v1-SyncPartRequest"></a>

### SyncPartRequest
Chunked Sync Service Messages.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session_id | [string](#string) |  | Unique session identifier for this sync operation. |
| chunk_index | [uint32](#uint32) |  | Current chunk index (0-based). |
| chunk_data | [bytes](#bytes) |  | Actual chunk data. |
| chunk_checksum | [string](#string) |  | CRC32 checksum for this chunk. |
| parts_info | [PartInfo](#banyandb-cluster-v1-PartInfo) | repeated | Information about parts contained in this chunk. |
| metadata | [SyncMetadata](#banyandb-cluster-v1-SyncMetadata) |  | Sent with first chunk (chunk_index = 0). |
| completion | [SyncCompletion](#banyandb-cluster-v1-SyncCompletion) |  | Sent with last chunk to finalize. |
| version_info | [VersionInfo](#banyandb-cluster-v1-VersionInfo) |  | version_info contains version information |






<a name="banyandb-cluster-v1-SyncPartResponse"></a>

### SyncPartResponse
SyncPartResponse contains the response for a sync part request.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session_id | [string](#string) |  |  |
| chunk_index | [uint32](#uint32) |  |  |
| status | [SyncStatus](#banyandb-cluster-v1-SyncStatus) |  |  |
| error | [string](#string) |  |  |
| sync_result | [SyncResult](#banyandb-cluster-v1-SyncResult) |  | Final result when sync completes. |
| version_compatibility | [VersionCompatibility](#banyandb-cluster-v1-VersionCompatibility) |  | version_compatibility contains version compatibility information when status indicates version issues |






<a name="banyandb-cluster-v1-SyncResult"></a>

### SyncResult
SyncResult contains the result of a sync operation.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | [bool](#bool) |  | Whether entire sync was successful. |
| total_bytes_received | [uint64](#uint64) |  | Total bytes received. |
| duration_ms | [int64](#int64) |  | Time taken for sync in milliseconds. |
| chunks_received | [uint32](#uint32) |  | Number of chunks successfully received. |
| parts_received | [uint32](#uint32) |  | Number of parts successfully received. |
| parts_results | [PartResult](#banyandb-cluster-v1-PartResult) | repeated | Results for each part. |






<a name="banyandb-cluster-v1-VersionCompatibility"></a>

### VersionCompatibility



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| supported | [bool](#bool) |  | supported indicates whether the client version is supported |
| server_api_version | [string](#string) |  | server_api_version is the API version of the server |
| supported_api_versions | [string](#string) | repeated | supported_api_versions lists API versions supported by the server |
| server_file_format_version | [string](#string) |  | server_file_format_version is the file format version of the server |
| supported_file_format_versions | [string](#string) | repeated | supported_file_format_versions lists file format versions supported by the server |
| reason | [string](#string) |  | reason provides human-readable explanation of version incompatibility |






<a name="banyandb-cluster-v1-VersionInfo"></a>

### VersionInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| file_format_version | [string](#string) |  | file_format_version indicates the file format version used |
| compatible_file_format_version | [string](#string) | repeated | compatible_file_format_version lists backward compatible versions |
| api_version | [string](#string) |  | api_version indicates the API semantic version |





 


<a name="banyandb-cluster-v1-SyncStatus"></a>

### SyncStatus
SyncStatus represents the status of a sync operation.

| Name | Number | Description |
| ---- | ------ | ----------- |
| SYNC_STATUS_UNSPECIFIED | 0 | Unspecified status. |
| SYNC_STATUS_CHUNK_RECEIVED | 1 | Chunk received and validated successfully. |
| SYNC_STATUS_CHUNK_CHECKSUM_MISMATCH | 2 | Chunk checksum validation failed. |
| SYNC_STATUS_CHUNK_OUT_OF_ORDER | 3 | Chunk received out of expected order. |
| SYNC_STATUS_SESSION_NOT_FOUND | 4 | Session ID not recognized. |
| SYNC_STATUS_SYNC_COMPLETE | 5 | Entire sync operation completed successfully. |
| SYNC_STATUS_VERSION_UNSUPPORTED | 6 | Version not supported for sync operations. |
| SYNC_STATUS_FORMAT_VERSION_MISMATCH | 7 | File format version incompatible. |


 

 


<a name="banyandb-cluster-v1-ChunkedSyncService"></a>

### ChunkedSyncService
ChunkedSyncService provides streaming sync capabilities for chunked data transfer.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| SyncPart | [SyncPartRequest](#banyandb-cluster-v1-SyncPartRequest) stream | [SyncPartResponse](#banyandb-cluster-v1-SyncPartResponse) stream | SyncPart synchronizes part data using chunked transfer. |


<a name="banyandb-cluster-v1-Service"></a>

### Service


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Send | [SendRequest](#banyandb-cluster-v1-SendRequest) stream | [SendResponse](#banyandb-cluster-v1-SendResponse) stream |  |
| HealthCheck | [HealthCheckRequest](#banyandb-cluster-v1-HealthCheckRequest) | [HealthCheckResponse](#banyandb-cluster-v1-HealthCheckResponse) |  |

 



<a name="banyandb_common_v1_rpc-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/common/v1/rpc.proto



<a name="banyandb-common-v1-APIVersion"></a>

### APIVersion
APIVersion is the version of the API


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [string](#string) |  | version is the version of the API |
| revision | [string](#string) |  | revision is the commit hash of the API |






<a name="banyandb-common-v1-GetAPIVersionRequest"></a>

### GetAPIVersionRequest
GetAPIVersionRequest is the request for GetAPIVersion

empty






<a name="banyandb-common-v1-GetAPIVersionResponse"></a>

### GetAPIVersionResponse
GetAPIVersionResponse is the response for GetAPIVersion


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [APIVersion](#banyandb-common-v1-APIVersion) |  | version is the version of the API |





 

 

 


<a name="banyandb-common-v1-Service"></a>

### Service
Service is the service for the API

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetAPIVersion | [GetAPIVersionRequest](#banyandb-common-v1-GetAPIVersionRequest) | [GetAPIVersionResponse](#banyandb-common-v1-GetAPIVersionResponse) | GetAPIVersion returns the version of the API |

 



<a name="banyandb_database_v1_database-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/database/v1/database.proto



<a name="banyandb-database-v1-Node"></a>

### Node



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |
| roles | [Role](#banyandb-database-v1-Role) | repeated |  |
| grpc_address | [string](#string) |  |  |
| http_address | [string](#string) |  |  |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| labels | [Node.LabelsEntry](#banyandb-database-v1-Node-LabelsEntry) | repeated | labels is a set of key-value pairs to describe the node. |
| property_repair_gossip_grpc_address | [string](#string) |  |  |






<a name="banyandb-database-v1-Node-LabelsEntry"></a>

### Node.LabelsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="banyandb-database-v1-Shard"></a>

### Shard



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [uint64](#uint64) |  |  |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |
| catalog | [banyandb.common.v1.Catalog](#banyandb-common-v1-Catalog) |  |  |
| node | [string](#string) |  |  |
| total | [uint32](#uint32) |  |  |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |





 


<a name="banyandb-database-v1-Role"></a>

### Role


| Name | Number | Description |
| ---- | ------ | ----------- |
| ROLE_UNSPECIFIED | 0 |  |
| ROLE_META | 1 |  |
| ROLE_DATA | 2 |  |
| ROLE_LIAISON | 3 |  |


 

 

 



<a name="banyandb_database_v1_schema-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/database/v1/schema.proto



<a name="banyandb-database-v1-Entity"></a>

### Entity



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tag_names | [string](#string) | repeated |  |






<a name="banyandb-database-v1-FieldSpec"></a>

### FieldSpec
FieldSpec is the specification of field


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | name is the identity of a field |
| field_type | [FieldType](#banyandb-database-v1-FieldType) |  | field_type denotes the type of field value |
| encoding_method | [EncodingMethod](#banyandb-database-v1-EncodingMethod) |  | encoding_method indicates how to encode data during writing |
| compression_method | [CompressionMethod](#banyandb-database-v1-CompressionMethod) |  | compression_method indicates how to compress data during writing |






<a name="banyandb-database-v1-IndexRule"></a>

### IndexRule
IndexRule defines how to generate indices based on tags and the index type
IndexRule should bind to a subject through an IndexRuleBinding to generate proper indices.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | metadata define the rule&#39;s identity |
| tags | [string](#string) | repeated | tags are the combination that refers to an indexed object If the elements in tags are more than 1, the object will generate a multi-tag index Caveat: All tags in a multi-tag MUST have an identical IndexType |
| type | [IndexRule.Type](#banyandb-database-v1-IndexRule-Type) |  | type is the IndexType of this IndexObject. |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at indicates when the IndexRule is updated |
| analyzer | [string](#string) |  | analyzer analyzes tag value to support the full-text searching for TYPE_INVERTED indices. available analyzers are: - &#34;standard&#34; provides grammar based tokenization - &#34;simple&#34; breaks text into tokens at any non-letter character, such as numbers, spaces, hyphens and apostrophes, discards non-letter characters, and changes uppercase to lowercase. - &#34;keyword&#34; is a “noop” analyzer which returns the entire input string as a single token. - &#34;url&#34; breaks test into tokens at any non-letter and non-digit character. |
| no_sort | [bool](#bool) |  | no_sort indicates whether the index is not for sorting. |






<a name="banyandb-database-v1-IndexRuleBinding"></a>

### IndexRuleBinding
IndexRuleBinding is a bridge to connect severalIndexRules to a subject
This binding is valid between begin_at_nanoseconds and expire_at_nanoseconds, that provides flexible strategies
to control how to generate time series indices.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | metadata is the identity of this binding |
| rules | [string](#string) | repeated | rules refers to the IndexRule |
| subject | [Subject](#banyandb-database-v1-Subject) |  | subject indicates the subject of binding action |
| begin_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | begin_at_nanoseconds is the timestamp, after which the binding will be active |
| expire_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | expire_at_nanoseconds it the timestamp, after which the binding will be inactive expire_at_nanoseconds must be larger than begin_at_nanoseconds |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at indicates when the IndexRuleBinding is updated |






<a name="banyandb-database-v1-Measure"></a>

### Measure
Measure intends to store data point


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | metadata is the identity of a measure |
| tag_families | [TagFamilySpec](#banyandb-database-v1-TagFamilySpec) | repeated | tag_families are for filter measures |
| fields | [FieldSpec](#banyandb-database-v1-FieldSpec) | repeated | fields denote measure values |
| entity | [Entity](#banyandb-database-v1-Entity) |  | entity indicates which tags will be to generate a series and shard a measure |
| interval | [string](#string) |  | interval indicates how frequently to send a data point valid time units are &#34;ns&#34;, &#34;us&#34; (or &#34;µs&#34;), &#34;ms&#34;, &#34;s&#34;, &#34;m&#34;, &#34;h&#34;, &#34;d&#34;. |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at indicates when the measure is updated |
| index_mode | [bool](#bool) |  | index_mode specifies whether the data should be stored exclusively in the index, meaning it will not be stored in the data storage system. |
| sharding_key | [ShardingKey](#banyandb-database-v1-ShardingKey) |  | sharding_key determines the distribution of TopN-related data. |






<a name="banyandb-database-v1-Property"></a>

### Property
Property stores the user defined data


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | metadata is the identity of a property |
| tags | [TagSpec](#banyandb-database-v1-TagSpec) | repeated | tag stores the content of a property |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at indicates when the property is updated |






<a name="banyandb-database-v1-ShardingKey"></a>

### ShardingKey



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tag_names | [string](#string) | repeated |  |






<a name="banyandb-database-v1-Stream"></a>

### Stream
Stream intends to store streaming data, for example, traces or logs


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | metadata is the identity of a trace series |
| tag_families | [TagFamilySpec](#banyandb-database-v1-TagFamilySpec) | repeated | tag_families |
| entity | [Entity](#banyandb-database-v1-Entity) |  | entity indicates how to generate a series and shard a stream |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at indicates when the stream is updated |






<a name="banyandb-database-v1-Subject"></a>

### Subject
Subject defines which stream or measure would generate indices


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| catalog | [banyandb.common.v1.Catalog](#banyandb-common-v1-Catalog) |  | catalog is where the subject belongs to todo validate plugin exist bug https://github.com/bufbuild/protoc-gen-validate/issues/672 |
| name | [string](#string) |  | name refers to a stream or measure in a particular catalog |






<a name="banyandb-database-v1-TagFamilySpec"></a>

### TagFamilySpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| tags | [TagSpec](#banyandb-database-v1-TagSpec) | repeated | tags defines accepted tags |






<a name="banyandb-database-v1-TagSpec"></a>

### TagSpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| type | [TagType](#banyandb-database-v1-TagType) |  |  |






<a name="banyandb-database-v1-TopNAggregation"></a>

### TopNAggregation
TopNAggregation generates offline TopN statistics for a measure&#39;s TopN approximation


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | metadata is the identity of an aggregation |
| source_measure | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | source_measure denotes the data source of this aggregation |
| field_name | [string](#string) |  | field_name is the name of field used for ranking |
| field_value_sort | [banyandb.model.v1.Sort](#banyandb-model-v1-Sort) |  | field_value_sort indicates how to sort fields ASC: bottomN DESC: topN UNSPECIFIED: topN &#43; bottomN todo validate plugin exist bug https://github.com/bufbuild/protoc-gen-validate/issues/672 |
| group_by_tag_names | [string](#string) | repeated | group_by_tag_names groups data points into statistical counters |
| criteria | [banyandb.model.v1.Criteria](#banyandb-model-v1-Criteria) |  | criteria select partial data points from measure |
| counters_number | [int32](#int32) |  | counters_number sets the number of counters to be tracked. The default value is 1000 |
| lru_size | [int32](#int32) |  | lru_size defines how much entry is allowed to be maintained in the memory |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at indicates when the measure is updated |






<a name="banyandb-database-v1-Trace"></a>

### Trace
Trace defines a tracing-specific storage resource.
It is suitable for storing traces and spans.
The name of a Trace is a logical namespace within a group,
while the group of a Trace corresponds to a physical directory.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | metadata is the identity of the trace resource. |
| tags | [TraceTagSpec](#banyandb-database-v1-TraceTagSpec) | repeated | tags are the specification of tags. |
| trace_id_tag_name | [string](#string) |  | trace_id_tag_name is the name of the tag that stores the trace ID. |
| timestamp_tag_name | [string](#string) |  | timestamp_tag_name is the name of the tag that stores the timestamp. |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at indicates when the trace resource is updated. |
| span_id_tag_name | [string](#string) |  | span_id_tag_name is the name of the tag that stores the span ID. |






<a name="banyandb-database-v1-TraceTagSpec"></a>

### TraceTagSpec
TraceTagSpec defines the specification of a tag in a trace.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | name is the name of the tag. |
| type | [TagType](#banyandb-database-v1-TagType) |  | type is the type of the tag. |





 


<a name="banyandb-database-v1-CompressionMethod"></a>

### CompressionMethod


| Name | Number | Description |
| ---- | ------ | ----------- |
| COMPRESSION_METHOD_UNSPECIFIED | 0 |  |
| COMPRESSION_METHOD_ZSTD | 1 |  |



<a name="banyandb-database-v1-EncodingMethod"></a>

### EncodingMethod


| Name | Number | Description |
| ---- | ------ | ----------- |
| ENCODING_METHOD_UNSPECIFIED | 0 |  |
| ENCODING_METHOD_GORILLA | 1 |  |



<a name="banyandb-database-v1-FieldType"></a>

### FieldType


| Name | Number | Description |
| ---- | ------ | ----------- |
| FIELD_TYPE_UNSPECIFIED | 0 |  |
| FIELD_TYPE_STRING | 1 |  |
| FIELD_TYPE_INT | 2 |  |
| FIELD_TYPE_DATA_BINARY | 3 |  |
| FIELD_TYPE_FLOAT | 4 |  |



<a name="banyandb-database-v1-IndexRule-Type"></a>

### IndexRule.Type
Type determine the index structure under the hood

| Name | Number | Description |
| ---- | ------ | ----------- |
| TYPE_UNSPECIFIED | 0 |  |
| TYPE_INVERTED | 1 |  |
| TYPE_SKIPPING | 2 |  |
| TYPE_TREE | 3 | TYPE_TREE is a tree index, which is used for storing hierarchical data. |



<a name="banyandb-database-v1-TagType"></a>

### TagType


| Name | Number | Description |
| ---- | ------ | ----------- |
| TAG_TYPE_UNSPECIFIED | 0 |  |
| TAG_TYPE_STRING | 1 |  |
| TAG_TYPE_INT | 2 |  |
| TAG_TYPE_STRING_ARRAY | 3 |  |
| TAG_TYPE_INT_ARRAY | 4 |  |
| TAG_TYPE_DATA_BINARY | 5 |  |
| TAG_TYPE_TIMESTAMP | 6 |  |


 

 

 



<a name="banyandb_database_v1_rpc-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/database/v1/rpc.proto



<a name="banyandb-database-v1-DataInfo"></a>

### DataInfo
DataInfo contains data storage information for a specific node.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node | [Node](#banyandb-database-v1-Node) |  | node is the node that stores this data. |
| segment_info | [SegmentInfo](#banyandb-database-v1-SegmentInfo) | repeated | segment_info contains information about each segment on this node. |
| data_size_bytes | [int64](#int64) |  | data_size_bytes is the total size of data on this node in bytes. |






<a name="banyandb-database-v1-GetClusterStateRequest"></a>

### GetClusterStateRequest







<a name="banyandb-database-v1-GetClusterStateResponse"></a>

### GetClusterStateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| route_tables | [GetClusterStateResponse.RouteTablesEntry](#banyandb-database-v1-GetClusterStateResponse-RouteTablesEntry) | repeated | Liaison node: map&#39;s key could be &#34;tire1&#34; and &#34;tire2&#34;. tire1 route traffic between liaison nodes, tire2 spread data among data nodes Data node: map&#39;s key could be &#34;property&#34; for gossip. Lifecycle agent: map&#39;s key could be the next stage&#39;s name. |






<a name="banyandb-database-v1-GetClusterStateResponse-RouteTablesEntry"></a>

### GetClusterStateResponse.RouteTablesEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [RouteTable](#banyandb-database-v1-RouteTable) |  |  |






<a name="banyandb-database-v1-GetCurrentNodeRequest"></a>

### GetCurrentNodeRequest







<a name="banyandb-database-v1-GetCurrentNodeResponse"></a>

### GetCurrentNodeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node | [Node](#banyandb-database-v1-Node) |  |  |






<a name="banyandb-database-v1-GroupDeletionTask"></a>

### GroupDeletionTask
GroupDeletionTask represents the status of a group deletion operation.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| current_phase | [GroupDeletionTask.Phase](#banyandb-database-v1-GroupDeletionTask-Phase) |  | current_phase is the current phase of the deletion task. |
| total_counts | [GroupDeletionTask.TotalCountsEntry](#banyandb-database-v1-GroupDeletionTask-TotalCountsEntry) | repeated | total_counts maps resource types to their total count. |
| deleted_counts | [GroupDeletionTask.DeletedCountsEntry](#banyandb-database-v1-GroupDeletionTask-DeletedCountsEntry) | repeated | deleted_counts maps resource types to the count of deleted resources. |
| total_data_size_bytes | [int64](#int64) |  | total_data_size_bytes is the total size of data to be deleted in bytes. |
| deleted_data_size_bytes | [int64](#int64) |  | deleted_data_size_bytes is the size of data that has been deleted in bytes. |
| message | [string](#string) |  | message provides additional information about the task status. |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | created_at is the timestamp when the task was created. |






<a name="banyandb-database-v1-GroupDeletionTask-DeletedCountsEntry"></a>

### GroupDeletionTask.DeletedCountsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [int32](#int32) |  |  |






<a name="banyandb-database-v1-GroupDeletionTask-TotalCountsEntry"></a>

### GroupDeletionTask.TotalCountsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [int32](#int32) |  |  |






<a name="banyandb-database-v1-GroupRegistryServiceCreateRequest"></a>

### GroupRegistryServiceCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [banyandb.common.v1.Group](#banyandb-common-v1-Group) |  |  |






<a name="banyandb-database-v1-GroupRegistryServiceCreateResponse"></a>

### GroupRegistryServiceCreateResponse







<a name="banyandb-database-v1-GroupRegistryServiceDeleteRequest"></a>

### GroupRegistryServiceDeleteRequest
GroupRegistryServiceDeleteRequest is the request for deleting a group.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  | group is the name of the group to delete. |
| dry_run | [bool](#bool) |  | dry_run indicates whether to perform a dry run without actually deleting data. When true, returns what would be deleted without making changes. |
| force | [bool](#bool) |  | force indicates whether to force delete the group even if it contains data. When false, deletion will fail if the group is not empty. |






<a name="banyandb-database-v1-GroupRegistryServiceDeleteResponse"></a>

### GroupRegistryServiceDeleteResponse
GroupRegistryServiceDeleteResponse is the response for deleting a group.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  | deleted indicates whether the group was deleted. |
| task_id | [string](#string) |  | task_id is the ID of the background deletion task. |






<a name="banyandb-database-v1-GroupRegistryServiceExistRequest"></a>

### GroupRegistryServiceExistRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |






<a name="banyandb-database-v1-GroupRegistryServiceExistResponse"></a>

### GroupRegistryServiceExistResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| has_group | [bool](#bool) |  |  |






<a name="banyandb-database-v1-GroupRegistryServiceGetRequest"></a>

### GroupRegistryServiceGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |






<a name="banyandb-database-v1-GroupRegistryServiceGetResponse"></a>

### GroupRegistryServiceGetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [banyandb.common.v1.Group](#banyandb-common-v1-Group) |  |  |






<a name="banyandb-database-v1-GroupRegistryServiceInspectRequest"></a>

### GroupRegistryServiceInspectRequest
GroupRegistryServiceInspectRequest is the request for inspecting a group&#39;s detailed information.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  | group is the name of the group to inspect. |






<a name="banyandb-database-v1-GroupRegistryServiceInspectResponse"></a>

### GroupRegistryServiceInspectResponse
GroupRegistryServiceInspectResponse is the response for inspecting a group.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [banyandb.common.v1.Group](#banyandb-common-v1-Group) |  | group contains the group metadata. |
| schema_info | [SchemaInfo](#banyandb-database-v1-SchemaInfo) |  | schema_info contains information about all schemas in the group. |
| data_info | [DataInfo](#banyandb-database-v1-DataInfo) | repeated | data_info contains data storage information for each node. |
| liaison_info | [LiaisonInfo](#banyandb-database-v1-LiaisonInfo) | repeated | liaison_info contains information about pending operations in liaison. |






<a name="banyandb-database-v1-GroupRegistryServiceListRequest"></a>

### GroupRegistryServiceListRequest







<a name="banyandb-database-v1-GroupRegistryServiceListResponse"></a>

### GroupRegistryServiceListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [banyandb.common.v1.Group](#banyandb-common-v1-Group) | repeated |  |






<a name="banyandb-database-v1-GroupRegistryServiceQueryRequest"></a>

### GroupRegistryServiceQueryRequest
GroupRegistryServiceQueryRequest is the request for querying a group deletion task.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  | group is the name of the group whose deletion task to query. |






<a name="banyandb-database-v1-GroupRegistryServiceQueryResponse"></a>

### GroupRegistryServiceQueryResponse
GroupRegistryServiceQueryResponse is the response for querying a group deletion task.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| task | [GroupDeletionTask](#banyandb-database-v1-GroupDeletionTask) |  | task contains the status of the deletion task. |






<a name="banyandb-database-v1-GroupRegistryServiceUpdateRequest"></a>

### GroupRegistryServiceUpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [banyandb.common.v1.Group](#banyandb-common-v1-Group) |  |  |






<a name="banyandb-database-v1-GroupRegistryServiceUpdateResponse"></a>

### GroupRegistryServiceUpdateResponse







<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceCreateRequest"></a>

### IndexRuleBindingRegistryServiceCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index_rule_binding | [IndexRuleBinding](#banyandb-database-v1-IndexRuleBinding) |  |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceCreateResponse"></a>

### IndexRuleBindingRegistryServiceCreateResponse







<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceDeleteRequest"></a>

### IndexRuleBindingRegistryServiceDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceDeleteResponse"></a>

### IndexRuleBindingRegistryServiceDeleteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceExistRequest"></a>

### IndexRuleBindingRegistryServiceExistRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceExistResponse"></a>

### IndexRuleBindingRegistryServiceExistResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| has_group | [bool](#bool) |  |  |
| has_index_rule_binding | [bool](#bool) |  |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceGetRequest"></a>

### IndexRuleBindingRegistryServiceGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceGetResponse"></a>

### IndexRuleBindingRegistryServiceGetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index_rule_binding | [IndexRuleBinding](#banyandb-database-v1-IndexRuleBinding) |  |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceListRequest"></a>

### IndexRuleBindingRegistryServiceListRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceListResponse"></a>

### IndexRuleBindingRegistryServiceListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index_rule_binding | [IndexRuleBinding](#banyandb-database-v1-IndexRuleBinding) | repeated |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceUpdateRequest"></a>

### IndexRuleBindingRegistryServiceUpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index_rule_binding | [IndexRuleBinding](#banyandb-database-v1-IndexRuleBinding) |  |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceUpdateResponse"></a>

### IndexRuleBindingRegistryServiceUpdateResponse







<a name="banyandb-database-v1-IndexRuleRegistryServiceCreateRequest"></a>

### IndexRuleRegistryServiceCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index_rule | [IndexRule](#banyandb-database-v1-IndexRule) |  |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceCreateResponse"></a>

### IndexRuleRegistryServiceCreateResponse







<a name="banyandb-database-v1-IndexRuleRegistryServiceDeleteRequest"></a>

### IndexRuleRegistryServiceDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceDeleteResponse"></a>

### IndexRuleRegistryServiceDeleteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceExistRequest"></a>

### IndexRuleRegistryServiceExistRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceExistResponse"></a>

### IndexRuleRegistryServiceExistResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| has_group | [bool](#bool) |  |  |
| has_index_rule | [bool](#bool) |  |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceGetRequest"></a>

### IndexRuleRegistryServiceGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceGetResponse"></a>

### IndexRuleRegistryServiceGetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index_rule | [IndexRule](#banyandb-database-v1-IndexRule) |  |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceListRequest"></a>

### IndexRuleRegistryServiceListRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceListResponse"></a>

### IndexRuleRegistryServiceListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index_rule | [IndexRule](#banyandb-database-v1-IndexRule) | repeated |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceUpdateRequest"></a>

### IndexRuleRegistryServiceUpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index_rule | [IndexRule](#banyandb-database-v1-IndexRule) |  |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceUpdateResponse"></a>

### IndexRuleRegistryServiceUpdateResponse







<a name="banyandb-database-v1-InvertedIndexInfo"></a>

### InvertedIndexInfo
InvertedIndexInfo contains information about the inverted index.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data_count | [int64](#int64) |  | data_count is the number of entries in the inverted index. |
| data_size_bytes | [int64](#int64) |  | data_size_bytes is the size of the inverted index in bytes. |






<a name="banyandb-database-v1-LiaisonInfo"></a>

### LiaisonInfo
LiaisonInfo contains information about pending operations in liaison.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pending_write_data_count | [int64](#int64) |  | pending_write_data_count is the number of data entries waiting to be written. |
| pending_sync_part_count | [int64](#int64) |  | pending_sync_part_count is the number of parts waiting to be synchronized. |
| pending_sync_data_size_bytes | [int64](#int64) |  | pending_sync_data_size_bytes is the size of data waiting to be synchronized in bytes. |
| pending_handoff_part_count | [int64](#int64) |  | pending_handoff_part_count is the number of parts waiting for handoff. |
| pending_handoff_data_size_bytes | [int64](#int64) |  | pending_handoff_data_size_bytes is the size of data waiting for handoff in bytes. |






<a name="banyandb-database-v1-MeasureRegistryServiceCreateRequest"></a>

### MeasureRegistryServiceCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| measure | [Measure](#banyandb-database-v1-Measure) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceCreateResponse"></a>

### MeasureRegistryServiceCreateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mod_revision | [int64](#int64) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceDeleteRequest"></a>

### MeasureRegistryServiceDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceDeleteResponse"></a>

### MeasureRegistryServiceDeleteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceExistRequest"></a>

### MeasureRegistryServiceExistRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceExistResponse"></a>

### MeasureRegistryServiceExistResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| has_group | [bool](#bool) |  |  |
| has_measure | [bool](#bool) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceGetRequest"></a>

### MeasureRegistryServiceGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceGetResponse"></a>

### MeasureRegistryServiceGetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| measure | [Measure](#banyandb-database-v1-Measure) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceListRequest"></a>

### MeasureRegistryServiceListRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceListResponse"></a>

### MeasureRegistryServiceListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| measure | [Measure](#banyandb-database-v1-Measure) | repeated |  |






<a name="banyandb-database-v1-MeasureRegistryServiceUpdateRequest"></a>

### MeasureRegistryServiceUpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| measure | [Measure](#banyandb-database-v1-Measure) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceUpdateResponse"></a>

### MeasureRegistryServiceUpdateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mod_revision | [int64](#int64) |  |  |






<a name="banyandb-database-v1-PropertyRegistryServiceCreateRequest"></a>

### PropertyRegistryServiceCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| property | [Property](#banyandb-database-v1-Property) |  |  |






<a name="banyandb-database-v1-PropertyRegistryServiceCreateResponse"></a>

### PropertyRegistryServiceCreateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mod_revision | [int64](#int64) |  |  |






<a name="banyandb-database-v1-PropertyRegistryServiceDeleteRequest"></a>

### PropertyRegistryServiceDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-PropertyRegistryServiceDeleteResponse"></a>

### PropertyRegistryServiceDeleteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |






<a name="banyandb-database-v1-PropertyRegistryServiceExistRequest"></a>

### PropertyRegistryServiceExistRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-PropertyRegistryServiceExistResponse"></a>

### PropertyRegistryServiceExistResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| has_group | [bool](#bool) |  |  |
| has_property | [bool](#bool) |  |  |






<a name="banyandb-database-v1-PropertyRegistryServiceGetRequest"></a>

### PropertyRegistryServiceGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-PropertyRegistryServiceGetResponse"></a>

### PropertyRegistryServiceGetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| property | [Property](#banyandb-database-v1-Property) |  |  |






<a name="banyandb-database-v1-PropertyRegistryServiceListRequest"></a>

### PropertyRegistryServiceListRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |






<a name="banyandb-database-v1-PropertyRegistryServiceListResponse"></a>

### PropertyRegistryServiceListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| properties | [Property](#banyandb-database-v1-Property) | repeated |  |






<a name="banyandb-database-v1-PropertyRegistryServiceUpdateRequest"></a>

### PropertyRegistryServiceUpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| property | [Property](#banyandb-database-v1-Property) |  |  |






<a name="banyandb-database-v1-PropertyRegistryServiceUpdateResponse"></a>

### PropertyRegistryServiceUpdateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mod_revision | [int64](#int64) |  |  |






<a name="banyandb-database-v1-RouteTable"></a>

### RouteTable
RouteTable represents a collection of nodes grouped by their health state.
It provides a view of nodes that are registered, actively healthy, and those being evicted.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| registered | [Node](#banyandb-database-v1-Node) | repeated | registered contains all nodes that have been discovered and registered in this route. |
| active | [string](#string) | repeated | active contains node names (Node.Metadata.Name) that are currently healthy and can handle requests. |
| evictable | [string](#string) | repeated | evictable contains node names (Node.Metadata.Name) that are unhealthy and being retried before eviction. |






<a name="banyandb-database-v1-SIDXInfo"></a>

### SIDXInfo
SIDXInfo contains information about sidx.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data_count | [int64](#int64) |  | data_count is the number of entries in sidx. |
| data_size_bytes | [int64](#int64) |  | data_size_bytes is the size of sidx in bytes. |
| part_count | [int64](#int64) |  | part_count is the number of parts in sidx. |






<a name="banyandb-database-v1-SchemaInfo"></a>

### SchemaInfo
SchemaInfo contains information about all schema objects in a group.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| streams | [string](#string) | repeated | streams is the list of stream names in the group. |
| measures | [string](#string) | repeated | measures is the list of measure names in the group. |
| traces | [string](#string) | repeated | traces is the list of trace names in the group. |
| properties | [string](#string) | repeated | properties is the list of property names in the group. |
| index_rules | [string](#string) | repeated | index_rules is the list of index rule names in the group. |
| index_rule_bindings | [string](#string) | repeated | index_rule_bindings is the list of index rule binding names in the group. |
| topn_aggregations | [string](#string) | repeated | topn_aggregations is the list of TopN aggregation names in the group. |






<a name="banyandb-database-v1-SegmentInfo"></a>

### SegmentInfo
SegmentInfo contains information about a specific time segment.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| segment_id | [string](#string) |  | segment_id is the unique identifier of the segment. |
| time_range_start | [string](#string) |  | time_range_start is the start time of the segment. |
| time_range_end | [string](#string) |  | time_range_end is the end time of the segment. |
| shard_info | [ShardInfo](#banyandb-database-v1-ShardInfo) | repeated | shard_info contains information about each shard in this segment. |
| series_index_info | [SeriesIndexInfo](#banyandb-database-v1-SeriesIndexInfo) |  | series_index_info contains information about the series index. |






<a name="banyandb-database-v1-SeriesIndexInfo"></a>

### SeriesIndexInfo
SeriesIndexInfo contains information about the series index.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data_count | [int64](#int64) |  | data_count is the number of entries in the series index. |
| data_size_bytes | [int64](#int64) |  | data_size_bytes is the size of the series index in bytes. |






<a name="banyandb-database-v1-ShardInfo"></a>

### ShardInfo
ShardInfo contains information about a specific shard.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| shard_id | [uint32](#uint32) |  | shard_id is the unique identifier of the shard. |
| data_count | [int64](#int64) |  | data_count is the number of data entries in this shard. |
| data_size_bytes | [int64](#int64) |  | data_size_bytes is the size of data in this shard in bytes. |
| part_count | [int64](#int64) |  | part_count is the number of parts in this shard. |
| inverted_index_info | [InvertedIndexInfo](#banyandb-database-v1-InvertedIndexInfo) |  | inverted_index_info contains information about the inverted index. |
| sidx_info | [SIDXInfo](#banyandb-database-v1-SIDXInfo) |  | sidx_info contains information about sidx. |






<a name="banyandb-database-v1-Snapshot"></a>

### Snapshot



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| catalog | [banyandb.common.v1.Catalog](#banyandb-common-v1-Catalog) |  |  |
| name | [string](#string) |  |  |
| error | [string](#string) |  |  |






<a name="banyandb-database-v1-SnapshotRequest"></a>

### SnapshotRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| groups | [SnapshotRequest.Group](#banyandb-database-v1-SnapshotRequest-Group) | repeated |  |






<a name="banyandb-database-v1-SnapshotRequest-Group"></a>

### SnapshotRequest.Group



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| catalog | [banyandb.common.v1.Catalog](#banyandb-common-v1-Catalog) |  |  |
| group | [string](#string) |  |  |






<a name="banyandb-database-v1-SnapshotResponse"></a>

### SnapshotResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| snapshots | [Snapshot](#banyandb-database-v1-Snapshot) | repeated |  |






<a name="banyandb-database-v1-StreamRegistryServiceCreateRequest"></a>

### StreamRegistryServiceCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stream | [Stream](#banyandb-database-v1-Stream) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceCreateResponse"></a>

### StreamRegistryServiceCreateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mod_revision | [int64](#int64) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceDeleteRequest"></a>

### StreamRegistryServiceDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceDeleteResponse"></a>

### StreamRegistryServiceDeleteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceExistRequest"></a>

### StreamRegistryServiceExistRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceExistResponse"></a>

### StreamRegistryServiceExistResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| has_group | [bool](#bool) |  |  |
| has_stream | [bool](#bool) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceGetRequest"></a>

### StreamRegistryServiceGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceGetResponse"></a>

### StreamRegistryServiceGetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stream | [Stream](#banyandb-database-v1-Stream) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceListRequest"></a>

### StreamRegistryServiceListRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceListResponse"></a>

### StreamRegistryServiceListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stream | [Stream](#banyandb-database-v1-Stream) | repeated |  |






<a name="banyandb-database-v1-StreamRegistryServiceUpdateRequest"></a>

### StreamRegistryServiceUpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stream | [Stream](#banyandb-database-v1-Stream) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceUpdateResponse"></a>

### StreamRegistryServiceUpdateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mod_revision | [int64](#int64) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceCreateRequest"></a>

### TopNAggregationRegistryServiceCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| top_n_aggregation | [TopNAggregation](#banyandb-database-v1-TopNAggregation) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceCreateResponse"></a>

### TopNAggregationRegistryServiceCreateResponse







<a name="banyandb-database-v1-TopNAggregationRegistryServiceDeleteRequest"></a>

### TopNAggregationRegistryServiceDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceDeleteResponse"></a>

### TopNAggregationRegistryServiceDeleteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceExistRequest"></a>

### TopNAggregationRegistryServiceExistRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceExistResponse"></a>

### TopNAggregationRegistryServiceExistResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| has_group | [bool](#bool) |  |  |
| has_top_n_aggregation | [bool](#bool) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceGetRequest"></a>

### TopNAggregationRegistryServiceGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceGetResponse"></a>

### TopNAggregationRegistryServiceGetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| top_n_aggregation | [TopNAggregation](#banyandb-database-v1-TopNAggregation) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceListRequest"></a>

### TopNAggregationRegistryServiceListRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceListResponse"></a>

### TopNAggregationRegistryServiceListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| top_n_aggregation | [TopNAggregation](#banyandb-database-v1-TopNAggregation) | repeated |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceUpdateRequest"></a>

### TopNAggregationRegistryServiceUpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| top_n_aggregation | [TopNAggregation](#banyandb-database-v1-TopNAggregation) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceUpdateResponse"></a>

### TopNAggregationRegistryServiceUpdateResponse







<a name="banyandb-database-v1-TraceRegistryServiceCreateRequest"></a>

### TraceRegistryServiceCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| trace | [Trace](#banyandb-database-v1-Trace) |  |  |






<a name="banyandb-database-v1-TraceRegistryServiceCreateResponse"></a>

### TraceRegistryServiceCreateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mod_revision | [int64](#int64) |  |  |






<a name="banyandb-database-v1-TraceRegistryServiceDeleteRequest"></a>

### TraceRegistryServiceDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-TraceRegistryServiceDeleteResponse"></a>

### TraceRegistryServiceDeleteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |






<a name="banyandb-database-v1-TraceRegistryServiceExistRequest"></a>

### TraceRegistryServiceExistRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-TraceRegistryServiceExistResponse"></a>

### TraceRegistryServiceExistResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| has_group | [bool](#bool) |  |  |
| has_trace | [bool](#bool) |  |  |






<a name="banyandb-database-v1-TraceRegistryServiceGetRequest"></a>

### TraceRegistryServiceGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-TraceRegistryServiceGetResponse"></a>

### TraceRegistryServiceGetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| trace | [Trace](#banyandb-database-v1-Trace) |  |  |






<a name="banyandb-database-v1-TraceRegistryServiceListRequest"></a>

### TraceRegistryServiceListRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |






<a name="banyandb-database-v1-TraceRegistryServiceListResponse"></a>

### TraceRegistryServiceListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| trace | [Trace](#banyandb-database-v1-Trace) | repeated |  |






<a name="banyandb-database-v1-TraceRegistryServiceUpdateRequest"></a>

### TraceRegistryServiceUpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| trace | [Trace](#banyandb-database-v1-Trace) |  |  |






<a name="banyandb-database-v1-TraceRegistryServiceUpdateResponse"></a>

### TraceRegistryServiceUpdateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mod_revision | [int64](#int64) |  |  |





 


<a name="banyandb-database-v1-GroupDeletionTask-Phase"></a>

### GroupDeletionTask.Phase
Phase represents the current phase of the deletion task.

| Name | Number | Description |
| ---- | ------ | ----------- |
| PHASE_UNSPECIFIED | 0 |  |
| PHASE_PENDING | 1 | PHASE_PENDING indicates the task is waiting to start. |
| PHASE_IN_PROGRESS | 2 | PHASE_IN_PROGRESS indicates the task is currently executing. |
| PHASE_COMPLETED | 3 | PHASE_COMPLETED indicates the task has completed successfully. |
| PHASE_FAILED | 4 | PHASE_FAILED indicates the task has failed. |


 

 


<a name="banyandb-database-v1-ClusterStateService"></a>

### ClusterStateService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetClusterState | [GetClusterStateRequest](#banyandb-database-v1-GetClusterStateRequest) | [GetClusterStateResponse](#banyandb-database-v1-GetClusterStateResponse) |  |


<a name="banyandb-database-v1-GroupRegistryService"></a>

### GroupRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [GroupRegistryServiceCreateRequest](#banyandb-database-v1-GroupRegistryServiceCreateRequest) | [GroupRegistryServiceCreateResponse](#banyandb-database-v1-GroupRegistryServiceCreateResponse) |  |
| Update | [GroupRegistryServiceUpdateRequest](#banyandb-database-v1-GroupRegistryServiceUpdateRequest) | [GroupRegistryServiceUpdateResponse](#banyandb-database-v1-GroupRegistryServiceUpdateResponse) |  |
| Delete | [GroupRegistryServiceDeleteRequest](#banyandb-database-v1-GroupRegistryServiceDeleteRequest) | [GroupRegistryServiceDeleteResponse](#banyandb-database-v1-GroupRegistryServiceDeleteResponse) | Delete removes a group and all its associated data. Supports dry-run mode to preview what would be deleted. |
| Get | [GroupRegistryServiceGetRequest](#banyandb-database-v1-GroupRegistryServiceGetRequest) | [GroupRegistryServiceGetResponse](#banyandb-database-v1-GroupRegistryServiceGetResponse) |  |
| List | [GroupRegistryServiceListRequest](#banyandb-database-v1-GroupRegistryServiceListRequest) | [GroupRegistryServiceListResponse](#banyandb-database-v1-GroupRegistryServiceListResponse) |  |
| Exist | [GroupRegistryServiceExistRequest](#banyandb-database-v1-GroupRegistryServiceExistRequest) | [GroupRegistryServiceExistResponse](#banyandb-database-v1-GroupRegistryServiceExistResponse) | Exist doesn&#39;t expose an HTTP endpoint. Please use HEAD method to touch Get instead |
| Inspect | [GroupRegistryServiceInspectRequest](#banyandb-database-v1-GroupRegistryServiceInspectRequest) | [GroupRegistryServiceInspectResponse](#banyandb-database-v1-GroupRegistryServiceInspectResponse) | Inspect retrieves detailed information about a group including its schemas, data distribution, and pending operations. |
| Query | [GroupRegistryServiceQueryRequest](#banyandb-database-v1-GroupRegistryServiceQueryRequest) | [GroupRegistryServiceQueryResponse](#banyandb-database-v1-GroupRegistryServiceQueryResponse) | Query retrieves the status of a group deletion task. |


<a name="banyandb-database-v1-IndexRuleBindingRegistryService"></a>

### IndexRuleBindingRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [IndexRuleBindingRegistryServiceCreateRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceCreateRequest) | [IndexRuleBindingRegistryServiceCreateResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceCreateResponse) |  |
| Update | [IndexRuleBindingRegistryServiceUpdateRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceUpdateRequest) | [IndexRuleBindingRegistryServiceUpdateResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceUpdateResponse) |  |
| Delete | [IndexRuleBindingRegistryServiceDeleteRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceDeleteRequest) | [IndexRuleBindingRegistryServiceDeleteResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceDeleteResponse) |  |
| Get | [IndexRuleBindingRegistryServiceGetRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceGetRequest) | [IndexRuleBindingRegistryServiceGetResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceGetResponse) |  |
| List | [IndexRuleBindingRegistryServiceListRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceListRequest) | [IndexRuleBindingRegistryServiceListResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceListResponse) |  |
| Exist | [IndexRuleBindingRegistryServiceExistRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceExistRequest) | [IndexRuleBindingRegistryServiceExistResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceExistResponse) | Exist doesn&#39;t expose an HTTP endpoint. Please use HEAD method to touch Get instead |


<a name="banyandb-database-v1-IndexRuleRegistryService"></a>

### IndexRuleRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [IndexRuleRegistryServiceCreateRequest](#banyandb-database-v1-IndexRuleRegistryServiceCreateRequest) | [IndexRuleRegistryServiceCreateResponse](#banyandb-database-v1-IndexRuleRegistryServiceCreateResponse) |  |
| Update | [IndexRuleRegistryServiceUpdateRequest](#banyandb-database-v1-IndexRuleRegistryServiceUpdateRequest) | [IndexRuleRegistryServiceUpdateResponse](#banyandb-database-v1-IndexRuleRegistryServiceUpdateResponse) |  |
| Delete | [IndexRuleRegistryServiceDeleteRequest](#banyandb-database-v1-IndexRuleRegistryServiceDeleteRequest) | [IndexRuleRegistryServiceDeleteResponse](#banyandb-database-v1-IndexRuleRegistryServiceDeleteResponse) |  |
| Get | [IndexRuleRegistryServiceGetRequest](#banyandb-database-v1-IndexRuleRegistryServiceGetRequest) | [IndexRuleRegistryServiceGetResponse](#banyandb-database-v1-IndexRuleRegistryServiceGetResponse) |  |
| List | [IndexRuleRegistryServiceListRequest](#banyandb-database-v1-IndexRuleRegistryServiceListRequest) | [IndexRuleRegistryServiceListResponse](#banyandb-database-v1-IndexRuleRegistryServiceListResponse) |  |
| Exist | [IndexRuleRegistryServiceExistRequest](#banyandb-database-v1-IndexRuleRegistryServiceExistRequest) | [IndexRuleRegistryServiceExistResponse](#banyandb-database-v1-IndexRuleRegistryServiceExistResponse) | Exist doesn&#39;t expose an HTTP endpoint. Please use HEAD method to touch Get instead |


<a name="banyandb-database-v1-MeasureRegistryService"></a>

### MeasureRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [MeasureRegistryServiceCreateRequest](#banyandb-database-v1-MeasureRegistryServiceCreateRequest) | [MeasureRegistryServiceCreateResponse](#banyandb-database-v1-MeasureRegistryServiceCreateResponse) |  |
| Update | [MeasureRegistryServiceUpdateRequest](#banyandb-database-v1-MeasureRegistryServiceUpdateRequest) | [MeasureRegistryServiceUpdateResponse](#banyandb-database-v1-MeasureRegistryServiceUpdateResponse) |  |
| Delete | [MeasureRegistryServiceDeleteRequest](#banyandb-database-v1-MeasureRegistryServiceDeleteRequest) | [MeasureRegistryServiceDeleteResponse](#banyandb-database-v1-MeasureRegistryServiceDeleteResponse) |  |
| Get | [MeasureRegistryServiceGetRequest](#banyandb-database-v1-MeasureRegistryServiceGetRequest) | [MeasureRegistryServiceGetResponse](#banyandb-database-v1-MeasureRegistryServiceGetResponse) |  |
| List | [MeasureRegistryServiceListRequest](#banyandb-database-v1-MeasureRegistryServiceListRequest) | [MeasureRegistryServiceListResponse](#banyandb-database-v1-MeasureRegistryServiceListResponse) |  |
| Exist | [MeasureRegistryServiceExistRequest](#banyandb-database-v1-MeasureRegistryServiceExistRequest) | [MeasureRegistryServiceExistResponse](#banyandb-database-v1-MeasureRegistryServiceExistResponse) | Exist doesn&#39;t expose an HTTP endpoint. Please use HEAD method to touch Get instead |


<a name="banyandb-database-v1-NodeQueryService"></a>

### NodeQueryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetCurrentNode | [GetCurrentNodeRequest](#banyandb-database-v1-GetCurrentNodeRequest) | [GetCurrentNodeResponse](#banyandb-database-v1-GetCurrentNodeResponse) |  |


<a name="banyandb-database-v1-PropertyRegistryService"></a>

### PropertyRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [PropertyRegistryServiceCreateRequest](#banyandb-database-v1-PropertyRegistryServiceCreateRequest) | [PropertyRegistryServiceCreateResponse](#banyandb-database-v1-PropertyRegistryServiceCreateResponse) |  |
| Update | [PropertyRegistryServiceUpdateRequest](#banyandb-database-v1-PropertyRegistryServiceUpdateRequest) | [PropertyRegistryServiceUpdateResponse](#banyandb-database-v1-PropertyRegistryServiceUpdateResponse) |  |
| Delete | [PropertyRegistryServiceDeleteRequest](#banyandb-database-v1-PropertyRegistryServiceDeleteRequest) | [PropertyRegistryServiceDeleteResponse](#banyandb-database-v1-PropertyRegistryServiceDeleteResponse) |  |
| Get | [PropertyRegistryServiceGetRequest](#banyandb-database-v1-PropertyRegistryServiceGetRequest) | [PropertyRegistryServiceGetResponse](#banyandb-database-v1-PropertyRegistryServiceGetResponse) |  |
| List | [PropertyRegistryServiceListRequest](#banyandb-database-v1-PropertyRegistryServiceListRequest) | [PropertyRegistryServiceListResponse](#banyandb-database-v1-PropertyRegistryServiceListResponse) |  |
| Exist | [PropertyRegistryServiceExistRequest](#banyandb-database-v1-PropertyRegistryServiceExistRequest) | [PropertyRegistryServiceExistResponse](#banyandb-database-v1-PropertyRegistryServiceExistResponse) | Exist doesn&#39;t expose an HTTP endpoint. Please use HEAD method to touch Get instead |


<a name="banyandb-database-v1-SnapshotService"></a>

### SnapshotService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Snapshot | [SnapshotRequest](#banyandb-database-v1-SnapshotRequest) | [SnapshotResponse](#banyandb-database-v1-SnapshotResponse) |  |


<a name="banyandb-database-v1-StreamRegistryService"></a>

### StreamRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [StreamRegistryServiceCreateRequest](#banyandb-database-v1-StreamRegistryServiceCreateRequest) | [StreamRegistryServiceCreateResponse](#banyandb-database-v1-StreamRegistryServiceCreateResponse) |  |
| Update | [StreamRegistryServiceUpdateRequest](#banyandb-database-v1-StreamRegistryServiceUpdateRequest) | [StreamRegistryServiceUpdateResponse](#banyandb-database-v1-StreamRegistryServiceUpdateResponse) |  |
| Delete | [StreamRegistryServiceDeleteRequest](#banyandb-database-v1-StreamRegistryServiceDeleteRequest) | [StreamRegistryServiceDeleteResponse](#banyandb-database-v1-StreamRegistryServiceDeleteResponse) |  |
| Get | [StreamRegistryServiceGetRequest](#banyandb-database-v1-StreamRegistryServiceGetRequest) | [StreamRegistryServiceGetResponse](#banyandb-database-v1-StreamRegistryServiceGetResponse) |  |
| List | [StreamRegistryServiceListRequest](#banyandb-database-v1-StreamRegistryServiceListRequest) | [StreamRegistryServiceListResponse](#banyandb-database-v1-StreamRegistryServiceListResponse) |  |
| Exist | [StreamRegistryServiceExistRequest](#banyandb-database-v1-StreamRegistryServiceExistRequest) | [StreamRegistryServiceExistResponse](#banyandb-database-v1-StreamRegistryServiceExistResponse) | Exist doesn&#39;t expose an HTTP endpoint. Please use HEAD method to touch Get instead |


<a name="banyandb-database-v1-TopNAggregationRegistryService"></a>

### TopNAggregationRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [TopNAggregationRegistryServiceCreateRequest](#banyandb-database-v1-TopNAggregationRegistryServiceCreateRequest) | [TopNAggregationRegistryServiceCreateResponse](#banyandb-database-v1-TopNAggregationRegistryServiceCreateResponse) |  |
| Update | [TopNAggregationRegistryServiceUpdateRequest](#banyandb-database-v1-TopNAggregationRegistryServiceUpdateRequest) | [TopNAggregationRegistryServiceUpdateResponse](#banyandb-database-v1-TopNAggregationRegistryServiceUpdateResponse) |  |
| Delete | [TopNAggregationRegistryServiceDeleteRequest](#banyandb-database-v1-TopNAggregationRegistryServiceDeleteRequest) | [TopNAggregationRegistryServiceDeleteResponse](#banyandb-database-v1-TopNAggregationRegistryServiceDeleteResponse) |  |
| Get | [TopNAggregationRegistryServiceGetRequest](#banyandb-database-v1-TopNAggregationRegistryServiceGetRequest) | [TopNAggregationRegistryServiceGetResponse](#banyandb-database-v1-TopNAggregationRegistryServiceGetResponse) |  |
| List | [TopNAggregationRegistryServiceListRequest](#banyandb-database-v1-TopNAggregationRegistryServiceListRequest) | [TopNAggregationRegistryServiceListResponse](#banyandb-database-v1-TopNAggregationRegistryServiceListResponse) |  |
| Exist | [TopNAggregationRegistryServiceExistRequest](#banyandb-database-v1-TopNAggregationRegistryServiceExistRequest) | [TopNAggregationRegistryServiceExistResponse](#banyandb-database-v1-TopNAggregationRegistryServiceExistResponse) | Exist doesn&#39;t expose an HTTP endpoint. Please use HEAD method to touch Get instead |


<a name="banyandb-database-v1-TraceRegistryService"></a>

### TraceRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [TraceRegistryServiceCreateRequest](#banyandb-database-v1-TraceRegistryServiceCreateRequest) | [TraceRegistryServiceCreateResponse](#banyandb-database-v1-TraceRegistryServiceCreateResponse) |  |
| Update | [TraceRegistryServiceUpdateRequest](#banyandb-database-v1-TraceRegistryServiceUpdateRequest) | [TraceRegistryServiceUpdateResponse](#banyandb-database-v1-TraceRegistryServiceUpdateResponse) |  |
| Delete | [TraceRegistryServiceDeleteRequest](#banyandb-database-v1-TraceRegistryServiceDeleteRequest) | [TraceRegistryServiceDeleteResponse](#banyandb-database-v1-TraceRegistryServiceDeleteResponse) |  |
| Get | [TraceRegistryServiceGetRequest](#banyandb-database-v1-TraceRegistryServiceGetRequest) | [TraceRegistryServiceGetResponse](#banyandb-database-v1-TraceRegistryServiceGetResponse) |  |
| List | [TraceRegistryServiceListRequest](#banyandb-database-v1-TraceRegistryServiceListRequest) | [TraceRegistryServiceListResponse](#banyandb-database-v1-TraceRegistryServiceListResponse) |  |
| Exist | [TraceRegistryServiceExistRequest](#banyandb-database-v1-TraceRegistryServiceExistRequest) | [TraceRegistryServiceExistResponse](#banyandb-database-v1-TraceRegistryServiceExistResponse) | Exist doesn&#39;t expose an HTTP endpoint. Please use HEAD method to touch Get instead |

 



<a name="banyandb_fodc_v1_rpc-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/fodc/v1/rpc.proto



<a name="banyandb-fodc-v1-Metric"></a>

### Metric



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| labels | [Metric.LabelsEntry](#banyandb-fodc-v1-Metric-LabelsEntry) | repeated |  |
| value | [double](#double) |  |  |
| description | [string](#string) |  |  |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |






<a name="banyandb-fodc-v1-Metric-LabelsEntry"></a>

### Metric.LabelsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="banyandb-fodc-v1-RegisterAgentRequest"></a>

### RegisterAgentRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_role | [string](#string) |  |  |
| labels | [RegisterAgentRequest.LabelsEntry](#banyandb-fodc-v1-RegisterAgentRequest-LabelsEntry) | repeated |  |
| pod_name | [string](#string) |  |  |
| container_names | [string](#string) | repeated |  |






<a name="banyandb-fodc-v1-RegisterAgentRequest-LabelsEntry"></a>

### RegisterAgentRequest.LabelsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="banyandb-fodc-v1-RegisterAgentResponse"></a>

### RegisterAgentResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | [bool](#bool) |  |  |
| message | [string](#string) |  |  |
| heartbeat_interval_seconds | [int64](#int64) |  |  |
| agent_id | [string](#string) |  |  |






<a name="banyandb-fodc-v1-StreamMetricsRequest"></a>

### StreamMetricsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metrics | [Metric](#banyandb-fodc-v1-Metric) | repeated |  |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |






<a name="banyandb-fodc-v1-StreamMetricsResponse"></a>

### StreamMetricsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| start_time | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Optional start time for time window |
| end_time | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Optional end time for time window |





 

 

 


<a name="banyandb-fodc-v1-FODCService"></a>

### FODCService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| RegisterAgent | [RegisterAgentRequest](#banyandb-fodc-v1-RegisterAgentRequest) stream | [RegisterAgentResponse](#banyandb-fodc-v1-RegisterAgentResponse) stream | Bi-directional stream for agent registration and heartbeat |
| StreamMetrics | [StreamMetricsRequest](#banyandb-fodc-v1-StreamMetricsRequest) stream | [StreamMetricsResponse](#banyandb-fodc-v1-StreamMetricsResponse) stream | Bi-directional stream for metrics Agent sends StreamMetricsRequest (metrics data), Proxy sends StreamMetricsResponse (metrics requests) |

 



<a name="banyandb_measure_v1_write-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/measure/v1/write.proto



<a name="banyandb-measure-v1-DataPointSpec"></a>

### DataPointSpec
DataPointSpec defines the specification of a data point.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tag_family_spec | [TagFamilySpec](#banyandb-measure-v1-TagFamilySpec) | repeated | the tag family specification |
| field_names | [string](#string) | repeated | the field names |






<a name="banyandb-measure-v1-DataPointValue"></a>

### DataPointValue
DataPointValue is the data point for writing. It only contains values.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | timestamp is in the timeunit of milliseconds. |
| tag_families | [banyandb.model.v1.TagFamilyForWrite](#banyandb-model-v1-TagFamilyForWrite) | repeated | the order of tag_families&#39; items match DataPointSpec |
| fields | [banyandb.model.v1.FieldValue](#banyandb-model-v1-FieldValue) | repeated | the order of fields match DataPointSpec |
| version | [int64](#int64) |  | the version of the data point |






<a name="banyandb-measure-v1-InternalWriteRequest"></a>

### InternalWriteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| shard_id | [uint32](#uint32) |  |  |
| entity_values | [banyandb.model.v1.TagValue](#banyandb-model-v1-TagValue) | repeated |  |
| request | [WriteRequest](#banyandb-measure-v1-WriteRequest) |  |  |






<a name="banyandb-measure-v1-TagFamilySpec"></a>

### TagFamilySpec
TagFamilySpec defines the specification of a tag family.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | name of the tag family |
| tag_names | [string](#string) | repeated | names of tags in the tag family |






<a name="banyandb-measure-v1-WriteRequest"></a>

### WriteRequest
WriteRequest is the request contract for write


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | the metadata is required only for the first request of gRPC stream. |
| data_point | [DataPointValue](#banyandb-measure-v1-DataPointValue) |  | the data_point is required. |
| message_id | [uint64](#uint64) |  | the message_id is required. |
| data_point_spec | [DataPointSpec](#banyandb-measure-v1-DataPointSpec) |  | the data point specification. If this is not set with the indicated metadata, use the schema definition. If this is not set, use the existing spec declaration from previous requests in the current gRPC stream. |






<a name="banyandb-measure-v1-WriteResponse"></a>

### WriteResponse
WriteResponse is the response contract for write


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message_id | [uint64](#uint64) |  | the message_id from request. |
| status | [string](#string) |  | status indicates the request processing result |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | the metadata from request when request fails |





 

 

 

 



<a name="banyandb_measure_v1_rpc-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/measure/v1/rpc.proto



<a name="banyandb-measure-v1-DeleteExpiredSegmentsRequest"></a>

### DeleteExpiredSegmentsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |
| segment_suffixes | [string](#string) | repeated |  |






<a name="banyandb-measure-v1-DeleteExpiredSegmentsResponse"></a>

### DeleteExpiredSegmentsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [int64](#int64) |  |  |





 

 

 


<a name="banyandb-measure-v1-MeasureService"></a>

### MeasureService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Query | [QueryRequest](#banyandb-measure-v1-QueryRequest) | [QueryResponse](#banyandb-measure-v1-QueryResponse) |  |
| InternalQuery | [InternalQueryRequest](#banyandb-measure-v1-InternalQueryRequest) | [InternalQueryResponse](#banyandb-measure-v1-InternalQueryResponse) | InternalQuery is used for internal distributed query between liaison and data nodes. Returns InternalQueryResponse with shard information for proper deduplication. |
| Write | [WriteRequest](#banyandb-measure-v1-WriteRequest) stream | [WriteResponse](#banyandb-measure-v1-WriteResponse) stream |  |
| TopN | [TopNRequest](#banyandb-measure-v1-TopNRequest) | [TopNResponse](#banyandb-measure-v1-TopNResponse) |  |
| DeleteExpiredSegments | [DeleteExpiredSegmentsRequest](#banyandb-measure-v1-DeleteExpiredSegmentsRequest) | [DeleteExpiredSegmentsResponse](#banyandb-measure-v1-DeleteExpiredSegmentsResponse) |  |

 



<a name="banyandb_property_v1_gossip-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/property/v1/gossip.proto



<a name="banyandb-property-v1-PropagationContext"></a>

### PropagationContext



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| nodes | [string](#string) | repeated |  |
| max_propagation_count | [int32](#int32) |  |  |
| current_propagation_count | [int32](#int32) |  |  |
| origin_node | [string](#string) |  |  |






<a name="banyandb-property-v1-PropagationRequest"></a>

### PropagationRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| context | [PropagationContext](#banyandb-property-v1-PropagationContext) |  |  |
| trace_context | [PropagationTraceContext](#banyandb-property-v1-PropagationTraceContext) |  |  |
| group | [string](#string) |  |  |
| shard_id | [uint32](#uint32) |  |  |






<a name="banyandb-property-v1-PropagationResponse"></a>

### PropagationResponse







<a name="banyandb-property-v1-PropagationTraceContext"></a>

### PropagationTraceContext



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| trace_id | [string](#string) |  |  |
| parent_span_id | [string](#string) |  |  |





 

 

 


<a name="banyandb-property-v1-GossipService"></a>

### GossipService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Propagation | [PropagationRequest](#banyandb-property-v1-PropagationRequest) | [PropagationResponse](#banyandb-property-v1-PropagationResponse) | Propagation is used to propagate messages across nodes in the cluster. |

 



<a name="banyandb_property_v1_repair-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/property/v1/repair.proto



<a name="banyandb-property-v1-DifferTreeSummary"></a>

### DifferTreeSummary



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| nodes | [TreeLeafNode](#banyandb-property-v1-TreeLeafNode) | repeated | if the nodes is empty, mean the server side don&#39;t have more tree leaf nodes to send. |






<a name="banyandb-property-v1-PropertyMissing"></a>

### PropertyMissing



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entity | [string](#string) |  |  |






<a name="banyandb-property-v1-PropertySync"></a>

### PropertySync



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [bytes](#bytes) |  |  |
| property | [Property](#banyandb-property-v1-Property) |  |  |
| delete_time | [int64](#int64) |  |  |






<a name="banyandb-property-v1-PropertySyncWithFrom"></a>

### PropertySyncWithFrom



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| from | [PropertySyncFromType](#banyandb-property-v1-PropertySyncFromType) |  |  |
| property | [PropertySync](#banyandb-property-v1-PropertySync) |  |  |






<a name="banyandb-property-v1-RepairRequest"></a>

### RepairRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tree_root | [TreeRoot](#banyandb-property-v1-TreeRoot) |  | compare stage |
| tree_slots | [TreeSlots](#banyandb-property-v1-TreeSlots) |  |  |
| property_missing | [PropertyMissing](#banyandb-property-v1-PropertyMissing) |  | repair stage case 1: client missing but server existing |
| property_sync | [PropertySync](#banyandb-property-v1-PropertySync) |  | case 2: client existing but server missing case 3: SHA value mismatches |
| wait_next_differ | [WaitNextDifferData](#banyandb-property-v1-WaitNextDifferData) |  | wait next differ tree summary for process |






<a name="banyandb-property-v1-RepairResponse"></a>

### RepairResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| root_compare | [RootCompare](#banyandb-property-v1-RootCompare) |  | compare stage |
| differ_tree_summary | [DifferTreeSummary](#banyandb-property-v1-DifferTreeSummary) |  |  |
| property_sync | [PropertySyncWithFrom](#banyandb-property-v1-PropertySyncWithFrom) |  | repair stage case 1: return from PropertyMissing case 3: return if the client is older |






<a name="banyandb-property-v1-RootCompare"></a>

### RootCompare



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tree_found | [bool](#bool) |  |  |
| root_sha_match | [bool](#bool) |  |  |






<a name="banyandb-property-v1-TreeLeafNode"></a>

### TreeLeafNode



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot_index | [int32](#int32) |  | slot_index is the index of the slot in the tree. |
| exists | [bool](#bool) |  | if the slot is empty, means the server side don&#39;t have the slot. |
| entity | [string](#string) |  | if the slot and entity exists, the SHA value of the entity. |
| sha | [string](#string) |  |  |






<a name="banyandb-property-v1-TreeRoot"></a>

### TreeRoot



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |
| shard_id | [uint32](#uint32) |  |  |
| root_sha | [string](#string) |  |  |






<a name="banyandb-property-v1-TreeSlotSHA"></a>

### TreeSlotSHA



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot | [int32](#int32) |  |  |
| value | [string](#string) |  |  |






<a name="banyandb-property-v1-TreeSlots"></a>

### TreeSlots



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot_sha | [TreeSlotSHA](#banyandb-property-v1-TreeSlotSHA) | repeated |  |






<a name="banyandb-property-v1-WaitNextDifferData"></a>

### WaitNextDifferData






 


<a name="banyandb-property-v1-PropertySyncFromType"></a>

### PropertySyncFromType


| Name | Number | Description |
| ---- | ------ | ----------- |
| PROPERTY_SYNC_FROM_TYPE_UNSPECIFIED | 0 |  |
| PROPERTY_SYNC_FROM_TYPE_MISSING | 1 | client missing but server existing |
| PROPERTY_SYNC_FROM_TYPE_SYNC | 2 | client existing but server missing or SHA value mismatches |


 

 


<a name="banyandb-property-v1-RepairService"></a>

### RepairService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Repair | [RepairRequest](#banyandb-property-v1-RepairRequest) stream | [RepairResponse](#banyandb-property-v1-RepairResponse) stream |  |

 



<a name="banyandb_stream_v1_write-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/stream/v1/write.proto



<a name="banyandb-stream-v1-ElementValue"></a>

### ElementValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element_id | [string](#string) |  | element_id could be span_id of a Span or segment_id of a Segment in the context of stream |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | timestamp is in the timeunit of milliseconds. It represents 1) either the start time of a Span/Segment, 2) or the timestamp of a log |
| tag_families | [banyandb.model.v1.TagFamilyForWrite](#banyandb-model-v1-TagFamilyForWrite) | repeated | the order of tag_families&#39; items match TagFamilySpec |






<a name="banyandb-stream-v1-InternalWriteRequest"></a>

### InternalWriteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| shard_id | [uint32](#uint32) |  |  |
| entity_values | [banyandb.model.v1.TagValue](#banyandb-model-v1-TagValue) | repeated |  |
| request | [WriteRequest](#banyandb-stream-v1-WriteRequest) |  |  |






<a name="banyandb-stream-v1-TagFamilySpec"></a>

### TagFamilySpec
TagFamilySpec defines the specification of a tag family.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | name of the tag family |
| tag_names | [string](#string) | repeated | names of tags in the tag family |






<a name="banyandb-stream-v1-WriteRequest"></a>

### WriteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | the metadata is required only for the first request of gRPC stream. |
| element | [ElementValue](#banyandb-stream-v1-ElementValue) |  | the element is required. |
| message_id | [uint64](#uint64) |  | the message_id is required. |
| tag_family_spec | [TagFamilySpec](#banyandb-stream-v1-TagFamilySpec) | repeated | the tag family specification. If this is not set with the indicated metadata, use the schema definition. If this is not set, use the existing spec declaration from previous requests in the current gRPC stream. |






<a name="banyandb-stream-v1-WriteResponse"></a>

### WriteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message_id | [uint64](#uint64) |  | the message_id from request. |
| status | [string](#string) |  | status indicates the request processing result |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | the metadata from request when request fails |





 

 

 

 



<a name="banyandb_stream_v1_rpc-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/stream/v1/rpc.proto



<a name="banyandb-stream-v1-DeleteExpiredSegmentsRequest"></a>

### DeleteExpiredSegmentsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |
| segment_suffixes | [string](#string) | repeated |  |






<a name="banyandb-stream-v1-DeleteExpiredSegmentsResponse"></a>

### DeleteExpiredSegmentsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [int64](#int64) |  |  |





 

 

 


<a name="banyandb-stream-v1-StreamService"></a>

### StreamService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Query | [QueryRequest](#banyandb-stream-v1-QueryRequest) | [QueryResponse](#banyandb-stream-v1-QueryResponse) |  |
| Write | [WriteRequest](#banyandb-stream-v1-WriteRequest) stream | [WriteResponse](#banyandb-stream-v1-WriteResponse) stream |  |
| DeleteExpiredSegments | [DeleteExpiredSegmentsRequest](#banyandb-stream-v1-DeleteExpiredSegmentsRequest) | [DeleteExpiredSegmentsResponse](#banyandb-stream-v1-DeleteExpiredSegmentsResponse) |  |

 



<a name="banyandb_trace_v1_write-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/trace/v1/write.proto



<a name="banyandb-trace-v1-InternalWriteRequest"></a>

### InternalWriteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| shard_id | [uint32](#uint32) |  |  |
| request | [WriteRequest](#banyandb-trace-v1-WriteRequest) |  |  |






<a name="banyandb-trace-v1-TagSpec"></a>

### TagSpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tag_names | [string](#string) | repeated |  |






<a name="banyandb-trace-v1-WriteRequest"></a>

### WriteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |
| tags | [banyandb.model.v1.TagValue](#banyandb-model-v1-TagValue) | repeated |  |
| span | [bytes](#bytes) |  |  |
| version | [uint64](#uint64) |  |  |
| tag_spec | [TagSpec](#banyandb-trace-v1-TagSpec) |  |  |






<a name="banyandb-trace-v1-WriteResponse"></a>

### WriteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |
| version | [uint64](#uint64) |  |  |
| status | [string](#string) |  |  |





 

 

 

 



<a name="banyandb_trace_v1_rpc-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/trace/v1/rpc.proto



<a name="banyandb-trace-v1-DeleteExpiredSegmentsRequest"></a>

### DeleteExpiredSegmentsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |
| segment_suffixes | [string](#string) | repeated |  |






<a name="banyandb-trace-v1-DeleteExpiredSegmentsResponse"></a>

### DeleteExpiredSegmentsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [int64](#int64) |  |  |





 

 

 


<a name="banyandb-trace-v1-TraceService"></a>

### TraceService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Query | [QueryRequest](#banyandb-trace-v1-QueryRequest) | [QueryResponse](#banyandb-trace-v1-QueryResponse) |  |
| Write | [WriteRequest](#banyandb-trace-v1-WriteRequest) stream | [WriteResponse](#banyandb-trace-v1-WriteResponse) stream |  |
| DeleteExpiredSegments | [DeleteExpiredSegmentsRequest](#banyandb-trace-v1-DeleteExpiredSegmentsRequest) | [DeleteExpiredSegmentsResponse](#banyandb-trace-v1-DeleteExpiredSegmentsResponse) |  |

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

