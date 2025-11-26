/**
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Apache Software Foundation (ASF) licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

interface QueryRequest {
  query: string;
}

interface TagValue {
  str?: { value: string };
  int?: { value: number };
  float?: { value: number };
  binaryData?: unknown;
}

interface Tag {
  key: string;
  value: TagValue;
}

interface TagFamily {
  tags?: Tag[];
}

interface FieldValue {
  int?: { value: number };
  float?: { value: number };
  str?: { value: string };
  binaryData?: unknown;
}

interface Field {
  name: string;
  value: FieldValue;
}

interface DataPoint {
  timestamp?: string | number;
  sid?: string;
  version?: string | number;
  tagFamilies?: TagFamily[];
  fields?: Field[];
}

interface StreamResult {
  elements?: unknown[];
}

interface MeasureResult {
  dataPoints?: DataPoint[];
  data_points?: DataPoint[];
}

interface TraceResult {
  elements?: unknown[];
}

interface PropertyResult {
  items?: unknown[];
}

interface TopNResult {
  lists?: unknown[];
}

interface QueryResponse {
  // Response can be either wrapped in result or direct
  result?: {
    streamResult?: StreamResult;
    measureResult?: MeasureResult;
    traceResult?: TraceResult;
    propertyResult?: PropertyResult;
    topnResult?: TopNResult;
  };
  // Or directly at top level
  streamResult?: StreamResult;
  measureResult?: MeasureResult;
  traceResult?: TraceResult;
  propertyResult?: PropertyResult;
  topnResult?: TopNResult;
}

interface Group {
  metadata?: {
    name?: string;
  };
}

export interface ResourceMetadata {
  metadata?: {
    name?: string;
    group?: string;
  };
}

/**
 * BanyanDBClient wraps the BanyanDB HTTP client for executing queries.
 * Uses the HTTP API (grpc-gateway) instead of gRPC to avoid proto dependency issues.
 */
export class BanyanDBClient {
  private baseUrl: string;

  constructor(address: string) {
    // Convert gRPC address to HTTP address
    // Default HTTP port is 17913, gRPC is 17900
    if (address.includes(':')) {
      const [host, port] = address.split(':');
      // If it's the gRPC port, convert to HTTP port
      if (port === '17900') {
        this.baseUrl = `http://${host}:17913/api`;
      } else {
        this.baseUrl = `http://${host}:${port}/api`;
      }
    } else {
      // Default to HTTP port
      this.baseUrl = `http://${address}:17913/api`;
    }
  }

  /**
   * Execute a BydbQL query and return the result as a formatted string.
   */
  async query(bydbqlQuery: string, timeoutMs: number = 30000): Promise<string> {
    const request: QueryRequest = {
      query: bydbqlQuery,
    };

    const url = `${this.baseUrl}/v1/bydbql/query`;

    const queryDebugInfo = `Query: "${bydbqlQuery}"\nURL: ${url}`;

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(request),
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      // Read response text once
      const responseText = await response.text();

      if (!response.ok) {
        throw new Error(
          `Query execution failed: ${response.status} ${response.statusText}\n\n${queryDebugInfo}\n\nResponse: ${responseText}`,
        );
      }

      // Check if response has content
      if (!responseText || responseText.trim().length === 0) {
        throw new Error(
          `Empty response body from BanyanDB\n\n${queryDebugInfo}\n\nHTTP Status: ${response.status} ${response.statusText}`,
        );
      }

      let data: QueryResponse;
      try {
        data = JSON.parse(responseText) as QueryResponse;
      } catch (parseError) {
        throw new Error(
          `Invalid JSON response from BanyanDB: ${parseError instanceof Error ? parseError.message : String(parseError)}\n\n${queryDebugInfo}\n\nResponse text: ${responseText.substring(0, 500)}`,
        );
      }

      const responseDebugInfo = `Raw response: ${JSON.stringify(data, null, 2)}`;

      if (!data) {
        throw new Error(
          `Empty response from BanyanDB: response body is null or undefined\n\n${queryDebugInfo}\n\n${responseDebugInfo}`,
        );
      }

      // Check for result types both at top level and inside result wrapper
      const hasResultType =
        data.streamResult ||
        data.measureResult ||
        data.traceResult ||
        data.propertyResult ||
        data.topnResult ||
        (data.result &&
          (data.result.streamResult ||
            data.result.measureResult ||
            data.result.traceResult ||
            data.result.propertyResult ||
            data.result.topnResult));

      if (!hasResultType) {
        // This is a valid response but with no result type - likely an empty result set
        const debugMsg = `Response has no result type fields\n\n${queryDebugInfo}\n\nFull response: ${JSON.stringify(data, null, 2)}`;
        return (
          `Query executed successfully but returned no results.\n\n${debugMsg}\n\nPossible reasons:\n` +
          '- The query matched no data (empty result set)\n' +
          '- The resource name or group name might be incorrect\n' +
          '- The time range might not contain any data\n\n' +
          'Try:\n' +
          '1. Use list_groups_schemas to verify the resource exists\n' +
          '2. Check the time range in your query\n' +
          '3. Verify the resource name and group name are correct'
        );
      }

      // Format the response based on the result type
      return this.formatResponse(data);
    } catch (error) {
      if (error instanceof Error) {
        // Check if it's a timeout error
        if (error.name === 'AbortError' || error.message.includes('aborted')) {
          throw new Error(
            `Query timeout after ${timeoutMs}ms. ` +
              `BanyanDB may be slow or unresponsive. ` +
              `Check that BanyanDB is running and accessible at ${this.baseUrl}\n\n${queryDebugInfo}`,
          );
        }
        // Check if it's a network/connection error
        if (
          error.message.includes('fetch failed') ||
          error.message.includes('ECONNREFUSED') ||
          error.message.includes('Failed to fetch') ||
          error.name === 'TypeError'
        ) {
          throw new Error(
            `Failed to connect to BanyanDB at ${this.baseUrl}. ` +
              `Please ensure BanyanDB is running and accessible. ` +
              `Check that the HTTP API is enabled on port 17913 (or the port you specified). ` +
              `You can verify by running: curl http://localhost:17913/api/v1/bydbql/query\n\n${queryDebugInfo}`,
          );
        }
        // If error already contains queryDebugInfo, don't duplicate it
        if (!error.message.includes(queryDebugInfo)) {
          throw new Error(`${error.message}\n\n${queryDebugInfo}`);
        }
        throw error;
      }
      throw new Error(`Query execution failed: ${String(error)}\n\n${queryDebugInfo}`);
    }
  }

  /**
   * Format the query response into a readable string.
   */
  private formatResponse(response: QueryResponse): string {
    // Handle both wrapped and direct response structures
    const streamResult = response.streamResult || response.result?.streamResult;
    const measureResult = response.measureResult || response.result?.measureResult;
    const traceResult = response.traceResult || response.result?.traceResult;
    const propertyResult = response.propertyResult || response.result?.propertyResult;
    const topnResult = response.topnResult || response.result?.topnResult;

    if (streamResult) {
      // Check if it's an empty result set
      if (streamResult.elements && Array.isArray(streamResult.elements) && streamResult.elements.length === 0) {
        return 'Stream Query Result: No data found (empty result set)';
      }
      return `Stream Query Result:\n${JSON.stringify(streamResult, null, 2)}`;
    }

    if (measureResult) {
      // Check if it's an empty result set
      if (
        measureResult.dataPoints &&
        Array.isArray(measureResult.dataPoints) &&
        measureResult.dataPoints.length === 0
      ) {
        return (
          'Measure Query Result: No data found (empty result set)\n\n' +
          'Possible reasons:\n' +
          '- No data exists for the specified time range\n' +
          '- The measure name or group name might be incorrect\n' +
          '- The time range might not contain any data points\n\n' +
          'Suggestions:\n' +
          '1. Use list_groups_schemas to verify the measure exists:\n' +
          '   - List groups: list_groups_schemas with resource_type="groups"\n' +
          '   - List measures: list_groups_schemas with resource_type="measures" and group="<group_name>"\n' +
          "2. Try expanding the time range (e.g., TIME >= '-24h' for last 24 hours)\n" +
          '3. Verify data was written to BanyanDB for this measure'
        );
      }
      // Also check for snake_case for backward compatibility
      if (
        measureResult.data_points &&
        Array.isArray(measureResult.data_points) &&
        measureResult.data_points.length === 0
      ) {
        return (
          'Measure Query Result: No data found (empty result set)\n\n' +
          'Possible reasons:\n' +
          '- No data exists for the specified time range\n' +
          '- The measure name or group name might be incorrect\n' +
          '- The time range might not contain any data points\n\n' +
          'Suggestions:\n' +
          '1. Use list_groups_schemas to verify the measure exists:\n' +
          '   - List groups: list_groups_schemas with resource_type="groups"\n' +
          '   - List measures: list_groups_schemas with resource_type="measures" and group="<group_name>"\n' +
          "2. Try expanding the time range (e.g., TIME >= '-24h' for last 24 hours)\n" +
          '3. Verify data was written to BanyanDB for this measure'
        );
      }
      return `Measure Query Result:\n${JSON.stringify(measureResult.dataPoints, null, 2)}`;
    }

    if (traceResult) {
      // Check if it's an empty result set
      if (traceResult.elements && Array.isArray(traceResult.elements) && traceResult.elements.length === 0) {
        return 'Trace Query Result: No data found (empty result set)';
      }
      return `Trace Query Result:\n${JSON.stringify(traceResult, null, 2)}`;
    }

    if (propertyResult) {
      // Check if it's an empty result set
      if (propertyResult.items && Array.isArray(propertyResult.items) && propertyResult.items.length === 0) {
        return 'Property Query Result: No data found (empty result set)';
      }
      return `Property Query Result:\n${JSON.stringify(propertyResult, null, 2)}`;
    }

    if (topnResult) {
      // Check if it's an empty result set
      if (topnResult.lists && Array.isArray(topnResult.lists) && topnResult.lists.length === 0) {
        return 'TopN Query Result: No data found (empty result set)';
      }
      return `TopN Query Result:\n${JSON.stringify(topnResult, null, 2)}`;
    }

    return 'Unknown result type';
  }
  /**
   * List all groups.
   */
  async listGroups(timeoutMs: number = 30000): Promise<Group[]> {
    const url = `${this.baseUrl}/v1/group/schema/lists`;

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to list groups: ${response.status} ${response.statusText} - ${errorText}`);
      }

      const data = (await response.json()) as { group?: Group[] };
      return data.group || [];
    } catch (error) {
      if (error instanceof Error) {
        if (error.name === 'AbortError' || error.message.includes('aborted')) {
          throw new Error(
            `List groups timeout after ${timeoutMs}ms. ` +
              `BanyanDB may be slow or unresponsive. ` +
              `Check that BanyanDB is running and accessible at ${this.baseUrl}`,
          );
        }
        if (
          error.message.includes('fetch failed') ||
          error.message.includes('ECONNREFUSED') ||
          error.message.includes('Failed to fetch') ||
          error.name === 'TypeError'
        ) {
          throw new Error(
            `Failed to connect to BanyanDB at ${this.baseUrl}. ` + `Please ensure BanyanDB is running and accessible.`,
          );
        }
        throw error;
      }
      throw new Error(`Failed to list groups: ${String(error)}`);
    }
  }

  /**
   * List streams in a group.
   */
  async listStreams(group: string, timeoutMs: number = 30000): Promise<ResourceMetadata[]> {
    const url = `${this.baseUrl}/v1/stream/schema/lists/${encodeURIComponent(group)}`;

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to list streams: ${response.status} ${response.statusText} - ${errorText}`);
      }

      const data = (await response.json()) as { stream?: ResourceMetadata[] };
      return data.stream || [];
    } catch (error) {
      if (error instanceof Error) {
        throw error;
      }
      throw new Error(`Failed to list streams: ${String(error)}`);
    }
  }

  /**
   * List measures in a group.
   */
  async listMeasures(group: string, timeoutMs: number = 30000): Promise<ResourceMetadata[]> {
    const url = `${this.baseUrl}/v1/measure/schema/lists/${encodeURIComponent(group)}`;

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to list measures: ${response.status} ${response.statusText} - ${errorText}`);
      }

      const data = (await response.json()) as { measure?: ResourceMetadata[] };
      return data.measure || [];
    } catch (error) {
      if (error instanceof Error) {
        throw error;
      }
      throw new Error(`Failed to list measures: ${String(error)}`);
    }
  }

  /**
   * List traces in a group.
   */
  async listTraces(group: string, timeoutMs: number = 30000): Promise<ResourceMetadata[]> {
    const url = `${this.baseUrl}/v1/trace/schema/lists/${encodeURIComponent(group)}`;

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to list traces: ${response.status} ${response.statusText} - ${errorText}`);
      }

      const data = (await response.json()) as { trace?: ResourceMetadata[] };
      return data.trace || [];
    } catch (error) {
      if (error instanceof Error) {
        throw error;
      }
      throw new Error(`Failed to list traces: ${String(error)}`);
    }
  }

  /**
   * List properties in a group.
   */
  async listProperties(group: string, timeoutMs: number = 30000): Promise<ResourceMetadata[]> {
    const url = `${this.baseUrl}/v1/property/schema/lists/${encodeURIComponent(group)}`;

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to list properties: ${response.status} ${response.statusText} - ${errorText}`);
      }

      const data = (await response.json()) as {
        properties?: ResourceMetadata[];
      };
      return data.properties || [];
    } catch (error) {
      if (error instanceof Error) {
        throw error;
      }
      throw new Error(`Failed to list properties: ${String(error)}`);
    }
  }

  /**
   * Create a group using JSON format.
   */
  async createGroup(groupJson: string): Promise<string> {
    const url = `${this.baseUrl}/v1/group/schema`;

    try {
      // Parse JSON string to object
      const group = JSON.parse(groupJson);

      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ group }),
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to create group: ${response.status} ${response.statusText} - ${errorText}`);
      }

      return `Group "${group.metadata?.name || 'unknown'}" created successfully`;
    } catch (error) {
      if (error instanceof Error) {
        if (error.message.includes('JSON')) {
          throw new Error(`Invalid JSON format: ${error.message}`);
        }
        throw error;
      }
      throw new Error(`Failed to create group: ${String(error)}`);
    }
  }
}
