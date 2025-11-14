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

interface QueryResponse {
  result?: {
    stream_result?: any;
    measure_result?: any;
    trace_result?: any;
    property_result?: any;
    topn_result?: any;
  };
}

interface Group {
  metadata?: {
    name?: string;
  };
}

interface ResourceMetadata {
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
    if (address.includes(":")) {
      const [host, port] = address.split(":");
      // If it's the gRPC port, convert to HTTP port
      if (port === "17900") {
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
    
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

      const response = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(request),
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(
          `Query execution failed: ${response.status} ${response.statusText} - ${errorText}`
        );
      }

      const data = (await response.json()) as QueryResponse;

      if (!data || !data.result) {
        throw new Error("Empty response from BanyanDB");
      }

      // Format the response based on the result type
      return this.formatResponse(data);
    } catch (error) {
      if (error instanceof Error) {
        // Check if it's a timeout error
        if (error.name === "AbortError" || error.message.includes("aborted")) {
          throw new Error(
            `Query timeout after ${timeoutMs}ms. ` +
            `BanyanDB may be slow or unresponsive. ` +
            `Check that BanyanDB is running and accessible at ${this.baseUrl}`
          );
        }
        // Check if it's a network/connection error
        if (error.message.includes("fetch failed") || 
            error.message.includes("ECONNREFUSED") ||
            error.message.includes("Failed to fetch") ||
            error.name === "TypeError") {
          throw new Error(
            `Failed to connect to BanyanDB at ${this.baseUrl}. ` +
            `Please ensure BanyanDB is running and accessible. ` +
            `Check that the HTTP API is enabled on port 17913 (or the port you specified). ` +
            `You can verify by running: curl http://localhost:17913/api/v1/bydbql/query`
          );
        }
        throw error;
      }
      throw new Error(`Query execution failed: ${String(error)}`);
    }
  }

  /**
   * Format the query response into a readable string.
   */
  private formatResponse(response: QueryResponse): string {
    const result = response.result!;

    if (result.stream_result) {
      return `Stream Query Result:\n${JSON.stringify(result.stream_result, null, 2)}`;
    }
    if (result.measure_result) {
      return `Measure Query Result:\n${JSON.stringify(result.measure_result, null, 2)}`;
    }
    if (result.trace_result) {
      return `Trace Query Result:\n${JSON.stringify(result.trace_result, null, 2)}`;
    }
    if (result.property_result) {
      return `Property Query Result:\n${JSON.stringify(result.property_result, null, 2)}`;
    }
    if (result.topn_result) {
      return `TopN Query Result:\n${JSON.stringify(result.topn_result, null, 2)}`;
    }

    return "Unknown result type";
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
        method: "GET",
        headers: {
          "Content-Type": "application/json",
        },
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(
          `Failed to list groups: ${response.status} ${response.statusText} - ${errorText}`
        );
      }

      const data = (await response.json()) as { group?: Group[] };
      return data.group || [];
    } catch (error) {
      if (error instanceof Error) {
        if (error.name === "AbortError" || error.message.includes("aborted")) {
          throw new Error(
            `List groups timeout after ${timeoutMs}ms. ` +
            `BanyanDB may be slow or unresponsive. ` +
            `Check that BanyanDB is running and accessible at ${this.baseUrl}`
          );
        }
        if (error.message.includes("fetch failed") || 
            error.message.includes("ECONNREFUSED") ||
            error.message.includes("Failed to fetch") ||
            error.name === "TypeError") {
          throw new Error(
            `Failed to connect to BanyanDB at ${this.baseUrl}. ` +
            `Please ensure BanyanDB is running and accessible.`
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
        method: "GET",
        headers: {
          "Content-Type": "application/json",
        },
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(
          `Failed to list streams: ${response.status} ${response.statusText} - ${errorText}`
        );
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
        method: "GET",
        headers: {
          "Content-Type": "application/json",
        },
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(
          `Failed to list measures: ${response.status} ${response.statusText} - ${errorText}`
        );
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
        method: "GET",
        headers: {
          "Content-Type": "application/json",
        },
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(
          `Failed to list traces: ${response.status} ${response.statusText} - ${errorText}`
        );
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
        method: "GET",
        headers: {
          "Content-Type": "application/json",
        },
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(
          `Failed to list properties: ${response.status} ${response.statusText} - ${errorText}`
        );
      }

      const data = (await response.json()) as { properties?: ResourceMetadata[] };
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
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ group }),
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(
          `Failed to create group: ${response.status} ${response.statusText} - ${errorText}`
        );
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

