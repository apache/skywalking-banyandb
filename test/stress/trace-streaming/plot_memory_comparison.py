#!/usr/bin/env python3
# Licensed to Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Apache Software Foundation (ASF) licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Plot memory monitoring data comparing write and query phases.

Usage:
    python3 plot_memory_comparison.py memory_write_small.csv memory_query_small.csv
    python3 plot_memory_comparison.py memory_write_small.csv memory_query_small.csv --output-dir ./charts
"""

import sys
import csv
import os
from datetime import datetime
import argparse

try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
except ImportError:
    print("Error: matplotlib not installed")
    print("Install with: pip3 install matplotlib")
    sys.exit(1)


def parse_csv(filename):
    """Parse memory CSV file."""
    timestamps = []
    heap_alloc = []
    heap_sys = []
    rss = []
    num_gc = []
    num_goroutine = []
    
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Parse timestamp
            ts = datetime.fromisoformat(row['Timestamp'].replace('Z', '+00:00'))
            timestamps.append(ts)
            
            # Parse memory values (already in MB)
            heap_alloc.append(float(row['HeapAlloc(MB)']))
            heap_sys.append(float(row['HeapSys(MB)']))
            rss.append(float(row['RSS(MB)']))
            num_gc.append(int(row['NumGC']))
            num_goroutine.append(int(row['NumGoroutine']))
    
    return {
        'timestamps': timestamps,
        'heap_alloc': heap_alloc,
        'heap_sys': heap_sys,
        'rss': rss,
        'num_gc': num_gc,
        'num_goroutine': num_goroutine
    }


def plot_single_phase(data, title, output_file=None):
    """Create memory visualization for a single phase."""
    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)
    
    # Memory usage over time
    ax1 = axes[0]
    ax1.plot(data['timestamps'], data['heap_alloc'], label='Heap Alloc', linewidth=2, color='#2E86AB')
    ax1.plot(data['timestamps'], data['heap_sys'], label='Heap Sys', linewidth=2, alpha=0.7, color='#A23B72')
    ax1.plot(data['timestamps'], data['rss'], label='RSS', linewidth=2, alpha=0.7, color='#F18F01')
    ax1.set_ylabel('Memory (MB)', fontsize=12)
    ax1.set_title(f'{title} - Memory Usage Over Time', fontsize=14, fontweight='bold')
    ax1.legend(loc='upper left', fontsize=10)
    ax1.grid(True, alpha=0.3)
    
    # Add peak/average annotations
    peak_heap = max(data['heap_alloc'])
    avg_heap = sum(data['heap_alloc']) / len(data['heap_alloc'])
    ax1.axhline(y=peak_heap, color='red', linestyle='--', alpha=0.5, label=f'Peak: {peak_heap:.1f} MB')
    ax1.axhline(y=avg_heap, color='green', linestyle='--', alpha=0.5, label=f'Avg: {avg_heap:.1f} MB')
    ax1.legend(loc='upper left', fontsize=10)
    
    # Garbage collection over time
    ax2 = axes[1]
    ax2.plot(data['timestamps'], data['num_gc'], label='GC Cycles', color='orange', linewidth=2)
    ax2.set_ylabel('GC Cycles', fontsize=12)
    ax2.set_title('Garbage Collection Activity', fontsize=12, fontweight='bold')
    ax2.legend(loc='upper left', fontsize=10)
    ax2.grid(True, alpha=0.3)
    
    # Goroutine count over time
    ax3 = axes[2]
    ax3.plot(data['timestamps'], data['num_goroutine'], label='Goroutines', color='purple', linewidth=2)
    ax3.set_ylabel('Goroutine Count', fontsize=12)
    ax3.set_xlabel('Time', fontsize=12)
    ax3.set_title('Goroutine Count', fontsize=12, fontweight='bold')
    ax3.legend(loc='upper left', fontsize=10)
    ax3.grid(True, alpha=0.3)
    
    # Format x-axis
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        print(f"Chart saved to: {output_file}")
    
    plt.close()
    
    # Return stats
    return {
        'peak_heap': peak_heap,
        'avg_heap': avg_heap,
        'min_heap': min(data['heap_alloc']),
        'peak_rss': max(data['rss']),
        'total_gc': max(data['num_gc']),
        'max_goroutines': max(data['num_goroutine'])
    }


def plot_comparison(write_data, query_data, output_file=None):
    """Create comparison plot for write vs query phases."""
    fig, axes = plt.subplots(2, 1, figsize=(16, 10), sharex=False)
    
    # Combine timestamps for proper alignment
    # Normalize to relative time (seconds from start)
    def to_relative_seconds(timestamps):
        start = timestamps[0]
        return [(t - start).total_seconds() for t in timestamps]
    
    write_times = to_relative_seconds(write_data['timestamps'])
    query_times = to_relative_seconds(query_data['timestamps'])
    
    # Memory comparison
    ax1 = axes[0]
    ax1.plot(write_times, write_data['heap_alloc'], label='Write - Heap Alloc', 
             linewidth=2, color='#2E86AB')
    ax1.plot(query_times, query_data['heap_alloc'], label='Query - Heap Alloc', 
             linewidth=2, color='#F18F01')
    ax1.set_ylabel('Heap Allocated (MB)', fontsize=12)
    ax1.set_title('Memory Usage Comparison: Write vs Query Phase', fontsize=14, fontweight='bold')
    ax1.legend(loc='upper left', fontsize=11)
    ax1.grid(True, alpha=0.3)
    
    # Add annotations
    write_peak = max(write_data['heap_alloc'])
    query_peak = max(query_data['heap_alloc'])
    ax1.axhline(y=write_peak, color='#2E86AB', linestyle='--', alpha=0.4)
    ax1.axhline(y=query_peak, color='#F18F01', linestyle='--', alpha=0.4)
    ax1.text(max(write_times) * 0.95, write_peak, f'{write_peak:.1f} MB', 
             va='bottom', ha='right', color='#2E86AB', fontweight='bold')
    ax1.text(max(query_times) * 0.95, query_peak, f'{query_peak:.1f} MB', 
             va='bottom', ha='right', color='#F18F01', fontweight='bold')
    
    # Goroutine comparison
    ax2 = axes[1]
    ax2.plot(write_times, write_data['num_goroutine'], label='Write - Goroutines', 
             linewidth=2, color='#2E86AB')
    ax2.plot(query_times, query_data['num_goroutine'], label='Query - Goroutines', 
             linewidth=2, color='#F18F01')
    ax2.set_ylabel('Goroutine Count', fontsize=12)
    ax2.set_xlabel('Time (seconds)', fontsize=12)
    ax2.set_title('Goroutine Count Comparison', fontsize=12, fontweight='bold')
    ax2.legend(loc='upper left', fontsize=11)
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        print(f"Comparison chart saved to: {output_file}")
    
    plt.close()


def print_analysis(phase_name, data, stats):
    """Print analysis for a phase."""
    print(f"\n=== {phase_name} Phase Analysis ===")
    print(f"Peak Heap Alloc: {stats['peak_heap']:.2f} MB")
    print(f"Avg Heap Alloc:  {stats['avg_heap']:.2f} MB")
    print(f"Min Heap Alloc:  {stats['min_heap']:.2f} MB")
    print(f"Peak RSS:        {stats['peak_rss']:.2f} MB")
    print(f"Total GC Cycles: {stats['total_gc']}")
    print(f"Max Goroutines:  {stats['max_goroutines']}")
    
    # Memory leak detection
    if len(data['heap_alloc']) > 4:
        first_quarter_avg = sum(data['heap_alloc'][:len(data['heap_alloc'])//4]) / (len(data['heap_alloc'])//4)
        last_quarter_avg = sum(data['heap_alloc'][-len(data['heap_alloc'])//4:]) / (len(data['heap_alloc'])//4)
        
        print(f"\nLeak Detection:")
        print(f"  First 25% avg:   {first_quarter_avg:.2f} MB")
        print(f"  Last 25% avg:    {last_quarter_avg:.2f} MB")
        
        if last_quarter_avg > first_quarter_avg * 1.5:
            print("  ⚠️  WARNING: Possible memory leak detected (50% increase)")
        elif last_quarter_avg > first_quarter_avg * 1.2:
            print("  ⚠️  CAUTION: Memory trending upward (20% increase)")
        else:
            print("  ✅ Memory appears stable (no leak detected)")


def print_comparison_analysis(write_stats, query_stats):
    """Print comparison analysis."""
    print("\n" + "="*60)
    print("=== WRITE vs QUERY COMPARISON ===")
    print("="*60)
    
    print("\nPeak Heap Memory:")
    print(f"  Write: {write_stats['peak_heap']:.2f} MB")
    print(f"  Query: {query_stats['peak_heap']:.2f} MB")
    if query_stats['peak_heap'] > write_stats['peak_heap']:
        ratio = query_stats['peak_heap'] / write_stats['peak_heap']
        print(f"  → Query uses {ratio:.1f}x more peak heap than write")
    else:
        ratio = write_stats['peak_heap'] / query_stats['peak_heap']
        print(f"  → Write uses {ratio:.1f}x more peak heap than query")
    
    print("\nAverage Heap Memory:")
    print(f"  Write: {write_stats['avg_heap']:.2f} MB")
    print(f"  Query: {query_stats['avg_heap']:.2f} MB")
    
    print("\nPeak RSS:")
    print(f"  Write: {write_stats['peak_rss']:.2f} MB")
    print(f"  Query: {query_stats['peak_rss']:.2f} MB")
    
    print("\nMax Goroutines:")
    print(f"  Write: {write_stats['max_goroutines']}")
    print(f"  Query: {query_stats['max_goroutines']}")
    
    print("\nGC Activity:")
    print(f"  Write: {write_stats['total_gc']} cycles")
    print(f"  Query: {query_stats['total_gc']} cycles")
    
    print("\n" + "="*60)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Plot memory comparison between write and query phases')
    parser.add_argument('write_csv', help='Path to write phase memory CSV file')
    parser.add_argument('query_csv', help='Path to query phase memory CSV file')
    parser.add_argument('--output-dir', '-o', default='.', help='Output directory for charts')
    parser.add_argument('--prefix', '-p', default='memory', help='Prefix for output files')
    
    args = parser.parse_args()
    
    try:
        # Create output directory if needed
        os.makedirs(args.output_dir, exist_ok=True)
        
        # Parse data
        print("Loading write phase data...")
        write_data = parse_csv(args.write_csv)
        
        print("Loading query phase data...")
        query_data = parse_csv(args.query_csv)
        
        # Generate individual plots
        print("\nGenerating write phase plot...")
        write_output = os.path.join(args.output_dir, f'{args.prefix}_write.png')
        write_stats = plot_single_phase(write_data, 'Write Phase', write_output)
        
        print("Generating query phase plot...")
        query_output = os.path.join(args.output_dir, f'{args.prefix}_query.png')
        query_stats = plot_single_phase(query_data, 'Query Phase', query_output)
        
        # Generate comparison plot
        print("Generating comparison plot...")
        comparison_output = os.path.join(args.output_dir, f'{args.prefix}_comparison.png')
        plot_comparison(write_data, query_data, comparison_output)
        
        # Print analysis
        print_analysis('Write', write_data, write_stats)
        print_analysis('Query', query_data, query_stats)
        print_comparison_analysis(write_stats, query_stats)
        
        print(f"\n✅ All charts generated in: {args.output_dir}/")
        print(f"   - {args.prefix}_write.png")
        print(f"   - {args.prefix}_query.png")
        print(f"   - {args.prefix}_comparison.png")
        
    except FileNotFoundError as e:
        print(f"Error: File not found: {e.filename}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
