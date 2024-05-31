# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import json
import matplotlib.pyplot as plt
import numpy as np
import sys

def geomean(data):
    return np.prod(data) ** (1 / len(data))

def generate_query_speedup_chart(baseline, comparison, label1: str, label2: str, benchmark: str):
    results = []
    for query in range(1, 23):
        a = np.median(np.array(baseline[str(query)]))
        b = np.median(np.array(comparison[str(query)]))
        if a > b:
            speedup = a/b-1
        else:
            speedup = -(1/(a/b)-1)
        results.append(("q" + str(query), round(speedup*100, 0)))

    results = sorted(results, key=lambda x: -x[1])

    queries, speedups = zip(*results)

    # Create figure and axis
    fig, ax = plt.subplots(figsize=(10, 6))

    # Create bar chart
    bars = ax.bar(queries, speedups, color='skyblue')

    # Add text annotations
    for bar, speedup in zip(bars, speedups):
        yval = bar.get_height()
        if yval >= 0:
            ax.text(bar.get_x() + bar.get_width() / 2.0, min(800, yval+5), f'{yval:.0f}%', va='bottom', ha='center', fontsize=8,
                    color='blue', rotation=90)
        else:
            ax.text(bar.get_x() + bar.get_width() / 2.0, yval, f'{yval:.0f}%', va='top', ha='center', fontsize=8,
                    color='blue', rotation=90)

    # Add title and labels
    ax.set_title(label2 + " speedup over " + label1 + " (" + benchmark + ")")
    ax.set_ylabel('Speedup (100% speedup = 2x faster)')
    ax.set_xlabel('Query')

    # Customize the y-axis to handle both positive and negative values better
    ax.axhline(0, color='black', linewidth=0.8)
    min_value = (min(speedups) // 100) * 100
    max_value = ((max(speedups) // 100) + 1) * 100 + 50
    ax.set_ylim(min_value, max_value)

    # Show grid for better readability
    ax.yaxis.grid(True)

    # Save the plot as an image file
    plt.savefig('tpch_queries_speedup.png', format='png')


def generate_query_comparison_chart(results, labels, benchmark: str):
    queries = []
    benches = []
    for _ in results:
        benches.append([])
    for query in range(1, 23):
        queries.append("q" + str(query))
        for i in range(0, len(results)):
            benches[i].append(np.median(np.array(results[i][str(query)])))

    # Define the width of the bars
    bar_width = 0.3

    # Define the positions of the bars on the x-axis
    index = np.arange(len(queries)) * 1.5

    # Create a bar chart
    fig, ax = plt.subplots(figsize=(15, 6))
    for i in range(0, len(results)):
        bar = ax.bar(index + i * bar_width, benches[i], bar_width, label=labels[i])

    # Add labels, title, and legend
    ax.set_title(benchmark)
    ax.set_xlabel('Queries')
    ax.set_ylabel('Query Time (seconds)')
    ax.set_xticks(index + bar_width / 2)
    ax.set_xticklabels(queries)
    ax.legend()

    # Save the plot as an image file
    plt.savefig('tpch_queries_compare.png', format='png')

def generate_summary(results, labels, benchmark: str):
    timings = []
    for _ in results:
        timings.append(0)

    for query in range(1, 23):
        for i in range(0, len(results)):
            timings[i] += np.median(np.array(results[i][str(query)]))

    # Create figure and axis
    fig, ax = plt.subplots()

    # Add title and labels
    ax.set_title(benchmark)
    ax.set_ylabel('Time in seconds to run all 22 TPC-H queries (lower is better)')

    times = [round(x,0) for x in timings]

    # Create bar chart
    bars = ax.bar(labels, times, color='skyblue')

    # Add text annotations
    for bar in bars:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2.0, yval, f'{yval}', va='bottom')  # va: vertical alignment

    plt.savefig('tpch_allqueries.png', format='png')

def main(files, labels, benchmark: str):
    results = []
    for filename in files:
        with open(filename) as f:
            results.append(json.load(f))
    generate_summary(results, labels, benchmark)
    generate_query_comparison_chart(results, labels, benchmark)
    if len(files) == 2:
        generate_query_speedup_chart(results[0], results[1], labels[0], labels[1], benchmark)

if __name__ == '__main__':
    argparse = argparse.ArgumentParser(description='Generate comparison')
    argparse.add_argument('filenames', nargs='+', type=str, help='JSON result files')
    argparse.add_argument('--labels', nargs='+', type=str, help='Labels')
    argparse.add_argument('--benchmark', type=str, help='Benchmark description')
    args = argparse.parse_args()
    main(args.filenames, args.labels, args.benchmark)
