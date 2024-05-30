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

import json
import matplotlib.pyplot as plt
import numpy as np
import sys

def generate_per_query_chart(baseline, comparison):
    results = []
    for query in range(1, 23):
        a = np.mean(np.array(baseline[str(query)]))
        b = np.mean(np.array(comparison[str(query)]))
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
            ax.text(bar.get_x() + bar.get_width() / 2.0, yval, f'{yval}%', va='bottom', ha='center', fontsize=8,
                    color='blue')
        else:
            ax.text(bar.get_x() + bar.get_width() / 2.0, yval, f'{yval}%', va='top', ha='center', fontsize=8,
                    color='blue')

    # Add title and labels
    ax.set_title('Comet Acceleration of TPC-H Queries')
    ax.set_ylabel('Speedup (100% speedup = 2x faster)')
    ax.set_xlabel('Query')

    # Customize the y-axis to handle both positive and negative values better
    ax.axhline(0, color='black', linewidth=0.8)
    ax.set_ylim(-400, 600)

    # Show grid for better readability
    ax.yaxis.grid(True)

    # Save the plot as an image file
    plt.savefig('comet_acceleration_tpch_queries.png', format='png')


def generate_summary(baseline, comparison):
    baseline_total = 0
    comparison_total = 0
    for query in range(1, 23):
        baseline_total += np.mean(np.array(baseline[str(query)]))
        comparison_total += np.mean(np.array(comparison[str(query)]))

    # TODO make labels configurable
    labels = ['Spark', 'Spark + Comet']
    times = [round(baseline_total,0), round(comparison_total,0)]

    # Create figure and axis
    fig, ax = plt.subplots()

    # Create bar chart
    bars = ax.bar(labels, times, color='skyblue')

    # Add text annotations
    for bar in bars:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2.0, yval, f'{yval}', va='bottom')  # va: vertical alignment

    # Add title and labels
    #TODO make title configurable
    ax.set_title('TPC-H Performance (scale factor 100)')
    ax.set_ylabel('Time in seconds to run all 22 TPC-H queries (lower is better)')

    plt.savefig('tpch_performance.png', format='png')

def main(filename1: str, filename2: str):
    with open(filename1) as f1:
        baseline = json.load(f1)
    with open(filename2) as f2:
        comparison = json.load(f2)
    generate_summary(baseline, comparison)
    generate_per_query_chart(baseline, comparison)

if __name__ == '__main__':
    # TODO argparse
    main(sys.argv[1], sys.argv[2])
