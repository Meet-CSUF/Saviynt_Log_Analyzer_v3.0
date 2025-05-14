#!/usr/bin/env python3
import pandas as pd
import os
import re
import numpy as np

# Load the CSV files
test_run1 = pd.read_csv('test_run1.csv')
test_run2 = pd.read_csv('test_run2.csv')

# Combine the data
combined_data = pd.concat([test_run1, test_run2], ignore_index=True)

# Clean file paths - remove everything before 'ecmv4'
def clean_path(path):
    match = re.search(r'ecmv4.*', path)
    if match:
        return match.group(0)
    return path

combined_data['clean_file_path'] = combined_data['file_path'].apply(clean_path)

# Calculate total changes for each file
def calculate_total_change(row):
    total_change = 0
    for col in ['error', 'warn', 'info', 'debug', 'trace', 'fatal']:
        before_col = f'before_{col}'
        after_col = f'after_{col}'
        total_change += abs(row[after_col] - row[before_col])
    return total_change

combined_data['total_change'] = combined_data.apply(calculate_total_change, axis=1)

# Remove duplicates if any (keeping the one with the highest total change)
combined_data = combined_data.sort_values('total_change', ascending=False)
combined_data = combined_data.drop_duplicates(subset=['clean_file_path'], keep='first')

# Sort by total change in descending order
sorted_data = combined_data.sort_values('total_change', ascending=False)

# Calculate total files and changes
total_files = len(sorted_data)
total_changes = sorted_data['total_change'].sum()
target_files_per_batch = total_files / 20  # Target number of files per batch
target_change_per_batch = total_changes / 20  # Target change per batch

print(f"Total files: {total_files}")
print(f"Total changes: {total_changes}")
print(f"Target files per batch: {target_files_per_batch}")
print(f"Target change per batch: {target_change_per_batch}")

# Create a more balanced distribution using a hybrid approach
# First, create initial batches based on file count
batches = [[] for _ in range(20)]
batch_changes = [0] * 20
batch_file_counts = [0] * 20

# Sort files by change amount (descending)
sorted_files = sorted_data.to_dict('records')
sorted_files.sort(key=lambda x: x['total_change'], reverse=True)

# First pass: distribute high-change files evenly
high_change_threshold = 10  # Files with significant changes
high_change_files = [f for f in sorted_files if f['total_change'] >= high_change_threshold]
other_files = [f for f in sorted_files if f['total_change'] < high_change_threshold]

# Distribute high-change files using a greedy approach
for file in high_change_files:
    # Find the batch with the lowest total change
    min_change_batch = min(range(20), key=lambda i: batch_changes[i])
    batches[min_change_batch].append(file)
    batch_changes[min_change_batch] += file['total_change']
    batch_file_counts[min_change_batch] += 1

# Second pass: distribute remaining files to balance file counts
# Sort remaining files by change (ascending) to distribute small changes first
other_files.sort(key=lambda x: x['total_change'])

# Calculate how many files each batch should ideally have
target_files_per_batch = total_files / 20

# Distribute remaining files
for file in other_files:
    # Find the batch with the fewest files
    min_files_batch = min(range(20), key=lambda i: batch_file_counts[i])
    batches[min_files_batch].append(file)
    batch_changes[min_files_batch] += file['total_change']
    batch_file_counts[min_files_batch] += 1

# Create batch summaries
batch_summaries = []
for i, batch in enumerate(batches):
    batch_summaries.append({
        'batch_number': i + 1,
        'file_count': len(batch),
        'total_change': sum(file['total_change'] for file in batch)
    })

# Create output dataframe
output_data = []
for i, batch in enumerate(batches):
    for file in batch:
        output_data.append({
            'batch_number': i + 1,
            'file_path': file['clean_file_path'],
            'original_path': file['file_path'],
            'total_change': file['total_change']
        })

output_df = pd.DataFrame(output_data)
output_df.to_csv('developer_batches_balanced.csv', index=False)

# Create a summary file
summary_df = pd.DataFrame(batch_summaries)
summary_df = summary_df.sort_values('batch_number')
summary_df.to_csv('batch_summary_balanced.csv', index=False)

# Print summary
print("\nBatch Summary:")
for batch in sorted(batch_summaries, key=lambda b: b['batch_number']):
    print(f"Batch {batch['batch_number']}: {batch['file_count']} files, {batch['total_change']} changes")

# Calculate statistics
file_counts = [batch['file_count'] for batch in batch_summaries]
change_counts = [batch['total_change'] for batch in batch_summaries]

print("\nStatistics:")
print(f"File count - Min: {min(file_counts)}, Max: {max(file_counts)}, Avg: {sum(file_counts)/len(file_counts):.1f}")
print(f"Change count - Min: {min(change_counts)}, Max: {max(change_counts)}, Avg: {sum(change_counts)/len(change_counts):.1f}")
print(f"File count standard deviation: {np.std(file_counts):.2f}")
print(f"Change count standard deviation: {np.std(change_counts):.2f}")
