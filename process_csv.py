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

# Calculate total changes across all files
total_changes = sorted_data['total_change'].sum()
target_change_per_batch = total_changes / 20  # 20 developers

# Create batches with approximately equal total change
batches = []
current_batch = []
current_batch_change = 0
batch_number = 1

for _, row in sorted_data.iterrows():
    # If adding this file would exceed the target and we already have files in the batch
    if current_batch_change + row['total_change'] > target_change_per_batch * 1.1 and current_batch:
        batches.append({
            'batch_number': batch_number,
            'files': current_batch,
            'total_change': current_batch_change,
            'file_count': len(current_batch)
        })
        batch_number += 1
        current_batch = []
        current_batch_change = 0
    
    current_batch.append({
        'file_path': row['clean_file_path'],
        'original_path': row['file_path'],
        'total_change': row['total_change']
    })
    current_batch_change += row['total_change']

# Add the last batch if it's not empty
if current_batch:
    batches.append({
        'batch_number': batch_number,
        'files': current_batch,
        'total_change': current_batch_change,
        'file_count': len(current_batch)
    })

# Adjust batches to get closer to 20 batches
# If we have fewer than 20 batches, split the largest ones
while len(batches) < 20:
    # Find the batch with the most files
    largest_batch_idx = max(range(len(batches)), key=lambda i: batches[i]['file_count'])
    largest_batch = batches[largest_batch_idx]
    
    if largest_batch['file_count'] <= 1:
        break  # Can't split batches with only one file
    
    # Split the batch into two
    mid_point = largest_batch['file_count'] // 2
    
    batch1_files = largest_batch['files'][:mid_point]
    batch2_files = largest_batch['files'][mid_point:]
    
    batch1_change = sum(f['total_change'] for f in batch1_files)
    batch2_change = sum(f['total_change'] for f in batch2_files)
    
    # Replace the original batch with the first half
    batches[largest_batch_idx] = {
        'batch_number': largest_batch['batch_number'],
        'files': batch1_files,
        'total_change': batch1_change,
        'file_count': len(batch1_files)
    }
    
    # Add the second half as a new batch
    batches.append({
        'batch_number': len(batches) + 1,
        'files': batch2_files,
        'total_change': batch2_change,
        'file_count': len(batch2_files)
    })

# If we have more than 20 batches, merge the smallest ones
while len(batches) > 20:
    # Find the two batches with the smallest total change
    sorted_by_change = sorted(range(len(batches)), key=lambda i: batches[i]['total_change'])
    smallest_idx = sorted_by_change[0]
    second_smallest_idx = sorted_by_change[1]
    
    # Merge the two smallest batches
    merged_files = batches[smallest_idx]['files'] + batches[second_smallest_idx]['files']
    merged_change = batches[smallest_idx]['total_change'] + batches[second_smallest_idx]['total_change']
    
    # Create the merged batch
    merged_batch = {
        'batch_number': min(batches[smallest_idx]['batch_number'], batches[second_smallest_idx]['batch_number']),
        'files': merged_files,
        'total_change': merged_change,
        'file_count': len(merged_files)
    }
    
    # Remove the two batches and add the merged one
    if smallest_idx > second_smallest_idx:
        batches.pop(smallest_idx)
        batches.pop(second_smallest_idx)
    else:
        batches.pop(second_smallest_idx)
        batches.pop(smallest_idx)
    
    batches.append(merged_batch)

# Renumber batches from 1 to 20
batches = sorted(batches, key=lambda b: b['total_change'], reverse=True)
for i, batch in enumerate(batches):
    batch['batch_number'] = i + 1

# Save the results
output_data = []
for batch in batches:
    for file_info in batch['files']:
        output_data.append({
            'batch_number': batch['batch_number'],
            'file_path': file_info['file_path'],
            'original_path': file_info['original_path'],
            'total_change': file_info['total_change']
        })

output_df = pd.DataFrame(output_data)
output_df.to_csv('developer_batches.csv', index=False)

# Create a summary file
summary_data = []
for batch in batches:
    summary_data.append({
        'batch_number': batch['batch_number'],
        'file_count': batch['file_count'],
        'total_change': batch['total_change']
    })

summary_df = pd.DataFrame(summary_data)
summary_df = summary_df.sort_values('batch_number')
summary_df.to_csv('batch_summary.csv', index=False)

# Print summary
print(f"Total files: {len(output_data)}")
print(f"Total changes: {total_changes}")
print(f"Target change per batch: {target_change_per_batch}")
print("\nBatch Summary:")
for batch in sorted(batches, key=lambda b: b['batch_number']):
    print(f"Batch {batch['batch_number']}: {batch['file_count']} files, {batch['total_change']} changes")
