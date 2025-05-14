#!/usr/bin/env python3
"""
Scalable Log Analysis Tool

This script provides scalable analysis of gzipped JSON log files using streaming processing
and out-of-core computations. It's designed to handle very large datasets efficiently.

Requirements:
    - Python 3.7+
    - pandas
    - dask
    - fastparquet
    - psutil
    - tqdm

Author: Amazon Q
Date: 2024
"""

import os
import gzip
import json
import sys
import psutil
import warnings
import traceback
import signal
from pathlib import Path
from datetime import datetime
import dateutil.parser
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import contextmanager

import pandas as pd
import dask.dataframe as dd
import numpy as np
from tqdm import tqdm

# Suppress specific warnings
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

class LogAnalysisError(Exception):
    """Custom exception for log analysis errors."""
    pass

@contextmanager
def suppress_semaphore_warning():
    """Context manager to suppress semaphore warnings."""
    warnings.filterwarnings(
        action="ignore",
        message="resource_tracker: There appear to be\\d+ leaked semaphore objects to clean up at shutdown",
        category=UserWarning,
    )
    try:
        yield
    finally:
        warnings.resetwarnings()

class LogAnalyzer:
    """Main class for scalable log analysis."""

    def __init__(self, base_folder, output_folder=None, max_memory_gb=None):
        """
        Initialize LogAnalyzer with configurable parameters.
        
        Args:
            base_folder (str): Path to log files
            output_folder (str): Path for output files (default: base_folder/analysis)
            max_memory_gb (float): Maximum memory usage in GB (default: 70% of system memory)
        """
        self.base_folder = Path(base_folder)
        self.output_folder = Path(output_folder) if output_folder else self.base_folder / 'analysis'
        self.output_folder.mkdir(parents=True, exist_ok=True)
        
        # Calculate memory limits
        total_memory = psutil.virtual_memory().total
        self.max_memory = max_memory_gb * 1024**3 if max_memory_gb else total_memory * 0.7
        self.chunk_size = self._calculate_chunk_size()
        
        # Initialize counters
        self.total_lines = 0
        self.error_lines = 0
        self.files_processed = 0
        
        # Create intermediate storage
        self.temp_dir = self.output_folder / 'temp'
        self.temp_dir.mkdir(exist_ok=True)
        
        # Analysis results storage
        self.analyses = {}

    def _calculate_chunk_size(self, sample_size=1000):
        """
        Dynamically calculate optimal chunk size based on sample data.
        
        Args:
            sample_size (int): Number of log lines to sample
            
        Returns:
            int: Calculated chunk size
        """
        try:
            # Sample a few log files to estimate memory usage
            sample_data = []
            for gz_file in self.base_folder.rglob('*.gz'):
                with gzip.open(gz_file, 'rt', encoding='utf-8') as f:
                    for _ in range(min(sample_size, 100)):
                        try:
                            line = next(f)
                            sample_data.append(json.loads(line.strip()))
                        except (StopIteration, json.JSONDecodeError):
                            continue
                if len(sample_data) >= sample_size:
                    break
            
            if not sample_data:
                return 10000  # Default if sampling fails
            
            # Calculate average record size
            avg_record_size = sum(sys.getsizeof(str(d)) for d in sample_data) / len(sample_data)
            chunk_size = int(self.max_memory * 0.1 / avg_record_size)  # Use 10% of max memory per chunk
            
            return max(1000, min(chunk_size, 100000))  # Keep within reasonable bounds
            
        except Exception as e:
            print(f"Warning: Error calculating chunk size: {str(e)}")
            return 10000  # Default fallback

    def _parse_log_entry(self, log_data):
        """
        Parse a single log entry from JSON data.
        
        Args:
            log_data (dict): Raw JSON log data
            
        Returns:
            dict: Parsed log entry or None if invalid
        """
        try:
            return {
                'timestamp': log_data.get('logtime'),
                'thread': log_data.get('thread'),
                'level': log_data.get('level'),
                'class': log_data.get('class'),
                'message': log_data.get('log'),
                'container': log_data.get('kubernetes', {}).get('container_name'),
                'namespace': log_data.get('kubernetes', {}).get('namespace_name'),
                'pod': log_data.get('kubernetes', {}).get('pod_name'),
                'host': log_data.get('kubernetes', {}).get('host')
            }
        except Exception:
            return None

    def process_file_streaming(self, file_path):
        """
        Process a single log file using streaming to minimize memory usage.
        
        Args:
            file_path (Path): Path to gzip file
            
        Returns:
            tuple: (Path to temp file, lines processed, error count)
        """
        temp_file = self.temp_dir / f"temp_{file_path.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        current_chunk = []
        lines_processed = 0
        errors = 0

        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    lines_processed += 1
                    try:
                        log_data = json.loads(line.strip())
                        entry = self._parse_log_entry(log_data)
                        if entry:
                            current_chunk.append(entry)
                            
                            # Write chunk to parquet when it reaches chunk size
                            if len(current_chunk) >= self.chunk_size:
                                self._save_chunk_to_parquet(current_chunk, temp_file)
                                current_chunk = []
                    except Exception:
                        errors += 1

                # Save any remaining records
                if current_chunk:
                    self._save_chunk_to_parquet(current_chunk, temp_file)

            return temp_file, lines_processed, errors

        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
            return None, lines_processed, errors

    def _save_chunk_to_parquet(self, chunk, file_path, append=True):
        """
        Save a chunk of data to parquet format.
        
        Args:
            chunk (list): List of log entries
            file_path (Path): Output file path
            append (bool): Whether to append to existing file
        """
        df = pd.DataFrame(chunk)
        if append and file_path.exists():
            df.to_parquet(file_path, append=True, engine='fastparquet')
        else:
            df.to_parquet(file_path, engine='fastparquet')
    def analyze_logs(self):
        """
        Main analysis method using parallel processing and streaming.
        
        Returns:
            dict: Analysis results
        """
        try:
            # Find all gz files
            gz_files = list(self.base_folder.rglob('*.gz'))
            total_files = len(gz_files)
            
            if not total_files:
                raise LogAnalysisError("No .gz files found in the specified directory")

            print(f"\nFound {total_files} files to process")
            print(f"Using chunk size of {self.chunk_size} records")
            print(f"Maximum memory limit: {self.max_memory / 1024**3:.1f} GB")
            
            temp_files = []
            
            # Process files in parallel
            with ProcessPoolExecutor() as executor:
                futures = {executor.submit(self.process_file_streaming, f): f 
                          for f in gz_files}
                
                with tqdm(total=len(futures), desc="Processing files") as pbar:
                    for future in as_completed(futures):
                        temp_file, lines, errors = future.result()
                        if temp_file:
                            temp_files.append(temp_file)
                        self.total_lines += lines
                        self.error_lines += errors
                        self.files_processed += 1
                        pbar.update(1)
                        pbar.set_postfix({
                            'Lines': self.total_lines,
                            'Errors': self.error_lines
                        })

            if not temp_files:
                raise LogAnalysisError("No valid data processed from log files")

            # Create Dask DataFrame from temporary parquet files
            print("\nCombining results...")
            ddf = dd.read_parquet(temp_files)
            
            # Perform analyses
            self.analyses = self._generate_analyses(ddf)
            
            # Save results
            self._save_analyses()
            
            # Print summary
            self._print_summary()
            
            return self.analyses

        except Exception as e:
            print(f"Analysis failed: {str(e)}")
            traceback.print_exc()
            raise
        finally:
            self._cleanup_temp_files()

    def _generate_analyses(self, ddf):
        """
        Generate analyses using Dask DataFrame with comprehensive error handling.
        
        Args:
            ddf (dask.DataFrame): Dask DataFrame containing log data
            
        Returns:
            dict: Analysis results
        """
        print("\nGenerating analyses...")
        analyses = {}
        
        try:
            # Validate input DataFrame
            required_columns = ['timestamp', 'level', 'class', 'pod', 'container', 'host', 'thread']
            missing_columns = [col for col in required_columns if col not in ddf.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

            # Validate data types and clean data
            print("Validating and cleaning data...")
            
            # Ensure level is uppercase and handle missing values
            ddf['level'] = ddf['level'].fillna('UNKNOWN').map(lambda x: str(x).upper())
            
            # Handle missing values in grouping columns
            ddf['class'] = ddf['class'].fillna('UNKNOWN')
            ddf['pod'] = ddf['pod'].fillna('UNKNOWN')
            ddf['container'] = ddf['container'].fillna('UNKNOWN')
            ddf['host'] = ddf['host'].fillna('UNKNOWN')
            ddf['thread'] = ddf['thread'].fillna('UNKNOWN')

            # Convert timestamps with error handling
            print("Converting timestamps...")
            try:
                ddf['parsed_timestamp'] = dd.to_datetime(
                    ddf['timestamp'],
                    format='%d/%b/%Y:%H:%M:%S +0000',
                    errors='coerce'
                )
                # Check for null timestamps after conversion
                null_timestamps = ddf['parsed_timestamp'].isnull().compute().sum()
                if null_timestamps > 0:
                    print(f"Warning: {null_timestamps} timestamps could not be parsed")
                
                ddf['hour'] = ddf['parsed_timestamp'].dt.hour
            except Exception as e:
                raise ValueError(f"Error parsing timestamps: {str(e)}")

            analysis_tasks = [
                ('class_level_counts', lambda: ddf.groupby(['class', 'level'])
                    .size().compute().unstack(fill_value=0).reset_index()),
                ('level_summary', lambda: ddf.level.value_counts().compute()),
                ('class_summary', lambda: ddf.groupby('class').size().compute()),
                ('pod_summary', lambda: ddf.groupby('pod').size().compute()),
                ('container_summary', lambda: ddf.groupby('container').size().compute()),
                ('host_summary', lambda: ddf.groupby('host').size().compute()),
                ('class_level_pod', lambda: ddf.groupby(['class', 'pod', 'level'])
                    .size().compute().unstack(fill_value=0).reset_index()),
                ('hourly_level_counts', lambda: ddf.groupby(['hour', 'level'])
                    .size().compute().unstack(fill_value=0).reset_index()),
                ('thread_summary', lambda: ddf.groupby('thread').size().compute()),
                ('error_analysis', lambda: ddf[ddf.level == 'ERROR']
                    .groupby(['class', 'pod']).size().compute().sort_values(ascending=False)),
                ('time_range', lambda: pd.DataFrame([{
                    'start_time': ddf['parsed_timestamp'].min().compute(),
                    'end_time': ddf['parsed_timestamp'].max().compute()
                }]))
            ]

            with tqdm(total=len(analysis_tasks), desc="Generating analyses") as pbar:
                for name, task in analysis_tasks:
                    try:
                        pbar.set_description(f"Generating {name}")
                        result = task()
                        
                        # Validate result is not empty
                        if result.empty:
                            print(f"\nWarning: {name} analysis produced empty result")
                        
                        analyses[name] = result
                        pbar.update(1)
                    except Exception as e:
                        error_msg = f"\nError generating {name}: {str(e)}"
                        print(error_msg)
                        raise LogAnalysisError(error_msg)

            # Validate final results
            if not analyses:
                raise LogAnalysisError("No analyses were generated successfully")

            print("\nAll analyses completed successfully!")
            
            # Validate time range
            if 'time_range' in analyses:
                time_range = analyses['time_range']
                if time_range['start_time'].iloc[0] > time_range['end_time'].iloc[0]:
                    print("\nWarning: Start time is after end time in the data")

            return analyses
                
        except Exception as e:
            if isinstance(e, LogAnalysisError):
                raise
            raise LogAnalysisError(f"Error generating analyses: {str(e)}")
        finally:
            # Clean up any temporary resources if needed
            pass

    def _save_analyses(self):
        """Save analysis results to CSV files."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        print("\nSaving analysis files...")
        for name, data in tqdm(self.analyses.items(), desc="Saving files"):
            output_file = self.output_folder / f"{name}_{timestamp}.csv"
            data.to_csv(output_file)
            print(f"Saved {name} to {output_file}")

    def _print_summary(self):
        """Print human-readable summary of analysis results."""
        print("\nLog Analysis Summary")
        print("-" * 80)
        
        # Time range
        if 'time_range' in self.analyses:
            time_range = self.analyses['time_range'].iloc[0]
            print("\nTime Range:")
            print(f"Start: {time_range['start_time']}")
            print(f"End: {time_range['end_time']}")
            duration = (time_range['end_time'] - time_range['start_time']).total_seconds() / 3600
            print(f"Duration: {duration:.2f} hours")
        
        # Log level distribution
        if 'level_summary' in self.analyses:
            print("\nLog Level Distribution:")
            level_counts = self.analyses['level_summary']
            total_logs = level_counts.sum()
            for level, count in level_counts.items():
                percentage = (count / total_logs) * 100
                print(f"{level:<10}: {count:>8,d} ({percentage:>6.2f}%)")
        
        # Top classes with errors
        if 'error_analysis' in self.analyses:
            print("\nTop 5 Classes with Errors:")
            for (class_name, pod), count in self.analyses['error_analysis'].head().items():
                print(f"{class_name:<40} ({pod}): {count:>6,d}")
        
        # Processing statistics
        print("\nProcessing Statistics:")
        print(f"Files Processed: {self.files_processed:,d}")
        print(f"Total Lines: {self.total_lines:,d}")
        print(f"Error Lines: {self.error_lines:,d}")
        if self.total_lines > 0:
            success_rate = ((self.total_lines - self.error_lines) / self.total_lines) * 100
            print(f"Success Rate: {success_rate:.2f}%")

    def _cleanup_temp_files(self):
        """Clean up temporary files and directory."""
        try:
            for file in self.temp_dir.glob('*.parquet'):
                file.unlink()
            self.temp_dir.rmdir()
        except Exception as e:
            print(f"Warning: Error cleaning up temporary files: {str(e)}")

def main():
    """Main execution function."""
    print("Scalable Log Analysis Tool")
    print("-" * 80)
    
    try:
        # Get input path
        if len(sys.argv) > 1:
            base_folder = sys.argv[1]
        else:
            base_folder = input("Enter path to log files: ").strip()
        
        # Optional memory limit
        max_memory = None
        if len(sys.argv) > 2:
            max_memory = float(sys.argv[2])
        
        # Initialize and run analyzer
        analyzer = LogAnalyzer(base_folder, max_memory_gb=max_memory)
        analyzer.analyze_logs()
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {str(e)}")
        print("\nStack trace:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
