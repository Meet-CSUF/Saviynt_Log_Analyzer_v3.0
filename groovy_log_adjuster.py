#!/usr/bin/env python3
"""
Groovy Log Level Adjuster

This script analyzes and adjusts log levels in Groovy files based on context.
It preserves all log messages and only changes the log level.
"""

import os
import re
import csv
import time
import argparse
import concurrent.futures
from collections import defaultdict
from typing import Dict, List, Tuple, Set, Optional

# Regular expression to match log statements in Groovy files
# This pattern captures the log level and the message content
LOG_PATTERN = re.compile(
    r'(log\.(?:error|warn|info|debug|trace|fatal)|logger\.(?:error|warn|info|debug|trace|fatal))'
    r'\s*\(\s*(.*?)(?:\s*,\s*(.+?))?\s*\)', 
    re.DOTALL
)

# Regular expression to identify method declarations
METHOD_PATTERN = re.compile(r'(?:public|private|protected)?\s+(?:static\s+)?(?:\w+(?:<.+?>)?(?:\[\])?\s+)?(\w+)\s*\([^)]*\)\s*(?:throws\s+[\w,\s]+)?\s*\{', re.DOTALL)

# Method entry/exit indicators for TRACE level
TRACE_INDICATORS = [
    "enter", "exit", "entering", "exiting", "start", "end", 
    "starting", "ending", "begin", "finish", "finished",
    "method", "function", "called", "calling", "returns", "returning"
]

# Warning indicators for WARN level
WARN_INDICATORS = [
    "warn", "warning", "caution", "missing", "skip", "skipped", 
    "skipping", "fallback", "deprecated", "non-critical", 
    "attention", "notice", "unexpected", "unusual", "retry",
    "not found", "not available", "timeout", "slow", "delay"
]

# Error indicators for ERROR level
ERROR_INDICATORS = [
    "error", "exception", "fail", "failed", "failure", "critical", 
    "severe", "unable", "cannot", "crash", "crashed", "invalid",
    "incorrect", "malformed", "rejected", "denied", "unauthorized",
    "forbidden", "violation", "corrupt", "corrupted", "broken"
]

# Fatal indicators for FATAL level
FATAL_INDICATORS = [
    "fatal", "catastrophic", "disaster", "emergency", "halt", 
    "shutdown", "terminate", "killed", "unrecoverable", "panic",
    "system down", "critical failure", "abort", "aborted"
]

# Info indicators for INFO level
INFO_INDICATORS = [
    "info", "status", "complete", "completed", "success", "successful",
    "processed", "count", "summary", "total", "configuration", "config",
    "parameter", "setting", "import", "export", "job", "batch", "record",
    "created", "updated", "deleted", "modified", "changed", "executed",
    "loaded", "initialized", "started", "stopped", "received", "sent"
]

class LogLevelAdjuster:
    """Class to handle the adjustment of log levels in Groovy files."""
    
    def __init__(self, max_workers: int = 1):
        """Initialize the LogLevelAdjuster.
        
        Args:
            max_workers: Maximum number of worker threads for parallel processing
        """
        self.max_workers = max_workers
        self.total_files = 0
        self.processed_files = 0
        self.metrics = []
        self.start_time = time.time()
    
    def extract_method_context(self, content: str) -> Dict[Tuple[int, int], str]:
        """Extract method names and their positions in the file.
        
        Args:
            content: The file content
            
        Returns:
            Dictionary mapping position ranges to method names
        """
        method_contexts = {}
        
        # Find all method declarations
        for match in METHOD_PATTERN.finditer(content):
            method_name = match.group(1)
            start_pos = match.start()
            
            # Find the matching closing brace
            open_braces = 1
            end_pos = start_pos + match.group(0).rfind('{') + 1
            
            while open_braces > 0 and end_pos < len(content):
                if content[end_pos] == '{':
                    open_braces += 1
                elif content[end_pos] == '}':
                    open_braces -= 1
                end_pos += 1
            
            method_contexts[(start_pos, end_pos)] = method_name
        
        return method_contexts
    
    def get_method_for_position(self, position: int, method_contexts: Dict[Tuple[int, int], str]) -> Optional[str]:
        """Get the method name for a given position in the file.
        
        Args:
            position: Position in the file
            method_contexts: Dictionary mapping position ranges to method names
            
        Returns:
            Method name or None if not in a method
        """
        for (start, end), method_name in method_contexts.items():
            if start <= position <= end:
                return method_name
        return None
    
    def determine_appropriate_level(self, current_level: str, message: str, method_name: Optional[str] = None) -> str:
        """Determine the appropriate log level based on the message content and context.
        
        Args:
            current_level: The current log level
            message: The log message content
            method_name: The name of the method containing this log statement
            
        Returns:
            The appropriate log level
        """
        message_lower = message.lower()
        
        # Check for method entry/exit patterns (TRACE)
        for indicator in TRACE_INDICATORS:
            if indicator in message_lower:
                return "trace"
        
        # Check if the log message mentions the method name and is about entry/exit
        if method_name and method_name.lower() in message_lower:
            if any(word in message_lower for word in ["start", "end", "enter", "exit", "call", "return"]):
                return "trace"
        
        # Check for error patterns (ERROR)
        for indicator in ERROR_INDICATORS:
            if indicator in message_lower:
                # Check for exception mentions
                if "exception" in message_lower or "throwable" in message_lower:
                    return "error"
                # Check for critical errors
                if any(word in message_lower for word in ["critical", "severe", "failed"]):
                    return "error"
                return "error"
        
        # Check for fatal patterns (FATAL)
        for indicator in FATAL_INDICATORS:
            if indicator in message_lower:
                return "fatal"
        
        # Check for warning patterns (WARN)
        for indicator in WARN_INDICATORS:
            if indicator in message_lower:
                return "warn"
        
        # Check for info patterns (INFO)
        for indicator in INFO_INDICATORS:
            if indicator in message_lower:
                return "info"
        
        # Context-based decisions
        if "query" in message_lower or "sql" in message_lower:
            return "debug"  # SQL queries are typically debug
        
        if "parameter" in message_lower or "value" in message_lower:
            if "=" in message or ":" in message:  # Likely showing a value
                return "debug"
        
        # If message contains data structure dumps or technical details
        if any(term in message_lower for term in ["map", "list", "array", "object", "json", "xml", "response", "request"]):
            if len(message) > 100:  # Long messages with data structures
                return "debug"
        
        # Default to DEBUG if no specific pattern is matched
        return "debug"
    
    def process_file(self, file_path: str) -> Dict[str, Dict[str, int]]:
        """Process a single Groovy file to adjust log levels.
        
        Args:
            file_path: Path to the Groovy file
            
        Returns:
            Dictionary containing before and after metrics
        """
        try:
            # Read the file content
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Extract method contexts
            method_contexts = self.extract_method_context(content)
            
            # Track log level counts before changes
            before_counts = defaultdict(int)
            after_counts = defaultdict(int)
            
            # Find all log statements
            modified_content = content
            matches = list(LOG_PATTERN.finditer(content))
            
            # Process matches in reverse order to avoid offset issues
            for match in reversed(matches):
                full_match = match.group(0)
                log_statement = match.group(1)
                message = match.group(2)
                
                # Extract current log level
                current_level = log_statement.split('.')[-1]
                before_counts[current_level] += 1
                
                # Get method context
                method_name = self.get_method_for_position(match.start(), method_contexts)
                
                # Determine appropriate level
                appropriate_level = self.determine_appropriate_level(current_level, message, method_name)
                after_counts[appropriate_level] += 1
                
                # Replace only if the level needs to change
                if current_level != appropriate_level:
                    new_log_statement = log_statement.replace(f".{current_level}", f".{appropriate_level}")
                    modified_content = (
                        modified_content[:match.start()] + 
                        full_match.replace(log_statement, new_log_statement) + 
                        modified_content[match.end():]
                    )
            
            # Write the modified content back to the file
            if content != modified_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(modified_content)
            
            return {
                "file_path": file_path,
                "before": dict(before_counts),
                "after": dict(after_counts)
            }
        
        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
            return {
                "file_path": file_path,
                "before": {},
                "after": {},
                "error": str(e)
            }
    
    def process_directory(self, directory_path: str, recursive: bool = False) -> List[Dict]:
        """Process all Groovy files in a directory.
        
        Args:
            directory_path: Path to the directory
            recursive: Whether to process subdirectories recursively
            
        Returns:
            List of dictionaries containing metrics for each file
        """
        groovy_files = []
        
        # Collect all Groovy files
        if recursive:
            for root, _, files in os.walk(directory_path):
                for file in files:
                    if file.endswith('.groovy'):
                        groovy_files.append(os.path.join(root, file))
        else:
            for file in os.listdir(directory_path):
                if file.endswith('.groovy'):
                    groovy_files.append(os.path.join(directory_path, file))
        
        self.total_files = len(groovy_files)
        print(f"Found {self.total_files} Groovy files to process")
        
        results = []
        
        # Process files in parallel if max_workers > 1
        if self.max_workers > 1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_file = {executor.submit(self.process_file, file): file for file in groovy_files}
                
                for future in concurrent.futures.as_completed(future_to_file):
                    file = future_to_file[future]
                    try:
                        result = future.result()
                        results.append(result)
                        self.processed_files += 1
                        print(f"Progress: {self.processed_files}/{self.total_files} files processed")
                    except Exception as e:
                        print(f"Error processing {file}: {str(e)}")
        else:
            # Process files sequentially
            for file in groovy_files:
                result = self.process_file(file)
                results.append(result)
                self.processed_files += 1
                print(f"Progress: {self.processed_files}/{self.total_files} files processed")
        
        return results
    
    def save_metrics_to_csv(self, metrics: List[Dict], output_file: str = "log_level_metrics.csv") -> None:
        """Save metrics to a CSV file.
        
        Args:
            metrics: List of dictionaries containing metrics
            output_file: Path to the output CSV file
        """
        if not metrics:
            print("No metrics to save")
            return
        
        # Prepare CSV headers
        fieldnames = ["file_path"]
        log_levels = ["error", "warn", "info", "debug", "trace", "fatal"]
        
        for level in log_levels:
            fieldnames.extend([f"before_{level}", f"after_{level}"])
        
        # Write to CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for metric in metrics:
                row = {"file_path": metric["file_path"]}
                
                for level in log_levels:
                    row[f"before_{level}"] = metric["before"].get(level, 0)
                    row[f"after_{level}"] = metric["after"].get(level, 0)
                
                writer.writerow(row)
        
        print(f"Metrics saved to {output_file}")
    
    def print_summary(self, metrics: List[Dict]) -> None:
        """Print a summary of the changes made.
        
        Args:
            metrics: List of dictionaries containing metrics
        """
        total_before = defaultdict(int)
        total_after = defaultdict(int)
        
        for metric in metrics:
            for level, count in metric["before"].items():
                total_before[level] += count
            for level, count in metric["after"].items():
                total_after[level] += count
        
        print("\nSummary of changes:")
        print("===================")
        print("Log Level | Before | After | Difference")
        print("---------|--------|-------|------------")
        
        for level in ["error", "warn", "info", "debug", "trace", "fatal"]:
            before = total_before.get(level, 0)
            after = total_after.get(level, 0)
            diff = after - before
            diff_str = f"+{diff}" if diff > 0 else str(diff)
            print(f"{level.upper():9} | {before:6} | {after:5} | {diff_str}")
        
        print("\nTotal execution time: {:.2f} seconds".format(time.time() - self.start_time))


def main():
    """Main function to parse arguments and run the log level adjuster."""
    parser = argparse.ArgumentParser(description="Adjust log levels in Groovy files based on context")
    parser.add_argument("--workers", type=int, default=1, help="Number of worker threads for parallel processing")
    args = parser.parse_args()
    
    print("Groovy Log Level Adjuster")
    print("========================\n")
    
    # Get path from user
    path = input("Enter the path to a Groovy file or directory: ").strip()
    
    # Validate path
    if not os.path.exists(path):
        print(f"Error: Path '{path}' does not exist")
        return
    
    # Initialize the adjuster
    workers = args.workers
    if workers <= 0:
        workers = 1
    elif workers > os.cpu_count():
        print(f"Warning: Requested {workers} workers, but only {os.cpu_count()} CPU cores available.")
        workers = os.cpu_count()
    
    adjuster = LogLevelAdjuster(max_workers=workers)
    
    # Process based on path type
    if os.path.isfile(path):
        if not path.endswith('.groovy'):
            print("Error: File must be a Groovy file (.groovy extension)")
            return
        
        print(f"Processing file: {path}")
        adjuster.total_files = 1
        result = adjuster.process_file(path)
        metrics = [result]
        adjuster.processed_files = 1
    else:  # Directory
        recursive = input("Process subdirectories recursively? (y/n): ").strip().lower() == 'y'
        print(f"Processing directory: {path} {'(recursively)' if recursive else ''}")
        metrics = adjuster.process_directory(path, recursive)
    
    # Save metrics and print summary
    if metrics:
        output_file = input("Enter the name for the CSV metrics file (default: log_level_metrics.csv): ").strip()
        if not output_file:
            output_file = "log_level_metrics.csv"
        
        adjuster.save_metrics_to_csv(metrics, output_file)
        adjuster.print_summary(metrics)
    else:
        print("No files were processed")


if __name__ == "__main__":
    main()
