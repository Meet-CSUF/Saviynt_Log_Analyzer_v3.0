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

# Regular expression to identify class declarations
CLASS_PATTERN = re.compile(r'(?:public|private|protected)?\s+(?:abstract\s+)?class\s+(\w+)(?:\s+extends\s+\w+)?(?:\s+implements\s+[\w,\s]+)?\s*\{', re.DOTALL)

# Regular expression to identify try-catch blocks
TRY_CATCH_PATTERN = re.compile(r'try\s*\{', re.DOTALL)
CATCH_PATTERN = re.compile(r'catch\s*\(\s*(\w+(?:\.\w+)*)\s+\w+\)\s*\{', re.DOTALL)

# Regular expression to identify conditional statements
CONDITIONAL_PATTERN = re.compile(r'if\s*\([^)]*\)\s*\{|else\s*\{|else\s+if\s*\([^)]*\)\s*\{|switch\s*\([^)]*\)\s*\{|case\s+.+?:', re.DOTALL)

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
    
    def extract_code_context(self, content: str) -> Dict:
        """Extract code context information from the file content.
        
        Args:
            content: The file content
            
        Returns:
            Dictionary containing code context information
        """
        context = {
            'methods': {},  # Maps (start, end) -> method_name
            'classes': {},  # Maps (start, end) -> class_name
            'try_catch': [],  # List of (start, end) for try blocks
            'catch_blocks': [],  # List of (start, end, exception_type) for catch blocks
            'conditionals': []  # List of (start, end) for conditional blocks
        }
        
        # Find all method declarations and their scopes
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
            
            context['methods'][(start_pos, end_pos)] = method_name
        
        # Find all class declarations and their scopes
        for match in CLASS_PATTERN.finditer(content):
            class_name = match.group(1)
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
            
            context['classes'][(start_pos, end_pos)] = class_name
        
        # Find all try blocks and their scopes
        for match in TRY_CATCH_PATTERN.finditer(content):
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
            
            context['try_catch'].append((start_pos, end_pos))
        
        # Find all catch blocks and their scopes
        for match in CATCH_PATTERN.finditer(content):
            exception_type = match.group(1)
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
            
            context['catch_blocks'].append((start_pos, end_pos, exception_type))
        
        # Find all conditional blocks and their scopes
        for match in CONDITIONAL_PATTERN.finditer(content):
            start_pos = match.start()
            
            # For case statements, we don't have a clear scope
            if match.group(0).strip().startswith('case '):
                continue
                
            # Find the matching closing brace
            open_braces = 1
            end_pos = start_pos + match.group(0).rfind('{') + 1
            
            # Skip if no opening brace found (one-line conditionals)
            if '{' not in match.group(0):
                continue
                
            while open_braces > 0 and end_pos < len(content):
                if content[end_pos] == '{':
                    open_braces += 1
                elif content[end_pos] == '}':
                    open_braces -= 1
                end_pos += 1
            
            context['conditionals'].append((start_pos, end_pos))
        
        return context
    
    def get_context_for_position(self, position: int, code_context: Dict) -> Dict:
        """Get the code context for a given position in the file.
        
        Args:
            position: Position in the file
            code_context: Dictionary containing code context information
            
        Returns:
            Dictionary with context information for the position
        """
        context_info = {
            'method': None,
            'class': None,
            'in_try_block': False,
            'in_catch_block': None,
            'in_conditional': False
        }
        
        # Check if in a method
        for (start, end), method_name in code_context['methods'].items():
            if start <= position <= end:
                context_info['method'] = method_name
                break
        
        # Check if in a class
        for (start, end), class_name in code_context['classes'].items():
            if start <= position <= end:
                context_info['class'] = class_name
                break
        
        # Check if in a try block
        for start, end in code_context['try_catch']:
            if start <= position <= end:
                context_info['in_try_block'] = True
                break
        
        # Check if in a catch block
        for start, end, exception_type in code_context['catch_blocks']:
            if start <= position <= end:
                context_info['in_catch_block'] = exception_type
                break
        
        # Check if in a conditional block
        for start, end in code_context['conditionals']:
            if start <= position <= end:
                context_info['in_conditional'] = True
                break
        
        return context_info
    
    def analyze_surrounding_code(self, content: str, position: int, window_size: int = 200) -> str:
        """Analyze code surrounding a log statement.
        
        Args:
            content: The file content
            position: Position of the log statement
            window_size: Size of the window to analyze before and after
            
        Returns:
            Surrounding code snippet
        """
        start = max(0, position - window_size)
        end = min(len(content), position + window_size)
        return content[start:end]
    
    def find_related_logs(self, content: str, position: int, window_size: int = 500) -> List[Tuple[str, str]]:
        """Find related log statements near the current log.
        
        Args:
            content: The file content
            position: Position of the current log statement
            window_size: Size of the window to search for related logs
            
        Returns:
            List of (log_level, message) tuples for nearby logs
        """
        start = max(0, position - window_size)
        end = min(len(content), position + window_size)
        surrounding_code = content[start:end]
        
        related_logs = []
        for match in LOG_PATTERN.finditer(surrounding_code):
            if start + match.start() != position:  # Skip the current log
                log_statement = match.group(1)
                message = match.group(2)
                level = log_statement.split('.')[-1]
                related_logs.append((level, message))
        
        return related_logs
    
    def analyze_log_pattern(self, method_name: str, class_name: str, content: str) -> Dict[str, List[str]]:
        """Analyze logging patterns within a method or class.
        
        Args:
            method_name: Name of the method
            class_name: Name of the class
            content: The file content
            
        Returns:
            Dictionary mapping log levels to their frequency in the method/class
        """
        pattern_info = defaultdict(list)
        
        # Find all log statements in the method or class
        for match in LOG_PATTERN.finditer(content):
            log_statement = match.group(1)
            message = match.group(2)
            level = log_statement.split('.')[-1]
            pattern_info[level].append(message)
        
        return pattern_info
    
    def determine_appropriate_level(self, current_level: str, message: str, context_info: Dict, 
                                   surrounding_code: str, related_logs: List[Tuple[str, str]]) -> str:
        """Determine the appropriate log level based on comprehensive context analysis.
        
        Args:
            current_level: The current log level
            message: The log message content
            context_info: Dictionary with context information
            surrounding_code: Code surrounding the log statement
            related_logs: List of related log statements
            
        Returns:
            The appropriate log level
        """
        message_lower = message.lower()
        method_name = context_info.get('method')
        class_name = context_info.get('class')
        in_try_block = context_info.get('in_try_block', False)
        in_catch_block = context_info.get('in_catch_block')
        in_conditional = context_info.get('in_conditional', False)
        
        # 1. Check for method entry/exit patterns (TRACE)
        for indicator in TRACE_INDICATORS:
            if indicator in message_lower:
                return "trace"
        
        # 2. Check if the log message mentions the method name and is about entry/exit
        if method_name and method_name.lower() in message_lower:
            if any(word in message_lower for word in ["start", "end", "enter", "exit", "call", "return"]):
                return "trace"
        
        # 3. Check for exception handling context
        if in_catch_block:
            # If we're in a catch block, this is likely an error or warning
            exception_type = in_catch_block.lower()
            if "exception" in message_lower or exception_type in message_lower:
                return "error"
            
            # Check surrounding code for exception handling patterns
            if "throw" in surrounding_code.lower() or "rethrow" in surrounding_code.lower():
                return "error"
        
        # 4. Check for error patterns (ERROR)
        for indicator in ERROR_INDICATORS:
            if indicator in message_lower:
                return "error"
        
        # 5. Check for fatal patterns (FATAL)
        for indicator in FATAL_INDICATORS:
            if indicator in message_lower:
                return "fatal"
        
        # 6. Check for warning patterns (WARN)
        for indicator in WARN_INDICATORS:
            if indicator in message_lower:
                return "warn"
        
        # 7. Check for info patterns (INFO)
        for indicator in INFO_INDICATORS:
            if indicator in message_lower:
                return "info"
        
        # 8. Analyze conditional logic
        if in_conditional:
            # If in a conditional that checks for errors or warnings
            conditional_lower = surrounding_code.lower()
            if any(term in conditional_lower for term in ["error", "exception", "fail", "invalid"]):
                if "!" in conditional_lower or "==" in conditional_lower:
                    # Checking for absence of error might be info
                    return "info"
                else:
                    # Checking for presence of error might be warn or error
                    return "warn"
        
        # 9. Analyze related logs for patterns
        if related_logs:
            # If surrounded by error logs, this might be an error too
            error_count = sum(1 for level, _ in related_logs if level in ["error", "fatal"])
            if error_count > len(related_logs) / 2:
                return "error"
            
            # If surrounded by info logs, this might be info too
            info_count = sum(1 for level, _ in related_logs if level == "info")
            if info_count > len(related_logs) / 2:
                return "info"
        
        # 10. Context-based decisions
        if "query" in message_lower or "sql" in message_lower:
            return "debug"  # SQL queries are typically debug
        
        if "parameter" in message_lower or "value" in message_lower:
            if "=" in message or ":" in message:  # Likely showing a value
                return "debug"
        
        # 11. If message contains data structure dumps or technical details
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
            
            # Extract code context
            code_context = self.extract_code_context(content)
            
            # Track log level counts before changes
            before_counts = defaultdict(int)
            after_counts = defaultdict(int)
            
            # Find all log statements
            modified_content = content
            matches = list(LOG_PATTERN.finditer(content))
            
            # First pass: collect information about all logs for pattern analysis
            all_logs = []
            for match in matches:
                position = match.start()
                log_statement = match.group(1)
                message = match.group(2)
                current_level = log_statement.split('.')[-1]
                
                context_info = self.get_context_for_position(position, code_context)
                surrounding_code = self.analyze_surrounding_code(content, position)
                related_logs = self.find_related_logs(content, position)
                
                all_logs.append({
                    'position': position,
                    'log_statement': log_statement,
                    'message': message,
                    'current_level': current_level,
                    'context_info': context_info,
                    'surrounding_code': surrounding_code,
                    'related_logs': related_logs
                })
            
            # Second pass: determine appropriate levels with full context awareness
            # Process matches in reverse order to avoid offset issues
            for log_info in reversed(all_logs):
                position = log_info['position']
                log_statement = log_info['log_statement']
                message = log_info['message']
                current_level = log_info['current_level']
                context_info = log_info['context_info']
                surrounding_code = log_info['surrounding_code']
                related_logs = log_info['related_logs']
                
                # Find the full match in the content
                match_start = position
                match_end = position
                for i in range(position, len(content)):
                    if content[position:i].count('(') == content[position:i].count(')') and i > position + len(log_statement):
                        match_end = i
                        break
                
                full_match = content[match_start:match_end]
                
                # Extract current log level
                before_counts[current_level] += 1
                
                # Determine appropriate level
                appropriate_level = self.determine_appropriate_level(
                    current_level, message, context_info, surrounding_code, related_logs
                )
                after_counts[appropriate_level] += 1
                
                # Replace only if the level needs to change
                if current_level != appropriate_level:
                    new_log_statement = log_statement.replace(f".{current_level}", f".{appropriate_level}")
                    modified_content = (
                        modified_content[:match_start] + 
                        full_match.replace(log_statement, new_log_statement) + 
                        modified_content[match_end:]
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
