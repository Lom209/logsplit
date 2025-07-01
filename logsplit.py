#!/usr/bin/env python3
"""
Log Splitter Tool

A Python tool to split log files based on:
1. Log severities (DEBUG, INFO, WARN, ERROR, FATAL, etc.)
2. Timestamp ranges

Usage:
    python log-split.py --input /path/to/logs --severity INFO --output /path/to/output
    python log-split.py --input /path/to/logs --start "2025-06-28 09:30:00" --end "2025-06-28 10:30:00"
"""

import argparse
import os
import re
import glob
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LogSplitter:    

    SEVERITY_LEVELS = {
        'TRACE': 0,
        'DEBUG': 1,
        'INFO': 2,
        'WARN': 3,
        'WARNING': 3,
        'ERROR': 4,
        'FATAL': 5,
        'CRITICAL': 5
    }
    
    TIMESTAMP_PATTERNS = [
        r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d{3})?)',
        r'(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d{3})?)',
        r'(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}(?:\.\d{3})?)',
    ]
    
    def __init__(self):
        self.compiled_patterns = [re.compile(pattern) for pattern in self.TIMESTAMP_PATTERNS]
    
    def find_log_files(self, input_path: str) -> List[str]:
        """Find all log files in the given path"""
        log_files = []
        
        if os.path.isfile(input_path):
            log_files.append(input_path)
        elif os.path.isdir(input_path):
            extensions = ['*.log', '*.txt', '*.out', '*.err']
            for ext in extensions:
                log_files.extend(glob.glob(os.path.join(input_path, '**', ext), recursive=True))
        else:
            raise FileNotFoundError(f"Input path does not exist: {input_path}")
        
        logger.info(f"Found {len(log_files)} log files")
        return log_files
    
    def extract_timestamp(self, line: str) -> Optional[datetime]:
        """Extract timestamp from a log line"""
        for pattern in self.compiled_patterns:
            match = pattern.search(line)
            if match:
                timestamp_str = match.group(1)
                # Try common timestamp formats
                formats = [
                    '%Y-%m-%d %H:%M:%S.%f',
                    '%Y-%m-%d %H:%M:%S',
                    '%Y/%m/%d %H:%M:%S.%f',
                    '%Y/%m/%d %H:%M:%S',
                    '%m/%d/%Y %H:%M:%S.%f',
                    '%m/%d/%Y %H:%M:%S'
                ]
                
                for fmt in formats:
                    try:
                        return datetime.strptime(timestamp_str, fmt)
                    except ValueError:
                        continue
        return None
    
    def extract_severity(self, line: str) -> Optional[str]:

        # Look for severity patterns in the line
        severity_pattern = r'\b(TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL|CRITICAL)\b'
        match = re.search(severity_pattern, line, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        return None
    
    def split_by_severity(self, log_files: List[str], min_severity: str, output_dir: str):

        min_level = self.SEVERITY_LEVELS.get(min_severity.upper(), 2)  # Default to INFO
        
        os.makedirs(output_dir, exist_ok=True)
        
        for log_file in log_files:
            logger.info(f"Processing {log_file} for severity filtering...")
            
            base_name = os.path.splitext(os.path.basename(log_file))[0]
            output_file = os.path.join(output_dir, f"{base_name}_severity_{min_severity.lower()}_and_above.log")
            
            matched_lines = 0
            total_lines = 0
            
            try:
                with open(log_file, 'r', encoding='utf-8', errors='ignore') as infile, \
                     open(output_file, 'w', encoding='utf-8') as outfile:
                    
                    for line in infile:
                        total_lines += 1
                        severity = self.extract_severity(line)
                        
                        if severity and self.SEVERITY_LEVELS.get(severity, 0) >= min_level:
                            outfile.write(line)
                            matched_lines += 1
                        elif not severity and min_level <= 1:  # Include lines without severity if min_level is DEBUG or lower
                            outfile.write(line)
                            matched_lines += 1
                
                logger.info(f"Filtered {matched_lines}/{total_lines} lines to {output_file}")
                
            except Exception as e:
                logger.error(f"Error processing {log_file}: {e}")
    
    def split_by_timerange(self, log_files: List[str], start_time: datetime, end_time: datetime, output_dir: str):

        os.makedirs(output_dir, exist_ok=True)
        
        for log_file in log_files:
            logger.info(f"Processing {log_file} for timestamp filtering...")
            
            base_name = os.path.splitext(os.path.basename(log_file))[0]
            start_str = start_time.strftime('%Y%m%d_%H%M%S')
            end_str = end_time.strftime('%Y%m%d_%H%M%S')
            output_file = os.path.join(output_dir, f"{base_name}_timerange_{start_str}_to_{end_str}.log")
            
            matched_lines = 0
            total_lines = 0
            
            try:
                with open(log_file, 'r', encoding='utf-8', errors='ignore') as infile, \
                     open(output_file, 'w', encoding='utf-8') as outfile:
                    
                    for line in infile:
                        total_lines += 1
                        timestamp = self.extract_timestamp(line)
                        
                        if timestamp and start_time <= timestamp <= end_time:
                            outfile.write(line)
                            matched_lines += 1
                
                logger.info(f"Filtered {matched_lines}/{total_lines} lines to {output_file}")
                
            except Exception as e:
                logger.error(f"Error processing {log_file}: {e}")

def parse_datetime(datetime_str: str) -> datetime:

    formats = [
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d',
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"Unable to parse datetime: {datetime_str}")

def main():
    parser = argparse.ArgumentParser(
        description="Split log files by severity level or timestamp range",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Split by severity (INFO and above)
  python log-split.py --input /var/log --severity INFO --output ./filtered_logs

  # Split by timestamp range
  python log-split.py --input /var/log/app.log --start "2025-06-28 09:30:00" --end "2025-06-28 10:30:00" --output ./time_filtered

  # Split by both severity and time (combine filters)
  python log-split.py --input /var/log --severity ERROR --start "2025-06-28 09:00:00" --end "2025-06-28 18:00:00" --output ./critical_logs
        """
    )
    
    parser.add_argument('--input', '-i', required=True,
                        help='Input log file or directory containing log files')
    parser.add_argument('--output', '-o', required=True,
                        help='Output directory for filtered log files')
    parser.add_argument('--severity', '-s',
                        choices=['TRACE', 'DEBUG', 'INFO', 'WARN', 'WARNING', 'ERROR', 'FATAL', 'CRITICAL'],
                        help='Minimum log severity level to include')
    parser.add_argument('--start',
                        help='Start timestamp (e.g., "2025-06-28 09:30:00")')
    parser.add_argument('--end',
                        help='End timestamp (e.g., "2025-06-28 10:30:00")')
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.severity and not (args.start and args.end):
        parser.error("Must specify either --severity or both --start and --end")
    
    if args.start and not args.end:
        parser.error("--start requires --end to be specified")
    
    if args.end and not args.start:
        parser.error("--end requires --start to be specified")
    
    # Initialize log splitter
    splitter = LogSplitter()
    
    try:
        # Find log files
        log_files = splitter.find_log_files(args.input)
        
        if not log_files:
            logger.error("No log files found in the specified input path")
            return 1
        
        # Parse timestamps if provided
        start_time = None
        end_time = None
        if args.start and args.end:
            start_time = parse_datetime(args.start)
            end_time = parse_datetime(args.end)
            
            if start_time >= end_time:
                logger.error("Start time must be before end time")
                return 1
        
        # Perform filtering
        if args.severity and start_time and end_time:
            # Combined filtering: first by time, then by severity
            logger.info("Applying combined severity and timestamp filtering...")
            temp_dir = os.path.join(args.output, 'temp')
            splitter.split_by_timerange(log_files, start_time, end_time, temp_dir)
            
            # Now filter the time-filtered files by severity
            temp_files = splitter.find_log_files(temp_dir)
            splitter.split_by_severity(temp_files, args.severity, args.output)
            
            # Clean up temp directory
            import shutil
            shutil.rmtree(temp_dir)
            
        elif args.severity:
            logger.info(f"Filtering by severity: {args.severity} and above...")
            splitter.split_by_severity(log_files, args.severity, args.output)
            
        elif start_time and end_time:
            logger.info(f"Filtering by timestamp range: {start_time} to {end_time}...")
            splitter.split_by_timerange(log_files, start_time, end_time, args.output)
        
        logger.info("Log splitting completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1

if __name__ == '__main__':
    exit(main())
