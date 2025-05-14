# Saviynt Log Analyzer

A Streamlit-based application for analyzing customer log files with comprehensive visualization and data persistence features.

## Features
- Processes .gz log files from hourly folders (YYYYMMDD-HH, e.g., 20250421-00)
- Analyzes log levels (DEBUG, INFO, WARN, ERROR, FATAL) by class and service
- Visualizes data with:
  - Tables for log level counts by class and service
  - Timeline graph of log levels
  - Pie charts for log distribution by class and service
  - Detailed breakdown tables per log level
- Supports pause/resume functionality
- Downloads results as an Excel file with multiple sheets
- Automatic or manual refresh
- Beautiful, responsive UI

## Installation
1. Clone the repository
2. Create a virtual environment: `python -m venv venv`
3. Activate the virtual environment: `source venv/bin/activate` (Windows: `venv\Scripts\activate`)
4. Install dependencies: `pip install -r requirements.txt`
5. Create `config.yaml` in `config/` directory
6. Run the application: `streamlit run app.py`

## Usage
1. Enter the log folder path (e.g., `/path/to/customer_logs`) in the sidebar
2. Start the analysis using the "Start Analysis" button
3. View visualizations in the main dashboard
4. Pause/resume analysis as needed
5. Download results as an Excel file
6. Adjust refresh interval via the sidebar slider

## Folder Structure
- Logs must be in subfolders named `YYYYMMDD-HH` (e.g., `20250421-00`)
- Files must be named `cluster-log-N.gz` (e.g., `cluster-log-0.gz`)
- Logs must be JSON with a `logtime` key to be processed

## Configuration
Edit `config/config.yaml` to modify:
- Refresh intervals
- Log levels to track
- Theme colors
- Data storage paths