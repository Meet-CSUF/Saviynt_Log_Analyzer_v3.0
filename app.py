import html
import streamlit as st
import pandas as pd
import requests
import yaml
import logging
import time
import json
import sqlite3
from datetime import datetime
from analyzer.visualizer import Visualizer
from analyzer.data_manager import export_to_excel, get_analysis_data, init_db
from retrying import retry
import os
import re
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Configure logging
logging.basicConfig(
    filename='log_analyzer.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Backend API base URL
BACKEND_URL = "http://localhost:8000"

def load_config():
    """Load configuration from YAML file."""
    try:
        with open('config/config.yaml', 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logger.error("Config file config/config.yaml is not found")
        st.error("Configuration file not found. Please create config/config.yaml.")
        return {'app': {'log_levels': [], 'data_dir': 'data', 'state_dir': 'data'}}
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        st.error(f"Error loading config: {str(e)}")
        return {'app': {'log_levels': []}}

def initialize_session_state():
    """Initialize Streamlit session state variables."""
    if 'selected_job_id' not in st.session_state:
        st.session_state.selected_job_id = None
    if 'dashboard_data' not in st.session_state:
        st.session_state.dashboard_data = None
    if 'notifications' not in st.session_state:
        st.session_state.notifications = []
    if 'csv_notifications' not in st.session_state:
        st.session_state.csv_notifications = []
    if 'backend_available' not in st.session_state:
        st.session_state.backend_available = False
    if 'db_initialized' not in st.session_state:
        st.session_state.db_initialized = False
    if 'log_viewer_job_id' not in st.session_state:
        st.session_state.log_viewer_job_id = None
    if 'cached_job_id' not in st.session_state:
        st.session_state.cached_job_id = None
    if 'show_dashboard' not in st.session_state:
        st.session_state.show_dashboard = False
    if 'last_notification_clear' not in st.session_state:
        st.session_state.last_notification_clear = time.time()
    if 'log_viewer_current_page' not in st.session_state:
        st.session_state.log_viewer_current_page = 1
    if 'log_viewer_total_pages' not in st.session_state:
        st.session_state.log_viewer_total_pages = 1
    if 'log_viewer_logs' not in st.session_state:
        st.session_state.log_viewer_logs = []
    if 'log_viewer_total_logs' not in st.session_state:
        st.session_state.log_viewer_total_logs = 0
    if 'log_viewer_last_job_id' not in st.session_state:
        st.session_state.log_viewer_last_job_id = None
    if 'customer_folders' not in st.session_state:
        st.session_state.customer_folders = []
    if 'customer_folders_page' not in st.session_state:
        st.session_state.customer_folders_page = 1
    if 'customer_folders_per_page' not in st.session_state:
        st.session_state.customer_folders_per_page = 100

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def check_backend_health():
    """Check if backend is running and fetch job status."""
    try:
        logger.debug(f"Attempting health check to {BACKEND_URL}/health")
        response = requests.get(f"{BACKEND_URL}/health", timeout=10)
        response.raise_for_status()
        st.session_state.backend_available = True
        logger.info("Backend health check passed")
        
        if st.session_state.selected_job_id:
            # Verify job_id exists in jobs table
            try:
                conn = sqlite3.connect('data/logs.db', timeout=30)
                cursor = conn.cursor()
                cursor.execute("SELECT job_id FROM jobs WHERE job_id = ?", (st.session_state.selected_job_id,))
                job_exists = cursor.fetchone()
                conn.close()
                
                if not job_exists:
                    logger.warning(f"Selected job_id {st.session_state.selected_job_id} not found in database")
                    st.session_state.notifications.append({
                        'type': 'warning',
                        'message': f"Selected job {st.session_state.selected_job_id} no longer exists. Please select a valid job.",
                        'timestamp': time.time()
                    })
                    st.session_state.selected_job_id = None
                    st.session_state.show_dashboard = False
                    return True
            except sqlite3.OperationalError as e:
                logger.error(f"Database error checking job_id {st.session_state.selected_job_id}: {str(e)}")
                st.session_state.notifications.append({
                    'type': 'error',
                    'message': f"Database error checking job status: {str(e)}",
                    'timestamp': time.time()
                })
                return True
            
            # Fetch job status
            try:
                job_response = requests.get(f"{BACKEND_URL}/jobs/{st.session_state.selected_job_id}/status", timeout=10)
                job_response.raise_for_status()
                job_data = job_response.json()
                files_processed = job_data.get('files_processed', 0)
                total_files = job_data.get('total_files', 0)
                logger.info(f"Backend status for job {st.session_state.selected_job_id}: {files_processed}/{total_files} files processed")
                st.session_state.notifications.append({
                    'type': 'success',
                    'message': f"Backend is healthy! Job {st.session_state.selected_job_id}: {files_processed}/{total_files} files processed.",
                    'timestamp': time.time()
                })
            except requests.RequestException as e:
                logger.warning(f"Failed to fetch job status: {str(e)}")
                st.session_state.notifications.append({
                    'type': 'warning',
                    'message': f"Backend is healthy, but failed to fetch job status: {str(e)}",
                    'timestamp': time.time()
                })
        else:
            st.session_state.notifications.append({
                'type': 'success',
                'message': "Backend is healthy! No job selected.",
                'timestamp': time.time()
            })
        return True
    except requests.RequestException as e:
        logger.warning(f"Backend health check failed: {str(e)}")
        st.session_state.backend_available = False
        st.session_state.notifications.append({
            'type': 'error',
            'message': "Backend is not responding. Job control actions are unavailable.",
            'timestamp': time.time()
        })
        return False

def apply_custom_css():
    """Apply Tailwind CSS with glassmorphism and custom styles for customer folders."""
    st.markdown(
        """
        <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
        <style>
        body {
            background: linear-gradient(to bottom, #f0ede6, #e5e2db);
            font-family: 'Inter', sans-serif;
            color: #1F2937;
        }
        .header {
            background: linear-gradient(90deg, #12133f, #2A2B5A);
            color: #FFFFFF;
            padding: 3rem 2rem;
            border-radius: 16px;
            text-align: center;
            margin-bottom: 2rem;
            box-shadow: 0 8px 16px rgba(0, 0, 0, 0.2);
            position: relative;
            overflow: hidden;
        }
        .header::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: radial-gradient(circle, rgba(255,255,255,0.1) 0%, transparent 70%);
            animation: pulse 4s infinite;
        }
        .header h1 {
            font-size: 3rem;
            font-weight: 700;
            margin: 0;
            text-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
        }
        .header p {
            font-size: 1.25rem;
            opacity: 0.9;
        }
        .card {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: 16px;
            padding: 2rem;
            margin-bottom: 1.5rem;
            box-shadow: 0 8px 16px rgba(0, 0, 0, 0.1);
            border: 1px solid rgba(209, 213, 219, 0.3);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        .card:hover {
            transform: translateY(-8px);
            box-shadow: 0 12px 24px rgba(0, 0, 0, 0.15);
        }
        .stButton>button {
            background: linear-gradient(90deg, #12133f, #2A2B5A);
            color: #FFFFFF;
            border: none;
            border-radius: 12px;
            padding: 0.75rem 2rem;
            font-size: 1.1rem;
            font-weight: 600;
            cursor: pointer;
            transition: transform 0.2s, background 0.3s, box-shadow 0.3s;
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
        }
        .stButton>button:hover {
            background: linear-gradient(90deg, #2A2B5A, #12133f);
            transform: scale(1.05);
            box-shadow: 0 6px 12px rgba(0, 0, 0, 0.3);
        }
        .stButton>button:disabled {
            background: #6B7280;
            cursor: not-allowed;
            transform: none;
            box-shadow: none;
        }
        .sidebar .stButton>button {
            width: 100%;
            margin-bottom: 1rem;
        }
        .stTextInput>div>input {
            border-radius: 12px;
            border: 1px solid #D1D5DB;
            padding: 0.75rem;
            background: rgba(255, 255, 255, 0.9);
            transition: border-color 0.2s, box-shadow 0.2s;
        }
        .stTextInput>div>input:focus {
            border-color: #12133f;
            box-shadow: 0 0 0 3px rgba(18, 19, 63, 0.1);
        }
        .stSelectbox>div>select {
            border-radius: 12px;
            border: 1px solid #D1D5DB;
            padding: 0.75rem;
            background: rgba(255, 255, 255, 0.9);
        }
        .notification-success {
            background: rgba(209, 250, 229, 0.95);
            color: #065F46;
            padding: 1rem;
            border-radius: 12px;
            margin-bottom: 1rem;
            border: 1px solid #34D399;
            display: flex;
            align-items: center;
            gap: 0.75rem;
            animation: slideIn 0.3s ease;
        }
        .notification-error {
            background: rgba(254, 226, 226, 0.95);
            color: #991B1B;
            padding: 1rem;
            border-radius: 12px;
            margin-bottom: 1rem;
            border: 1px solid #F87171;
            display: flex;
            align-items: center;
            gap: 0.75rem;
            animation: slideIn 0.3s ease;
        }
        .notification-warning {
            background: rgba(254, 243, 199, 0.95);
            color: #92400E;
            padding: 1rem;
            border-radius: 12px;
            margin-bottom: 1rem;
            border: 1px solid #FBBF24;
            display: flex;
            align-items: center;
            gap: 0.75rem;
            animation: slideIn 0.3s ease;
        }
        .sidebar-content {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: 16px;
            padding: 2rem;
            box-shadow: 0 8px 16px rgba(0, 0, 0, 0.1);
            border: 1px solid rgba(209, 213, 219, 0.3);
        }
        .tab-content {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: 16px;
            padding: 2.5rem;
            box-shadow: 0 8px 16px rgba(0, 0, 0, 0.1);
            border: 1px solid rgba(209, 213, 219, 0.3);
        }
        .tooltip {
            position: relative;
            display: inline-block;
        }
        .tooltip .tooltiptext {
            visibility: hidden;
            width: 200px;
            background: #2A2B5A;
            color: #FFFFFF;
            text-align: center;
            border-radius: 8px;
            padding: 0.75rem;
            position: absolute;
            z-index: 1;
            bottom: 125%;
            left: 50%;
            margin-left: -100px;
            opacity: 0;
            transition: opacity 0.3s;
        }
        .tooltip:hover .tooltiptext {
            visibility: visible;
            opacity: 1;
        }
        .folder-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
            gap: 1.5rem;
            padding: 1rem;
            max-height: 500px;
            overflow-y: auto;
        }
        .folder-card {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: 12px;
            padding: 1.5rem;
            text-align: center;
            border: 1px solid rgba(209, 213, 219, 0.3);
            transition: transform 0.3s ease, box-shadow 0.3s ease, background 0.3s ease;
            cursor: pointer;
            overflow: hidden;
            position: relative;
        }
        .folder-card:hover {
            transform: translateY(-8px);
            box-shadow: 0 12px 24px rgba(0, 0, 0, 0.15);
            background: rgba(255, 255, 255, 1);
        }
        .folder-card h3 {
            font-size: 1.1rem;
            font-weight: 600;
            color: #1F2937;
            margin: 0;
            word-break: break-word;
            white-space: normal;
        }
        .folder-card::before {
            content: '📁';
            font-size: 2rem;
            display: block;
            margin-bottom: 0.5rem;
            opacity: 0.7;
        }
        @media (max-width: 640px) {
            .folder-grid {
                grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
            }
            .folder-card h3 {
                font-size: 1rem;
            }
        }
        @keyframes slideIn {
            from { transform: translateX(-20px); opacity: 0; }
            to { transform: translateX(0); opacity: 1; }
        }
        @keyframes pulse {
            0% { transform: scale(1); opacity: 0.5; }
            50% { transform: scale(1.2); opacity: 0.3; }
            100% { transform: scale(1); opacity: 0.5; }
        }
        </style>
        """,
        unsafe_allow_html=True
    )

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def get_job_status():
    """Fetch all job statuses from SQLite database."""
    try:
        conn = sqlite3.connect('data/logs.db', timeout=30)
        query = """
            SELECT job_id, folder_path, status, files_processed, total_files, start_time, last_updated
            FROM jobs
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except sqlite3.OperationalError as e:
        logger.error(f"Database error fetching job status: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Database error fetching job status: {str(e)}",
            'timestamp': time.time()
        })
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error fetching job status: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error fetching job status: {str(e)}",
            'timestamp': time.time()
        })
        return pd.DataFrame()

@st.cache_data(hash_funcs={str: lambda x: x})
def get_job_metadata(job_id: str):
    """Fetch unique classes and services for a job from job_metadata table, cached."""
    try:
        conn = sqlite3.connect('data/logs.db', timeout=30)
        classes = pd.read_sql_query(
            "SELECT value FROM job_metadata WHERE job_id = ? AND type = 'class'",
            conn,
            params=[job_id]
        )['value'].dropna().unique().tolist()
        services = pd.read_sql_query(
            "SELECT value FROM job_metadata WHERE job_id = ? AND type = 'service'",
            conn,
            params=[job_id]
        )['value'].dropna().unique().tolist()
        conn.close()
        logger.info(f"Fetched metadata for job_id: {job_id}, classes: {len(classes)}, services: {len(services)}")
        return classes, services
    except sqlite3.OperationalError as e:
        logger.error(f"Database error fetching metadata for job_id {job_id}: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Database error fetching metadata: {str(e)}",
            'timestamp': time.time()
        })
        return [], []
    except Exception as e:
        logger.error(f"Error fetching metadata for job_id {job_id}: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error fetching metadata: {str(e)}",
            'timestamp': time.time()
        })
        return [], []

@st.cache_data(hash_funcs={str: lambda x: x})
def get_logs_by_class_and_level(job_id: str, class_name: str, level: str, page: int, logs_per_page: int, search_query: str = None, use_regex: bool = False):
    """Retrieve logs by class and level from SQLite, cached."""
    try:
        start_time = time.time()
        conn = sqlite3.connect('data/logs.db', timeout=30)
        cursor = conn.cursor()
        offset = (page - 1) * logs_per_page
        
        # Log query parameters
        logger.debug(f"get_logs_by_class_and_level: job_id={job_id}, class={class_name}, level={level}, page={page}, logs_per_page={logs_per_page}, search_query={search_query}, use_regex={use_regex}")
        
        # Base query
        if level == "ALL":
            query = """
                SELECT timestamp, log_message, level, class
                FROM logs
                WHERE job_id = ? AND class = ?
            """
            params = [job_id, class_name]
        else:
            query = """
                SELECT timestamp, log_message, level, class
                FROM logs
                WHERE job_id = ? AND class = ? AND level = ?
            """
            params = [job_id, class_name, level]
        
        # Add search query if provided
        if search_query and search_query.strip():
            if use_regex:
                query += " AND log_message REGEXP ?"
                params.append(search_query)
            else:
                query += " AND log_message LIKE ?"
                params.append(f'%{search_query}%')
        
        # Add sorting and pagination
        query += " ORDER BY timestamp LIMIT ? OFFSET ?"
        params.extend([logs_per_page, offset])
        
        # Log the exact query
        logger.debug(f"Executing SQL: {query} with params: {params}")
        
        # Execute data query
        cursor.execute(query, params)
        logs = [
            {"timestamp": row[0], "log_message": row[1], "level": row[2], "class": row[3]}
            for row in cursor.fetchall()
        ]
        
        # Count query
        if level == "ALL":
            count_query = """
                SELECT COUNT(*) as total
                FROM logs
                WHERE job_id = ? AND class = ?
            """
            count_params = [job_id, class_name]
        else:
            count_query = """
                SELECT COUNT(*) as total
                FROM logs
                WHERE job_id = ? AND class = ? AND level = ?
            """
            count_params = [job_id, class_name, level]
        
        if search_query and search_query.strip():
            if use_regex:
                count_query += " AND log_message REGEXP ?"
                count_params.append(search_query)
            else:
                count_query += " AND log_message LIKE ?"
                count_params.append(f'%{search_query}%')
        
        # Execute count query
        cursor.execute(count_query, count_params)
        total_logs = cursor.fetchone()[0]
        
        conn.close()
        query_time = time.time() - start_time
        logger.debug(f"Fetched {len(logs)} logs, total_logs={total_logs}, page={page}, query_time={query_time:.2f}s")
        return logs, total_logs
    except sqlite3.OperationalError as e:
        logger.error(f"Database error fetching logs by class and level: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Database error: {str(e)}",
            'timestamp': time.time()
        })
        raise
    except Exception as e:
        logger.error(f"Error fetching logs by class and level: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error fetching logs: {str(e)}",
            'timestamp': time.time()
        })
        raise

@st.cache_data(hash_funcs={str: lambda x: x})
def get_logs_by_service_and_level(job_id: str, service_name: str, level: str, page: int, logs_per_page: int, search_query: str = None, use_regex: bool = False):
    """Retrieve logs by service and level from SQLite, cached."""
    try:
        start_time = time.time()
        conn = sqlite3.connect('data/logs.db', timeout=30)
        cursor = conn.cursor()
        offset = (page - 1) * logs_per_page
        
        # Log query parameters
        logger.debug(f"get_logs_by_service_and_level: job_id={job_id}, service={service_name}, level={level}, page={page}, logs_per_page={logs_per_page}, search_query={search_query}, use_regex={use_regex}")
        
        # Base query
        if level == "ALL":
            query = """
                SELECT timestamp, log_message, level, service
                FROM logs
                WHERE job_id = ? AND service = ?
            """
            params = [job_id, service_name]
        else:
            query = """
                SELECT timestamp, log_message, level, service
                FROM logs
                WHERE job_id = ? AND service = ? AND level = ?
            """
            params = [job_id, service_name, level]
        
        # Add search query if provided
        if search_query and search_query.strip():
            if use_regex:
                query += " AND log_message REGEXP ?"
                params.append(search_query)
            else:
                query += " AND log_message LIKE ?"
                params.append(f'%{search_query}%')
        
        # Add sorting and pagination
        query += " ORDER BY timestamp LIMIT ? OFFSET ?"
        params.extend([logs_per_page, offset])
        
        # Log the exact query
        logger.debug(f"Executing SQL: {query} with params: {params}")
        
        # Execute data query
        cursor.execute(query, params)
        logs = [
            {"timestamp": row[0], "log_message": row[1], "level": row[2], "service": row[3]}
            for row in cursor.fetchall()
        ]
        
        # Count query
        if level == "ALL":
            count_query = """
                SELECT COUNT(*) as total
                FROM logs
                WHERE job_id = ? AND service = ?
            """
            count_params = [job_id, service_name]
        else:
            count_query = """
                SELECT COUNT(*) as total
                FROM logs
                WHERE job_id = ? AND service = ? AND level = ?
            """
            count_params = [job_id, service_name, level]
        
        if search_query and search_query.strip():
            if use_regex:
                count_query += " AND log_message REGEXP ?"
                count_params.append(search_query)
            else:
                count_query += " AND log_message LIKE ?"
                count_params.append(f'%{search_query}%')
        
        # Execute count query
        cursor.execute(count_query, count_params)
        total_logs = cursor.fetchone()[0]
        
        conn.close()
        query_time = time.time() - start_time
        logger.debug(f"Fetched {len(logs)} logs, total_logs={total_logs}, page={page}, query_time={query_time:.2f}s")
        return logs, total_logs
    except sqlite3.OperationalError as e:
        logger.error(f"Database error fetching logs by service and level: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Database error: {str(e)}",
            'timestamp': time.time()
        })
        raise
    except Exception as e:
        logger.error(f"Error fetching logs by service and level: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error fetching logs: {str(e)}",
            'timestamp': time.time()
        })
        raise

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def start_analysis(input_type, folder_path=None, customer_folder=None, start_datetime=None, end_datetime=None):
    """Start a new analysis job via backend API for local folder or S3 bucket."""
    if not st.session_state.backend_available:
        st.session_state.notifications.append({
            'type': 'error',
            'message': "Backend server is not running. Please start `python backend.py`.",
            'timestamp': time.time()
        })
        return

    # Validate inputs based on input type
    if input_type == "Local Folder":
        if not folder_path:
            st.session_state.notifications.append({
                'type': 'error',
                'message': "Please provide a log folder path.",
                'timestamp': time.time()
            })
            return
        payload = {"folder_path": folder_path}
    else:  # S3 Bucket
        # Validate customer folder
        if not customer_folder:
            st.session_state.notifications.append({
                'type': 'error',
                'message': "Please provide a customer folder name.",
                'timestamp': time.time()
            })
            return
        # Validate date-time format (YYYYMMDD-HH)
        datetime_pattern = r'^\d{8}-\d{2}$'
        if not start_datetime or not re.match(datetime_pattern, start_datetime):
            st.session_state.notifications.append({
                'type': 'error',
                'message': "Please provide a valid start date-time in YYYYMMDD-HH format.",
                'timestamp': time.time()
            })
            return
        if not end_datetime or not re.match(datetime_pattern, end_datetime):
            st.session_state.notifications.append({
                'type': 'error',
                'message': "Please provide a valid end date-time in YYYYMMDD-HH format.",
                'timestamp': time.time()
            })
            return
        # Validate date-time range
        try:
            start_dt = datetime.strptime(start_datetime, '%Y%m%d-%H')
            end_dt = datetime.strptime(end_datetime, '%Y%m%d-%H')
            if start_dt > end_dt:
                st.session_state.notifications.append({
                    'type': 'error',
                    'message': "Start date-time must be earlier than or equal to end date-time.",
                    'timestamp': time.time()
                })
                return
        except ValueError as e:
            logger.error(f"Invalid date-time format: {str(e)}")
            st.session_state.notifications.append({
                'type': 'error',
                'message': f"Invalid date-time format: {str(e)}",
                'timestamp': time.time()
            })
            return
        payload = {
            "customer_folder": customer_folder,
            "start_datetime": start_datetime,
            "end_datetime": end_datetime
        }

    try:
        response = requests.post(f"{BACKEND_URL}/jobs/start", json=payload, timeout=10)
        response.raise_for_status()
        job = response.json()
        st.session_state.selected_job_id = job['job_id']
        st.session_state.notifications.append({
            'type': 'success',
            'message': f"Started analysis job: {job['job_id']}",
            'timestamp': time.time()
        })
        logger.info(f"Started analysis job_id: {job['job_id']}, input_type: {input_type}, payload: {payload}")
    except requests.RequestException as e:
        logger.error(f"Error starting analysis: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error starting analysis: {str(e)}",
            'timestamp': time.time()
        })

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def pause_analysis(job_id):
    """Pause an analysis job via backend API."""
    if not st.session_state.backend_available:
        st.session_state.notifications.append({
            'type': 'error',
            'message': "Backend server is not running. Please start `python backend.py`.",
            'timestamp': time.time()
        })
        return
    try:
        response = requests.post(f"{BACKEND_URL}/jobs/{job_id}/pause", timeout=10)
        response.raise_for_status()
        st.session_state.notifications.append({
            'type': 'success',
            'message': f"Paused analysis job: {job_id}",
            'timestamp': time.time()
        })
        logger.info(f"Paused analysis job: {job_id}")
    except requests.RequestException as e:
        logger.error(f"Error pausing analysis: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error pausing analysis: {str(e)}",
            'timestamp': time.time()
        })

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def resume_analysis(job_id):
    """Resume a paused analysis job via backend API."""
    if not st.session_state.backend_available:
        st.session_state.notifications.append({
            'type': 'error',
            'message': "Backend server is not running. Please start `python backend.py`.",
            'timestamp': time.time()
        })
        return
    try:
        response = requests.post(f"{BACKEND_URL}/jobs/{job_id}/resume", timeout=10)
        response.raise_for_status()
        st.session_state.notifications.append({
            'type': 'success',
            'message': f"Resumed analysis job: {job_id}",
            'timestamp': time.time()
        })
        logger.info(f"Resumed analysis job: {job_id}")
    except requests.RequestException as e:
        logger.error(f"Error resuming analysis: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error resuming analysis: {str(e)}",
            'timestamp': time.time()
        })

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def delete_analysis(job_id):
    """Delete an analysis job and its associated data via backend API."""
    if not st.session_state.backend_available:
        st.session_state.notifications.append({
            'type': 'error',
            'message': "Backend server is not running. Please start `python backend.py`.",
            'timestamp': time.time()
        })
        return
    try:
        response = requests.post(f"{BACKEND_URL}/jobs/{job_id}/delete", timeout=10)
        response.raise_for_status()
        st.session_state.selected_job_id = None
        st.session_state.show_dashboard = False
        st.session_state.dashboard_data = None
        st.session_state.notifications.append({
            'type': 'success',
            'message': f"Deleted analysis job: {job_id}",
            'timestamp': time.time()
        })
        logger.info(f"Deleted analysis job: {job_id}")
    except requests.RequestException as e:
        logger.error(f"Error deleting analysis: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error deleting analysis: {str(e)}",
            'timestamp': time.time()
        })

def view_analysis(visualizer):
    """View analysis results for the selected job with progress feedback in main page."""
    try:
        if not st.session_state.selected_job_id:
            st.session_state.notifications.append({
                'type': 'warning',
                'message': "Please select a job to view analysis",
                'timestamp': time.time()
            })
            return
        
        with st.container():
            st.markdown('<div class="card">', unsafe_allow_html=True)
            with st.spinner("Loading analysis data..."):
                progress_bar = st.progress(0)
                status_text = st.empty()
                steps = 4
                step_increment = 1.0 / steps
                
                status_text.text("Fetching timeline data...")
                timeline_data = get_analysis_data(job_id=st.session_state.selected_job_id, query_type='timeline')
                # Sort timeline data by hour
                if not timeline_data.empty:
                    timeline_data['hour'] = pd.to_datetime(timeline_data['hour'])
                    timeline_data = timeline_data.sort_values('hour')
                progress_bar.progress(0.25)
                
                status_text.text("Fetching class-level counts...")
                level_counts_by_class = get_analysis_data(job_id=st.session_state.selected_job_id, query_type='class')
                # Pivot class data: class as index, levels as columns
                if not level_counts_by_class.empty:
                    class_pivot = level_counts_by_class.pivot(index='class', columns='level', values='count').fillna(0)
                    # Ensure all log levels are present as columns
                    config = load_config()
                    log_levels = config['app']['log_levels']
                    for level in log_levels:
                        if level not in class_pivot.columns:
                            class_pivot[level] = 0
                    class_pivot = class_pivot.reset_index()
                else:
                    class_pivot = pd.DataFrame(columns=['class'] + log_levels)
                progress_bar.progress(0.50)
                
                status_text.text("Fetching service-level counts...")
                level_counts_by_service = get_analysis_data(job_id=st.session_state.selected_job_id, query_type='service')
                # Pivot service data: service as index, levels as columns
                if not level_counts_by_service.empty:
                    service_pivot = level_counts_by_service.pivot(index='service', columns='level', values='count').fillna(0)
                    # Ensure all log levels are present as columns
                    for level in log_levels:
                        if level not in service_pivot.columns:
                            service_pivot[level] = 0
                    service_pivot = service_pivot.reset_index()
                else:
                    service_pivot = pd.DataFrame(columns=['service'] + log_levels)
                progress_bar.progress(0.75)
                
                status_text.text("Fetching class and service totals...")
                # Calculate total counts for class and service bar/pie charts
                class_totals = level_counts_by_class.groupby('class')['count'].sum().reset_index()
                service_totals = level_counts_by_service.groupby('service')['count'].sum().reset_index()
                progress_bar.progress(1.0)
                
                if all(df.empty for df in [timeline_data, level_counts_by_class, level_counts_by_service, class_totals, service_totals]):
                    st.session_state.notifications.append({
                        'type': 'warning',
                        'message': "No analysis data available for this job",
                        'timestamp': time.time()
                    })
                    progress_bar.empty()
                    status_text.empty()
                    st.markdown('</div>', unsafe_allow_html=True)
                    return
                
                st.session_state.dashboard_data = {
                    'timeline_data': timeline_data,
                    'class_pivot': class_pivot,
                    'service_pivot': service_pivot,
                    'class_totals': class_totals,
                    'service_totals': service_totals
                }
                
                st.session_state.show_dashboard = True
                
                st.session_state.notifications.append({
                    'type': 'success',
                    'message': "Analysis data loaded successfully",
                    'timestamp': time.time()
                })
                
                progress_bar.empty()
                status_text.empty()
            st.markdown('</div>', unsafe_allow_html=True)
            
    except Exception as e:
        logger.error(f"Error viewing analysis: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error viewing analysis: {str(e)}",
            'timestamp': time.time()
        })

def download_results(job_id):
    """Download analysis results as Excel."""
    try:
        with st.spinner("Generating Excel file..."):
            excel_file = export_to_excel(job_id)
            with open(excel_file, 'rb') as f:
                st.download_button(
                    label="Download Excel",
                    data=f,
                    file_name=f"analysis_results_{job_id}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            st.session_state.notifications.append({
                'type': 'success',
                'message': "Excel file generated successfully",
                'timestamp': time.time()
            })
    except FileNotFoundError:
        logger.error(f"Excel file not found for job_id: {job_id}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': "Excel file could not be generated",
            'timestamp': time.time()
        })
    except Exception as e:
        logger.error(f"Error downloading results: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error downloading results: {str(e)}",
            'timestamp': time.time()
        })

def process_csv_files(uploaded_files):
    """Process uploaded CSV files."""
    csv_data = {}
    for file in uploaded_files:
        try:
            df = pd.read_csv(file)
            file_name = file.name.lower().replace('.csv', '')
            if file_name in [
                'class_level_counts', 'level_summary', 'class_summary', 'pod_summary',
                'container_summary', 'host_summary', 'class_level_pod', 'hourly_level_counts',
                'thread_summary', 'error_analysis', 'time_range'
            ]:
                csv_data[file_name] = df
            else:
                st.session_state.csv_notifications.append({
                    'type': 'warning',
                    'message': f"Unsupported CSV file: {file.name}",
                    'timestamp': time.time()
                })
        except Exception as e:
            logger.error(f"Error processing CSV {file.name}: {str(e)}")
            st.session_state.csv_notifications.append({
                'type': 'error',
                'message': f"Error processing CSV {file.name}: {str(e)}",
                'timestamp': time.time()
            })
    return csv_data

def display_notifications():
    """Display notifications with 5-second auto-expiry."""
    current_time = time.time()
    
    # Clear expired notifications
    st.session_state.notifications = [n for n in st.session_state.notifications if current_time - n['timestamp'] < 5]
    
    # Render notifications in a single container
    notification_container = st.empty()
    with notification_container.container():
        for i, notification in enumerate(st.session_state.notifications):
            logger.debug(f"Displaying notification {i}: {notification['message']}")
            if notification['type'] == 'success':
                st.markdown(
                    f'<div class="notification-success" key="notification_{i}_{notification["timestamp"]}">✅ {notification["message"]}</div>',
                    unsafe_allow_html=True
                )
            elif notification['type'] == 'error':
                st.markdown(
                    f'<div class="notification-error" key="notification_{i}_{notification["timestamp"]}">❌ {notification["message"]}</div>',
                    unsafe_allow_html=True
                )
            elif notification['type'] == 'warning':
                st.markdown(
                    f'<div class="notification-warning" key="notification_{i}_{notification["timestamp"]}">⚠️ {notification["message"]}</div>',
                    unsafe_allow_html=True
                )
    
    # Force clear after 5 seconds
    if st.session_state.notifications and current_time - st.session_state.last_notification_clear >= 5:
        st.session_state.notifications = []
        st.session_state.last_notification_clear = current_time
        notification_container.empty()
        logger.debug("Cleared notification container")
        st.experimental_rerun()

def display_csv_notifications():
    """Display CSV-specific notifications with 5-second auto-expiry."""
    current_time = time.time()
    
    # Clear expired notifications
    st.session_state.csv_notifications = [n for n in st.session_state.csv_notifications if current_time - n['timestamp'] < 5]
    
    # Render notifications in a single container
    csv_notification_container = st.empty()
    with csv_notification_container.container():
        for i, notification in enumerate(st.session_state.csv_notifications):
            logger.debug(f"Displaying CSV notification {i}: {notification['message']}")
            if notification['type'] == 'success':
                st.markdown(
                    f'<div class="notification-success" key="csv_notification_{i}_{notification["timestamp"]}">✅ {notification["message"]}</div>',
                    unsafe_allow_html=True
                )
            elif notification['type'] == 'error':
                st.markdown(
                    f'<div class="notification-error" key="csv_notification_{i}_{notification["timestamp"]}">❌ {notification["message"]}</div>',
                    unsafe_allow_html=True
                )
            elif notification['type'] == 'warning':
                st.markdown(
                    f'<div class="notification-warning" key="csv_notification_{i}_{notification["timestamp"]}">⚠️ {notification["message"]}</div>',
                    unsafe_allow_html=True
                )
    
    # Force clear after 5 seconds
    if st.session_state.csv_notifications and current_time - st.session_state.last_notification_clear >= 5:
        st.session_state.csv_notifications = []
        st.session_state.last_notification_clear = current_time
        csv_notification_container.empty()
        logger.debug("Cleared CSV notification container")
        st.experimental_rerun()

def list_s3_customer_folders():
    """List customer folders in s3://k8-customer-logs using boto3."""
    try:
        logger.info("Listing customer folders in s3://k8-customer-logs using boto3")
        # Initialize boto3 S3 client
        s3_client = boto3.client('s3')
        bucket_name = 'k8-customer-logs'
        
        # List top-level prefixes (folders) in the bucket
        folders = []
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=bucket_name,
            Delimiter='/',
            PaginationConfig={'PageSize': 1000}
        )
        
        for page in page_iterator:
            prefixes = page.get('CommonPrefixes', [])
            for prefix in prefixes:
                folder_name = prefix.get('Prefix', '').rstrip('/')
                if folder_name:
                    folders.append(folder_name)
        
        if not folders:
            logger.warning("No customer folders found in s3://k8-customer-logs")
            st.session_state.notifications.append({
                'type': 'warning',
                'message': "No customer folders found in the S3 bucket.",
                'timestamp': time.time()
            })
            return []
        
        logger.info(f"Successfully retrieved {len(folders)} customer folders from S3")
        st.session_state.notifications.append({
            'type': 'success',
            'message': f"Successfully retrieved {len(folders)} customer folders.",
            'timestamp': time.time()
        })
        return sorted(folders)  # Sort for consistent display
    
    except NoCredentialsError:
        logger.error("AWS credentials not found or invalid")
        st.session_state.notifications.append({
            'type': 'error',
            'message': "AWS credentials not found. Please configure AWS credentials.",
            'timestamp': time.time()
        })
        return []
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"S3 ClientError: {error_code} - {error_message}")
        if error_code == 'AccessDenied':
            st.session_state.notifications.append({
                'type': 'error',
                'message': "Permission denied accessing S3 bucket. Check IAM role permissions.",
                'timestamp': time.time()
            })
        elif error_code == 'NoSuchBucket':
            st.session_state.notifications.append({
                'type': 'error',
                'message': "S3 bucket 'k8-customer-logs' not found.",
                'timestamp': time.time()
            })
        else:
            st.session_state.notifications.append({
                'type': 'error',
                'message': f"Failed to list customer folders: {error_message}",
                'timestamp': time.time()
            })
        return []
    except Exception as e:
        logger.error(f"Unexpected error listing S3 customer folders: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Unexpected error listing customer folders: {str(e)}",
            'timestamp': time.time()
        })
        return []

def get_job_date_range(job_id: str, folder_path: str) -> tuple:
    """Fetch or compute the date range for a job based on job type."""
    try:
        if folder_path.startswith('s3://'):
            # S3 job: Fetch from job_metadata
            conn = sqlite3.connect('data/logs.db', timeout=30)
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT type, value FROM job_metadata
                WHERE job_id = ? AND type IN ('start_datetime', 'end_datetime')
                """,
                (job_id,)
            )
            metadata = {row[0]: row[1] for row in cursor.fetchall()}
            conn.close()
            
            start_datetime = metadata.get('start_datetime')
            end_datetime = metadata.get('end_datetime')
            
            if start_datetime and end_datetime:
                try:
                    start_dt = datetime.strptime(start_datetime, '%Y%m%d-%H')
                    end_dt = datetime.strptime(end_datetime, '%Y%m%d-%H')
                    return (
                        start_dt.strftime('%Y-%m-%d %H:00'),
                        end_dt.strftime('%Y-%m-%d %H:00')
                    )
                except ValueError as e:
                    logger.error(f"Error parsing S3 datetime for job_id {job_id}: {str(e)}")
                    st.session_state.notifications.append({
                        'type': 'error',
                        'message': f"Invalid date format in metadata: {str(e)}",
                        'timestamp': time.time()
                    })
            return "N/A", "N/A"
        else:
            # Local job: Scan subfolders for YYYYMMDD-HH pattern
            if not os.path.isdir(folder_path):
                logger.warning(f"Folder path {folder_path} is not a valid directory for job_id {job_id}")
                st.session_state.notifications.append({
                    'type': 'warning',
                    'message': f"Invalid folder path for job {job_id}",
                    'timestamp': time.time()
                })
                return "N/A", "N/A"
            
            date_pattern = r'^\d{8}-\d{2}$'
            dates = []
            
            # Iterate over subfolders in the folder_path
            for subfolder in os.listdir(folder_path):
                subfolder_path = os.path.join(folder_path, subfolder)
                if os.path.isdir(subfolder_path) and re.match(date_pattern, subfolder):
                    try:
                        dt = datetime.strptime(subfolder, '%Y%m%d-%H')
                        dates.append(dt)
                    except ValueError:
                        logger.debug(f"Skipping invalid folder name {subfolder} in {folder_path}")
                        continue
            
            if not dates:
                logger.warning(f"No valid YYYYMMDD-HH folders found in {folder_path} for job_id {job_id}")
                st.session_state.notifications.append({
                    'type': 'warning',
                    'message': f"No valid date-time folders found in {folder_path}",
                    'timestamp': time.time()
                })
                return "N/A", "N/A"
            
            start_dt = min(dates)
            end_dt = max(dates)
            return (
                start_dt.strftime('%Y-%m-%d %H:00'),
                end_dt.strftime('%Y-%m-%d %H:00')
            )
    except Exception as e:
        logger.error(f"Error getting date range for job_id {job_id}: {str(e)}")
        st.session_state.notifications.append({
            'type': 'error',
            'message': f"Error getting date range for job {job_id}: {str(e)}",
            'timestamp': time.time()
        })
        return "N/A", "N/A"

def update_selected_job_id():
    """Update selected job ID in session state for Log Analysis tab."""
    selected_job = st.session_state.job_select
    if selected_job != 'Select a job...':
        st.session_state.selected_job_id = selected_job
        st.session_state.show_dashboard = False
    else:
        st.session_state.selected_job_id = None
        st.session_state.show_dashboard = False

def update_log_viewer_job_id():
    """Update selected job ID for Log Viewer tab and manage cache."""
    selected_job = st.session_state.log_viewer_job_select
    if selected_job != 'Select a job...':
        if st.session_state.log_viewer_job_id != selected_job:
            st.session_state.log_viewer_job_id = selected_job
            if st.session_state.log_viewer_last_job_id != selected_job:
                get_job_metadata.clear()
                get_logs_by_class_and_level.clear()
                get_logs_by_service_and_level.clear()
                st.session_state.cached_job_id = selected_job
                st.session_state.log_viewer_last_job_id = selected_job
                logger.info(f"Cleared cache for new job_id: {selected_job}")
            # Reset pagination
            st.session_state.log_viewer_current_page = 1
            st.session_state.log_viewer_total_pages = 1
            st.session_state.log_viewer_logs = []
            st.session_state.log_viewer_total_logs = 0
    else:
        st.session_state.log_viewer_job_id = None
        get_job_metadata.clear()
        get_logs_by_class_and_level.clear()
        get_logs_by_service_and_level.clear()
        st.session_state.cached_job_id = None
        st.session_state.log_viewer_last_job_id = None
        st.session_state.log_viewer_current_page = 1
        st.session_state.log_viewer_total_pages = 1
        st.session_state.log_viewer_logs = []
        st.session_state.log_viewer_total_logs = 0

def main():
    """Main Streamlit application."""
    st.set_page_config(page_title="Saviynt Log Analyzer", layout="wide", initial_sidebar_state="expanded")
    
    os.makedirs('data', exist_ok=True)
    initialize_session_state()
    
    if not st.session_state.db_initialized:
        try:
            init_db()
            st.session_state.db_initialized = True
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            st.error(f"Failed to initialize database: {str(e)}")
            return
    
    apply_custom_css()
    check_backend_health()

    st.markdown(
    """
    <div class="header">
        <h1 style="color: rgb(255, 75, 75);">Saviynt Log Analyzer v3.0</h1>
        <p>Unleash the Power of Log Analytics with Unmatched Precision</p>
    </div>
    """,
    unsafe_allow_html=True
)

    tab1, tab2, tab3, tab4 = st.tabs(["📊 Log Analysis", "🔍 Log Viewer", "📈 CSV Visualization", "📂 Customer Folders"])

    with tab1:
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        st.header("Log Analysis")
        config = load_config()
        visualizer = Visualizer(config)

        job_status_df = get_job_status()
        job_options = ['Select a job...']
        if not job_status_df.empty and 'job_id' in job_status_df.columns:
            job_options += job_status_df['job_id'].tolist()
        else:
            st.info("No jobs available. Start a new analysis to create a job.")

        col1, col2 = st.columns([3, 1])
        with col1:
            st.selectbox(
                "Select JOBID",
                options=job_options,
                key="job_select",
                on_change=update_selected_job_id,
                help="Choose a job to view its analysis results"
            )
        with col2:
            st.markdown('<div class="tooltip">', unsafe_allow_html=True)
            if st.button("Clear Cache", key="clear_cache"):
                st.cache_data.clear()
                st.cache_resource.clear()
                st.session_state.notifications.append({
                    'type': 'success',
                    'message': "Cache cleared successfully",
                    'timestamp': time.time()
                })
            st.markdown('<span class="tooltiptext">Clears cached data to refresh the application</span></div>', unsafe_allow_html=True)

        with st.container():
            if st.session_state.selected_job_id and not job_status_df.empty:
                job_info = job_status_df[job_status_df['job_id'] == st.session_state.selected_job_id].iloc[0]
                # Get date range for the job
                start_date_hour, end_date_hour = get_job_date_range(job_info['job_id'], job_info.get('folder_path', 'N/A'))
                st.markdown(
                    f"""
                    <div class="card">
                        <h3 class="text-lg font-semibold text-gray-800">Job Details</h3>
                        <p><strong>Job ID:</strong> {st.session_state.selected_job_id}</p>
                        <p><strong>Folder Path:</strong> {job_info.get('folder_path', 'N/A')}</p>
                        <p><strong>Status:</strong> {job_info.get('status', 'N/A')}</p>
                        <p><strong>Files Processed:</strong> {job_info.get('files_processed', 0)} / {job_info.get('total_files', 0)}</p>
                        <p><strong>Start Time:</strong> {job_info.get('start_time', 'N/A')}</p>
                        <p><strong>Last Updated:</strong> {job_info.get('last_updated', 'N/A')}</p>
                        <p><strong>Start Date and Hour:</strong> {start_date_hour}</p>
                        <p><strong>End Date and Hour:</strong> {end_date_hour}</p>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

            if st.session_state.show_dashboard and st.session_state.dashboard_data:
                st.markdown('<div class="card">', unsafe_allow_html=True)
                visualizer.display_dashboard(
                    st.session_state.dashboard_data['timeline_data'],
                    st.session_state.dashboard_data['class_pivot'],
                    st.session_state.dashboard_data['service_pivot'],
                    st.session_state.dashboard_data['class_totals'],
                    st.session_state.dashboard_data['service_totals']
                )
                st.markdown('</div>', unsafe_allow_html=True)

        with st.sidebar:
            st.markdown('<div class="sidebar-content">', unsafe_allow_html=True)
            st.header("Analysis Controls")

            # Input type selection
            input_type = st.radio(
                "Select Input Type",
                ["Local Folder", "S3 Bucket"],
                key="input_type",
                help="Choose whether to process logs from a local folder or stream from an S3 bucket."
            )

            if input_type == "Local Folder":
                folder_path = st.text_input(
                    "Log Folder Path",
                    placeholder="e.g., data/customer_logs",
                    key="folder_path",
                    help="Enter the path to the folder containing .gz log files."
                )
                customer_folder = start_datetime = end_datetime = None
            else:  # S3 Bucket
                folder_path = None
                customer_folder = st.text_input(
                    "Customer Folder Name",
                    placeholder="e.g., customer1",
                    key="customer_folder",
                    help="Enter the customer folder name from the S3 bucket (see Customer Folders tab)."
                )
                start_datetime = st.text_input(
                    "Start Date-Time (YYYYMMDD-HH)",
                    placeholder="e.g., 20250101-00",
                    key="start_datetime",
                    help="Enter the start date and hour in YYYYMMDD-HH format."
                )
                end_datetime = st.text_input(
                    "End Date-Time (YYYYMMDD-HH)",
                    placeholder="e.g., 20250101-23",
                    key="end_datetime",
                    help="Enter the end date and hour in YYYYMMDD-HH format."
                )

            st.markdown('<div class="tooltip">', unsafe_allow_html=True)
            if st.button("Start Analysis", key="start_analysis"):
                if st.session_state.backend_available:
                    start_analysis(input_type, folder_path, customer_folder, start_datetime, end_datetime)
                else:
                    st.session_state.notifications.append({
                        'type': 'error',
                        'message': "Cannot start analysis: Backend server is not running. Please start `python backend.py`.",
                        'timestamp': time.time()
                    })
            st.markdown('<span class="tooltiptext">Starts a new analysis job for the specified folder or S3 bucket</span></div>', unsafe_allow_html=True)

            if st.session_state.selected_job_id:
                st.markdown('<div class="tooltip">', unsafe_allow_html=True)
                if st.button("Pause Analysis", key="pause_analysis"):
                    if st.session_state.backend_available:
                        pause_analysis(st.session_state.selected_job_id)
                    else:
                        st.session_state.notifications.append({
                            'type': 'error',
                            'message': "Cannot pause analysis: Backend server is not running. Please start `python backend.py`.",
                            'timestamp': time.time()
                        })
                st.markdown('<span class="tooltiptext">Pauses the selected analysis job</span></div>', unsafe_allow_html=True)
                
                st.markdown('<div class="tooltip">', unsafe_allow_html=True)
                if st.button("Resume Analysis", key="resume_analysis"):
                    if st.session_state.backend_available:
                        resume_analysis(st.session_state.selected_job_id)
                    else:
                        st.session_state.notifications.append({
                            'type': 'error',
                            'message': "Cannot resume analysis: Backend server is not running. Please start `python backend.py`.",
                            'timestamp': time.time()
                        })
                st.markdown('<span class="tooltiptext">Resumes a paused analysis job</span></div>', unsafe_allow_html=True)
                
                st.markdown('<div class="tooltip">', unsafe_allow_html=True)
                if st.button("View Analysis", key="view_analysis"):
                    view_analysis(visualizer)
                st.markdown('<span class="tooltiptext">Displays analysis results for the selected job</span></div>', unsafe_allow_html=True)
                
                st.markdown('<div class="tooltip">', unsafe_allow_html=True)
                if st.button("Download Results", key="download_results"):
                    download_results(st.session_state.selected_job_id)
                st.markdown('<span class="tooltiptext">Downloads analysis results as an Excel file</span></div>', unsafe_allow_html=True)
                
                st.markdown('<div class="tooltip">', unsafe_allow_html=True)
                if st.button("Delete Analysis", key="delete_analysis"):
                    if st.session_state.backend_available:
                        delete_analysis(st.session_state.selected_job_id)
                    else:
                        st.session_state.notifications.append({
                            'type': 'error',
                            'message': "Cannot delete analysis: Backend server is not running. Please start `python backend.py`.",
                            'timestamp': time.time()
                        })
                st.markdown('<span class="tooltiptext">Deletes the selected analysis job and all associated data</span></div>', unsafe_allow_html=True)
            
            if st.button("Check Backend Status", key="check_backend_status"):
                check_backend_health()
            
            st.markdown('</div>', unsafe_allow_html=True)

        display_notifications()
        st.markdown('</div>', unsafe_allow_html=True)

    with tab2:
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        st.header("Log Viewer")
        
        job_status_df = get_job_status()
        job_options = ['Select a job...']
        if not job_status_df.empty and 'job_id' in job_status_df.columns:
            job_options += job_status_df['job_id'].tolist()
        else:
            st.info("No jobs available. Start a new analysis in the Log Analysis tab to create a job.")
        
        st.selectbox(
            "Select JOBID",
            options=job_options,
            key="log_viewer_job_select",
            on_change=update_log_viewer_job_id,
            help="Choose a job to view its logs"
        )

        if st.session_state.log_viewer_job_id:
            config = load_config()
            with st.spinner("Loading log viewer data..."):
                classes, services = get_job_metadata(st.session_state.log_viewer_job_id)
                class_options = ['None'] + classes if classes else ['None']
                service_options = ['None'] + services if services else ['None']
                
                log_level = st.selectbox(
                    "Select Log Level",
                    config['app']['log_levels'] + ['ALL'],
                    key="log_level_viewer",
                    help="Choose a log level to filter logs, or select ALL to view logs across all levels"
                )
                col1, col2 = st.columns(2)
                with col1:
                    selected_class = st.selectbox(
                        "Select Class",
                        class_options,
                        key="class_viewer",
                        help="Select a class to view its logs"
                    )
                with col2:
                    selected_service = st.selectbox(
                        "Select Service",
                        service_options,
                        key="service_viewer",
                        help="Select a service to view its logs"
                    )
                
                search_query = st.text_input(
                    "Search Logs",
                    placeholder="Enter search term",
                    key="search_viewer",
                    help="Search logs by message content"
                )
                use_regex = st.checkbox("Use Regex", key="regex_viewer", help="Enable regex for search queries")
                
                logs_per_page = 100000
                page = st.number_input(
                    "Page",
                    min_value=1,
                    max_value=max(1, st.session_state.log_viewer_total_pages),
                    value=st.session_state.log_viewer_current_page,
                    step=1,
                    key="page_viewer",
                    help="Select page for paginated results"
                )
                
                # Log selected class or service for debugging
                if selected_class != 'None':
                    logger.debug(f"Selected class: {selected_class} for job_id: {st.session_state.log_viewer_job_id}")
                if selected_service != 'None':
                    logger.debug(f"Selected service: {selected_service} for job_id: {st.session_state.log_viewer_job_id}")
                
                if st.button("Fetch Logs", key="fetch_logs"):
                    if selected_class == 'None' and selected_service == 'None':
                        st.session_state.notifications.append({
                            'type': 'error',
                            'message': "Please select a class or service",
                            'timestamp': time.time()
                        })
                    elif log_level not in config['app']['log_levels'] + ['ALL']:
                        st.session_state.notifications.append({
                            'type': 'error',
                            'message': "Please select a valid log level",
                            'timestamp': time.time()
                        })
                    else:
                        with st.spinner("Fetching logs..."):
                            try:
                                st.session_state.log_viewer_current_page = page
                                logs, total_logs = (get_logs_by_class_and_level if selected_class != 'None' else get_logs_by_service_and_level)(
                                    st.session_state.log_viewer_job_id,
                                    selected_class if selected_class != 'None' else selected_service,
                                    log_level,
                                    page,
                                    logs_per_page,
                                    search_query,
                                    use_regex
                                )
                                st.session_state.log_viewer_logs = logs
                                st.session_state.log_viewer_total_logs = total_logs
                                st.session_state.log_viewer_total_pages = max(1, (total_logs + logs_per_page - 1) // logs_per_page)
                                
                                if logs:
                                    st.dataframe(pd.DataFrame(logs), use_container_width=True)
                                    st.markdown(f"**Total Logs:** {total_logs} | **Page:** {page} of {st.session_state.log_viewer_total_pages}")
                                    st.download_button(
                                        label="Download Logs as JSON",
                                        data=json.dumps(logs, indent=2),
                                        file_name=f"{log_level}_logs_page_{page}.json",
                                        mime="application/json",
                                        key=f"download_viewer_{page}"
                                    )
                                    st.session_state.notifications.append({
                                        'type': 'success',
                                        'message': f"Loaded {len(logs)} logs for page {page}",
                                        'timestamp': time.time()
                                    })
                                else:
                                    st.info("No logs found for the selected criteria")
                                    logger.warning(f"No logs found for job_id: {st.session_state.log_viewer_job_id}, "
                                                  f"class: {selected_class}, service: {selected_service}, level: {log_level}")
                                    # Clear cache to prevent stale results
                                    get_logs_by_class_and_level.clear()
                                    get_logs_by_service_and_level.clear()
                                    st.session_state.log_viewer_logs = []
                                    st.session_state.log_viewer_total_logs = 0
                                    st.session_state.log_viewer_total_pages = 1
                                    st.session_state.notifications.append({
                                        'type': 'warning',
                                        'message': f"No logs found. Cache cleared. Try selecting a different class or service.",
                                        'timestamp': time.time()
                                    })
                            except Exception as e:
                                st.session_state.notifications.append({
                                    'type': 'error',
                                    'message': f"Failed to fetch logs: {str(e)}",
                                    'timestamp': time.time()
                                })
                                st.session_state.log_viewer_logs = []
                                st.session_state.log_viewer_total_logs = 0
                
                # Display current logs if available
                if st.session_state.log_viewer_logs:
                    st.dataframe(pd.DataFrame(st.session_state.log_viewer_logs), use_container_width=True)
                    st.markdown(f"**Total Logs:** {st.session_state.log_viewer_total_logs} | **Page:** {st.session_state.log_viewer_current_page} of {st.session_state.log_viewer_total_pages}")
                    st.download_button(
                        label="Download Logs as JSON",
                        data=json.dumps(st.session_state.log_viewer_logs, indent=2),
                        file_name=f"{log_level}_logs_page_{st.session_state.log_viewer_current_page}.json",
                        mime="application/json",
                        key=f"download_viewer_persistent_{st.session_state.log_viewer_current_page}"
                    )
        else:
            st.info("Please select a job to view logs")
        
        display_notifications()
        st.markdown('</div>', unsafe_allow_html=True)

    with tab3:
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        st.header("CSV Visualization")
        uploaded_files = st.file_uploader(
            "Upload CSV Files",
            accept_multiple_files=True,
            type=['csv'],
            help="Upload CSV files for visualization"
        )
        if uploaded_files:
            with st.spinner("Processing CSV files..."):
                csv_data = process_csv_files(uploaded_files)
                visualizer = Visualizer(load_config())
                visualizer.display_csv_dashboard(csv_data)
                display_csv_notifications()
        st.markdown('</div>', unsafe_allow_html=True)

    with tab4:
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        st.header("Customer Folders")
        
        # Button to list S3 folders
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="tooltip">', unsafe_allow_html=True)
        if st.button("List Customer Folders", key="list_s3_folders"):
            with st.spinner("Fetching customer folders from S3..."):
                folders = list_s3_customer_folders()
                # Store folders in session state for display
                st.session_state.customer_folders = folders
                # Reset pagination
                st.session_state.customer_folders_page = 1
        st.markdown('<span class="tooltiptext">Lists all customer folders in the S3 bucket s3://k8-customer-logs</span></div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Display folders if available
        if 'customer_folders' in st.session_state and st.session_state.customer_folders:
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.subheader(f"Customer Folders ({len(st.session_state.customer_folders)})")
            
            # Pagination controls
            total_folders = len(st.session_state.customer_folders)
            folders_per_page = st.session_state.customer_folders_per_page
            total_pages = max(1, (total_folders + folders_per_page - 1) // folders_per_page)
            
            col1, col2, col3 = st.columns([2, 3, 2])
            with col1:
                if st.button("Previous Page", key="prev_page_folders", disabled=st.session_state.customer_folders_page == 1):
                    st.session_state.customer_folders_page = max(1, st.session_state.customer_folders_page - 1)
            with col2:
                st.markdown(f"<p style='text-align: center; margin-top: 8px;'>Page {st.session_state.customer_folders_page} of {total_pages}</p>", unsafe_allow_html=True)
            with col3:
                if st.button("Next Page", key="next_page_folders", disabled=st.session_state.customer_folders_page == total_pages):
                    st.session_state.customer_folders_page = min(total_pages, st.session_state.customer_folders_page + 1)
            
            # Calculate folder range for current page
            start_idx = (st.session_state.customer_folders_page - 1) * folders_per_page
            end_idx = min(start_idx + folders_per_page, total_folders)
            paginated_folders = st.session_state.customer_folders[start_idx:end_idx]
            
            # Create a responsive grid of folder cards
            st.markdown('<div class="folder-grid">', unsafe_allow_html=True)
            for folder in paginated_folders:
                # Sanitize folder name to avoid HTML injection
                safe_folder = html.escape(folder)
                # Use folder name as key to ensure uniqueness
                st.markdown(
                    f"""
                    <div class="folder-card" key="folder_{safe_folder}">
                        <h3>{safe_folder}</h3>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            st.markdown('</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.info("Click 'List Customer Folders' to retrieve the list of customer folders from S3.")
        
        display_notifications()
        st.markdown('</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main()