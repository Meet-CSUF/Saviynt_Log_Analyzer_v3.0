import streamlit as st
import plotly.express as px
import pandas as pd
import logging
from typing import Dict

# Configure logging
logging.basicConfig(
    filename='log_analyzer.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Visualizer:
    """Handles visualization of log analysis data."""
    
    def __init__(self, config: Dict):
        """Initialize Visualizer with configuration."""
        self.config = config
        self.log_levels = config.get('app', {}).get('log_levels', [])
        logger.info("Visualizer initialized with config")

    def display_dashboard(self, timeline_data: pd.DataFrame, class_pivot: pd.DataFrame,
                         service_pivot: pd.DataFrame, class_totals: pd.DataFrame,
                         service_totals: pd.DataFrame):
        """Display the main dashboard with analysis visualizations."""
        try:
            st.subheader("Analysis Dashboard")
            
            # Timeline Data
            if not timeline_data.empty:
                st.markdown("### Log Counts Over Time")
                # Ensure hour is in datetime format
                timeline_data['hour'] = pd.to_datetime(timeline_data['hour'], errors='coerce')
                if timeline_data['hour'].isna().any():
                    logger.warning("Some timeline entries have invalid datetime values")
                    st.session_state.notifications.append({
                        'type': 'warning',
                        'message': "Some timeline entries have invalid datetime values and were excluded",
                        'timestamp': time.time()
                    })
                    timeline_data = timeline_data.dropna(subset=['hour'])
                
                if not timeline_data.empty:
                    fig_timeline = px.line(
                        timeline_data,
                        x='hour',
                        y='count',
                        color='level',
                        title="Log Counts by Hour",
                        labels={'hour': 'Time', 'count': 'Count', 'level': 'Log Level'},
                        color_discrete_sequence=px.colors.qualitative.Plotly
                    )
                    fig_timeline.update_layout(
                        xaxis_title="Time",
                        yaxis_title="Count",
                        legend_title="Log Level",
                        xaxis_tickformat="%Y-%m-%d %H:%M",
                        xaxis=dict(
                            tickmode='auto',
                            nticks=20,
                            rangeslider_visible=True,
                            showgrid=True,
                            gridcolor='rgba(200, 200, 200, 0.5)'
                        ),
                        yaxis=dict(
                            showgrid=True,
                            gridcolor='rgba(200, 200, 200, 0.5)'
                        ),
                        height=600,
                        margin=dict(b=150)
                    )
                    st.plotly_chart(fig_timeline, use_container_width=True)
                else:
                    st.info("No valid timeline data available for plotting")
                    logger.info("No valid timeline data after filtering")
            else:
                st.info("No timeline data available")
                logger.info("Timeline data is empty")
            
            # Log Level Counts by Class
            if not class_pivot.empty:
                st.markdown("### Log Level Counts by Class")
                st.dataframe(class_pivot, use_container_width=True)
            
            # Log Level Counts by Service
            if not service_pivot.empty:
                st.markdown("### Log Level Counts by Service")
                st.dataframe(service_pivot, use_container_width=True)
            
            # Class Distribution Bar Plot (Stacked by Log Level)
            if not class_pivot.empty:
                st.markdown("### Class Distribution by Log Level")
                # Melt the pivot table to long format for stacked bar plot
                class_melted = class_pivot.melt(id_vars=['class'], value_vars=self.log_levels,
                                               var_name='level', value_name='count')
                fig_class_bar = px.bar(
                    class_melted,
                    x='class',
                    y='count',
                    color='level',
                    barmode='stack',
                    title="Log Counts by Class and Level",
                    labels={'class': 'Class', 'count': 'Count', 'level': 'Log Level'},
                    color_discrete_sequence=px.colors.qualitative.Plotly
                )
                fig_class_bar.update_layout(
                    xaxis_title="Class",
                    yaxis_title="Count",
                    legend_title="Log Level",
                    xaxis_tickangle=45,
                    height=600,
                    margin=dict(b=150),
                    yaxis=dict(
                        showgrid=True,
                        gridcolor='rgba(200, 200, 200, 0.5)'
                    )
                )
                st.plotly_chart(fig_class_bar, use_container_width=True)
                logger.info("Displayed stacked bar plot for class distribution")
                
                # Class Pie Chart (unchanged)
                st.markdown("### Class Distribution (Pie)")
                fig_class_pie = px.pie(
                    class_totals,
                    names='class',
                    values='count',
                    title="Log Distribution by Class",
                    color_discrete_sequence=px.colors.qualitative.Plotly
                )
                st.plotly_chart(fig_class_pie, use_container_width=True)
            
            # Service Distribution Bar Plot (Stacked by Log Level)
            if not service_pivot.empty:
                st.markdown("### Service Distribution by Log Level")
                # Melt the pivot table to long format for stacked bar plot
                service_melted = service_pivot.melt(id_vars=['service'], value_vars=self.log_levels,
                                                  var_name='level', value_name='count')
                fig_service_bar = px.bar(
                    service_melted,
                    x='service',
                    y='count',
                    color='level',
                    barmode='stack',
                    title="Log Counts by Service and Level",
                    labels={'service': 'Service', 'count': 'Count', 'level': 'Log Level'},
                    color_discrete_sequence=px.colors.qualitative.Plotly
                )
                fig_service_bar.update_layout(
                    xaxis_title="Service",
                    yaxis_title="Count",
                    legend_title="Log Level",
                    xaxis_tickangle=45,
                    height=600,
                    margin=dict(b=150),
                    yaxis=dict(
                        showgrid=True,
                        gridcolor='rgba(200, 200, 200, 0.5)'
                    )
                )
                st.plotly_chart(fig_service_bar, use_container_width=True)
                logger.info("Displayed stacked bar plot for service distribution")
                
                # Service Pie Chart (unchanged)
                st.markdown("### Service Distribution (Pie)")
                fig_service_pie = px.pie(
                    service_totals,
                    names='service',
                    values='count',
                    title="Log Distribution by Service",
                    color_discrete_sequence=px.colors.qualitative.Plotly
                )
                st.plotly_chart(fig_service_pie, use_container_width=True)
            
            logger.info("Dashboard displayed successfully")
        except Exception as e:
            logger.error(f"Error displaying dashboard: {str(e)}")
            st.session_state.notifications.append({
                'type': 'error',
                'message': f"Error displaying dashboard: {str(e)}",
                'timestamp': time.time()
            })

    def display_csv_dashboard(self, csv_data: Dict[str, pd.DataFrame]):
        """Display dashboard for uploaded CSV files."""
        try:
            st.subheader("CSV Analysis Dashboard")
            
            for file_name, df in csv_data.items():
                st.markdown(f"### {file_name.replace('_', ' ').title()}")
                
                if file_name == 'class_level_counts':
                    fig = px.bar(
                        df,
                        x='class',
                        y='count',
                        color='level',
                        barmode='stack',
                        title="Class Level Counts",
                        labels={'class': 'Class', 'count': 'Count', 'level': 'Log Level'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                elif file_name == 'level_summary':
                    fig = px.pie(
                        df,
                        names='level',
                        values='count',
                        title="Log Level Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                elif file_name == 'hourly_level_counts':
                    df['hour'] = pd.to_datetime(df['hour'])
                    fig = px.line(
                        df,
                        x='hour',
                        y='count',
                        color='level',
                        title="Hourly Log Counts",
                        labels={'hour': 'Time', 'count': 'Count', 'level': 'Log Level'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                elif file_name in ['class_summary', 'pod_summary', 'container_summary', 'host_summary']:
                    fig = px.bar(
                        df,
                        x=file_name.split('_')[0],
                        y='count',
                        title=f"{file_name.split('_')[0].title()} Summary",
                        labels={file_name.split('_')[0]: file_name.split('_')[0].title(), 'count': 'Count'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                elif file_name == 'class_level_pod':
                    fig = px.scatter(
                        df,
                        x='class',
                        y='pod',
                        size='count',
                        color='level',
                        title="Class vs Pod by Level",
                        labels={'class': 'Class', 'pod': 'Pod', 'count': 'Count', 'level': 'Log Level'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                elif file_name == 'thread_summary':
                    fig = px.bar(
                        df,
                        x='thread',
                        y='count',
                        title="Thread Summary",
                        labels={'thread': 'Thread', 'count': 'Count'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                elif file_name == 'error_analysis':
                    fig = px.bar(
                        df,
                        x='error_type',
                        y='count',
                        title="Error Type Analysis",
                        labels={'error_type': 'Error Type', 'count': 'Count'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                elif file_name == 'time_range':
                    df['start_time'] = pd.to_datetime(df['start_time'])
                    df['end_time'] = pd.to_datetime(df['end_time'])
                    fig = go.Figure(data=[
                        go.Scatter(
                            x=df['start_time'],
                            y=df['event'],
                            mode='markers+lines',
                            name='Start Time'
                        ),
                        go.Scatter(
                            x=df['end_time'],
                            y=df['event'],
                            mode='markers+lines',
                            name='End Time'
                        )
                    ])
                    fig.update_layout(
                        title="Event Time Range",
                        xaxis_title="Time",
                        yaxis_title="Event"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                st.dataframe(df, use_container_width=True)
            
            logger.info("CSV dashboard displayed successfully")
            st.session_state.csv_notifications.append({
                'type': 'success',
                'message': "CSV files processed successfully",
                'timestamp': time.time()
            })
        except Exception as e:
            logger.error(f"Error displaying CSV dashboard: {str(e)}")
            st.session_state.csv_notifications.append({
                'type': 'error',
                'message': f"Error displaying CSV dashboard: {str(e)}",
                'timestamp': time.time()
            })