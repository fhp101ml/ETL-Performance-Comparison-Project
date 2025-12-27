import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import yaml
import os
import subprocess
import glob
import json
from datetime import datetime

# Page Config
st.set_page_config(page_title="ETL Performance Benchmark", layout="wide", page_icon="🚀")

# Title and Style
st.title("🚀 ETL Benchmark Dashboard")
st.markdown("""
<style>
    .metric-card {
        background-color: #262730;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

# Tabs
tab_run, tab_analyze, tab_history = st.tabs(["🚀 Run Experiment", "📊 Analyze Results", "📜 History"])

CONFIG_PATH = "config/experiment_matrix.yaml"
RESULTS_DIR = "experiments_results"

def load_config_dict():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def save_config_dict(config_data):
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(config_data, f, sort_keys=False)

def run_benchmark():
    cmd = ["python", "benchmark_etl.py"]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return process

# --- TAB 1: RUN EXPERIMENT (GUI FORM) ---
with tab_run:
    st.header("Configure & Run")
    
    current_config = load_config_dict()
    etl_config = current_config.get('etl', {})
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("⚙️ Configuration")
        with st.form("experiment_config_form"):
            st.info("Select parameters for the new benchmark run.")
            
            # Form Inputs
            selected_libraries = st.multiselect(
                "Libraries to Test",
                options=['pandas', 'polars', 'duckdb', 'dask'],
                default=etl_config.get('libraries', ['pandas', 'polars'])
            )
            
            selected_sources = st.multiselect(
                "Input Formats (Source)",
                options=['csv', 'parquet', 'json'],
                default=etl_config.get('sources', ['csv'])
            )
            
            selected_destinations = st.multiselect(
                "Output Formats (Destination)",
                options=['csv', 'parquet', 'json'],
                default=etl_config.get('destinations', ['csv'])
            )
            
            rows_limit = st.number_input(
                "Rows Limit (Data Size)", 
                min_value=100, 
                max_value=10000000, 
                value=etl_config.get('rows_limit', 1000),
                step=1000
            )
            
            submitted = st.form_submit_button("💾 Save Configuration")
            
            if submitted:
                # Update Config Object
                etl_config['libraries'] = selected_libraries
                etl_config['sources'] = selected_sources
                etl_config['destinations'] = selected_destinations
                etl_config['rows_limit'] = rows_limit
                current_config['etl'] = etl_config
                
                save_config_dict(current_config)
                st.success("Configuration updated and saved to YAML!")

    with col2:
        st.subheader("🚀 Execution")
        st.write(f"**Current Setup:** Testing {len(selected_libraries)} libraries × {len(selected_sources)} sources × {len(selected_destinations)} destinations.")
        
        if st.button("Start Benchmark Run", type="primary"):
            # Ensure latest config is saved (in case user didn't click save form)
            etl_config['libraries'] = selected_libraries
            etl_config['sources'] = selected_sources
            etl_config['destinations'] = selected_destinations
            etl_config['rows_limit'] = rows_limit
            current_config['etl'] = etl_config
            save_config_dict(current_config)
            
            with st.spinner("Running Benchmark... This may take a while."):
                process = run_benchmark()
                
                log_placeholder = st.empty()
                full_logs = ""
                
                while True:
                    output = process.stdout.readline()
                    if output == '' and process.poll() is not None:
                        break
                    if output:
                        full_logs += output
                        log_placeholder.code(full_logs[-2000:], language="bash")
                
                rc = process.poll()
                if rc == 0:
                    st.success("Benchmark Completed Successfully! 🎉")
                    st.balloons()
                else:
                    st.error("Benchmark Failed.")
                    st.error(process.stderr.read())

# --- TAB 2: ANALYZE RESULTS ---
with tab_analyze:
    st.header("Deep Analysis")
    
    experiments = sorted(glob.glob(os.path.join(RESULTS_DIR, "exp_*")), reverse=True)
    if not experiments:
        st.warning("No experiments found. Run one first!")
    else:
        selected_exp_path = st.selectbox("Select Experiment Run", experiments, format_func=lambda x: os.path.basename(x))
        
        try:
            results_df = pd.read_csv(os.path.join(selected_exp_path, "benchmark_results.csv"))
            with open(os.path.join(selected_exp_path, "manifest.json")) as f:
                manifest = json.load(f)
            
            st.markdown(f"**Timestamp:** {manifest['timestamp']} | **Experiment ID:** `{manifest['experiment_id']}`")
            
            # --- CHARTS ---
            fig_duration = px.bar(
                results_df, x="library", y="total_duration_sec", color="source_type", 
                barmode="group", title="Total Duration (Lower is Better)",
                hover_data=["destination_type", "file_rows"]
            )
            
            fig_memory = px.bar(
                results_df, x="library", y="peak_memory_bytes", color="destination_type",
                barmode="group", title="Peak Memory (Lower is Better)"
            )
            
            df_melted = results_df.melt(
                id_vars=["library", "source_type", "destination_type"], 
                value_vars=["setup_sec", "extract_sec", "transform_sec", "load_sec"],
                var_name="Phase", value_name="Duration (s)"
            )
            fig_breakdown = px.bar(
                df_melted, x="library", y="Duration (s)", color="Phase", 
                title="ETL Phase Breakdown", facet_col="source_type"
            )

            # Display
            c1, c2 = st.columns(2)
            c1.plotly_chart(fig_duration, use_container_width=True)
            c2.plotly_chart(fig_memory, use_container_width=True)
            st.plotly_chart(fig_breakdown, use_container_width=True)
            
            # --- SAVE REPORTS ---
            if st.button("💾 Save Charts to Experiment Folder"):
                charts_dir = os.path.join(selected_exp_path, "charts")
                os.makedirs(charts_dir, exist_ok=True)
                
                fig_duration.write_html(os.path.join(charts_dir, "duration_comparison.html"))
                fig_memory.write_html(os.path.join(charts_dir, "memory_comparison.html"))
                fig_breakdown.write_html(os.path.join(charts_dir, "phase_breakdown.html"))
                
                st.success(f"Charts saved to {charts_dir}")

            st.subheader("Raw Data")
            st.dataframe(results_df, use_container_width=True)
            
        except Exception as e:
            st.error(f"Error loading results: {e}")

# --- TAB 3: HISTORY ---
with tab_history:
    st.header("History & Comparisons")
    st.write("Future feature: Compare multiple experiments side-by-side.")
    if experiments:
        st.write(f"Found {len(experiments)} historical runs.")
        for exp in experiments:
            st.text(os.path.basename(exp))
