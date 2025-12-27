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

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return f.read()

def save_config(content):
    with open(CONFIG_PATH, "w") as f:
        f.write(content)

def run_benchmark():
    # Execute the python script
    # We use subprocess to run it in the same venv
    cmd = ["python", "benchmark_etl.py"]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return process

# --- TAB 1: RUN EXPERIMENT ---
with tab_run:
    st.header("Configure & Run")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.info("Edit the Benchmark Configuration below.")
        config_content = load_config()
        new_config = st.text_area("YAML Configuration", config_content, height=500)
        
        if st.button("💾 Save Config"):
            save_config(new_config)
            st.success("Configuration saved!")
            
    with col2:
        st.subheader("Execution Control")
        if st.button("🚀 Start Benchmark", type="primary"):
            save_config(new_config) # Auto-save before run
            with st.spinner("Running Benchmark... This may take a while."):
                process = run_benchmark()
                
                # Live log viewer
                log_placeholder = st.empty()
                full_logs = ""
                
                while True:
                    output = process.stdout.readline()
                    if output == '' and process.poll() is not None:
                        break
                    if output:
                        full_logs += output
                        log_placeholder.code(full_logs[-2000:], language="bash") # Show last 2000 chars
                
                rc = process.poll()
                if rc == 0:
                    st.success("Benchmark Completed Successfully! 🎉")
                    st.balloons()
                else:
                    st.error("Benchmark Failed.")
                    st.code(process.stderr.read())

# --- TAB 2: ANALYZE RESULTS ---
with tab_analyze:
    st.header("Deep Analysis")
    
    # 1. Select Experiment
    experiments = sorted(glob.glob(os.path.join(RESULTS_DIR, "exp_*")), reverse=True)
    if not experiments:
        st.warning("No experiments found. Run one first!")
    else:
        selected_exp_path = st.selectbox("Select Experiment Run", experiments, format_func=lambda x: os.path.basename(x))
        
        # Load Data
        try:
            results_df = pd.read_csv(os.path.join(selected_exp_path, "benchmark_results.csv"))
            with open(os.path.join(selected_exp_path, "manifest.json")) as f:
                manifest = json.load(f)
            
            st.markdown(f"**Timestamp:** {manifest['timestamp']} | **Experiment ID:** `{manifest['experiment_id']}`")
            
            # --- OVERVIEW METRICS ---
            st.subheader("Overview")
            m1, m2, m3, m4 = st.columns(4)
            best_run = results_df.loc[results_df['total_duration_sec'].idxmin()]
            worst_run = results_df.loc[results_df['total_duration_sec'].idxmax()]
            
            m1.metric("Total Runs", len(results_df))
            m2.metric("Fastest Run", f"{best_run['total_duration_sec']:.3f}s", f"{best_run['library']}")
            m3.metric("Slowest Run", f"{worst_run['total_duration_sec']:.3f}s", f"{worst_run['library']}")
            m4.metric("Avg Duration", f"{results_df['total_duration_sec'].mean():.3f}s")
            
            st.divider()

            # --- DETAILED CHARTS ---
            chart_col1, chart_col2 = st.columns(2)
            
            with chart_col1:
                st.subheader("⏱️ Total Duration by Library & Source")
                fig_bar = px.bar(
                    results_df, 
                    x="library", 
                    y="total_duration_sec", 
                    color="source_type", 
                    barmode="group",
                    title="Performance Comparison (Lower is Better)",
                    hover_data=["destination_type", "file_rows"]
                )
                st.plotly_chart(fig_bar, use_container_width=True)
                
            with chart_col2:
                st.subheader("💾 Peak Memory Usage")
                fig_mem = px.bar(
                    results_df, 
                    x="library", 
                    y="peak_memory_bytes", 
                    color="destination_type",
                    barmode="group",
                    title="Memory Consumption (Lower is Better)",
                    labels={"peak_memory_bytes": "Bytes"}
                )
                st.plotly_chart(fig_mem, use_container_width=True)
            
            st.subheader("🧱 Time Breakdown per Phase")
            # Melt for stacked bar chart of phases
            df_melted = results_df.melt(
                id_vars=["library", "source_type", "destination_type"], 
                value_vars=["setup_sec", "extract_sec", "transform_sec", "load_sec"],
                var_name="Phase", 
                value_name="Duration (s)"
            )
            fig_stack = px.bar(
                df_melted, 
                x="library", 
                y="Duration (s)", 
                color="Phase", 
                title="ETL Phase Breakdown",
                facet_col="source_type" # Break down by source type to see impact
            )
            st.plotly_chart(fig_stack, use_container_width=True)
            
            # --- DATA TABLE ---
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
