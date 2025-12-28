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
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AI Imports
try:
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_core.prompts import PromptTemplate
    from langchain_core.output_parsers import StrOutputParser
    AI_AVAILABLE = True
except ImportError as e:
    print(f"AI Import Error: {e}")
    AI_AVAILABLE = False

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
tab_run, tab_analyze, tab_ai, tab_history = st.tabs(["🚀 Run Experiment", "📊 Analyze Results", "🤖 AI Global Analysis", "📜 History"])

CONFIG_PATH = "config/experiment_matrix.yaml"
MANIFEST_PATH = "config/library_manifest.yaml"
RESULTS_DIR = "experiments_results"

def load_config_dict():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def load_manifest_dict():
    if os.path.exists(MANIFEST_PATH):
        with open(MANIFEST_PATH, "r") as f:
            return yaml.safe_load(f)
    return {}

def save_config_dict(config_data):
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(config_data, f, sort_keys=False)

def run_benchmark():
    cmd = ["python", "benchmark_etl.py"]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return process

def load_all_experiments_data():
    """Aggregates results from ALL experiments into a single DataFrame"""
    all_files = glob.glob(os.path.join(RESULTS_DIR, "exp_*/benchmark_results.csv"))
    if not all_files:
        return pd.DataFrame()
    
    df_list = []
    for f in all_files:
        try:
            temp_df = pd.read_csv(f)
            # Add experiment ID from path
            exp_id = os.path.basename(os.path.dirname(f))
            temp_df['experiment_id'] = exp_id
            df_list.append(temp_df)
        except:
            pass
            
    if df_list:
        return pd.concat(df_list, ignore_index=True)
    return pd.DataFrame()

# --- TAB 1: RUN EXPERIMENT (GUI FORM) ---
with tab_run:
    st.header("Configure & Run")
    
    current_config = load_config_dict()
    manifest = load_manifest_dict()
    
    etl_config = current_config.get('etl', {})
    
    # Get options from Manifest (Source of Truth) or fallback
    available_engines = manifest.get('capabilities', {}).get('supported_engines', ['pandas', 'polars', 'duckdb', 'dask'])
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("⚙️ Configuration")
        with st.form("experiment_config_form"):
            st.info("Select parameters for the new benchmark run.")
            
            # Form Inputs
            selected_libraries = st.multiselect(
                "Libraries to Test",
                options=available_engines,
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

        # Information Panel based on Manifest
        with st.expander("ℹ️ Library Capabilities"):
            st.markdown(f"**Version:** `{manifest.get('library_info', {}).get('version', 'unknown')}`")
            st.markdown("**Available Transformations:**")
            transformations = manifest.get('capabilities', {}).get('transformation_catalog', [])
            if transformations:
                st.table(pd.DataFrame(transformations)[['name', 'type', 'description']])

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
                manifest_meta = json.load(f)
            
            st.markdown(f"**Timestamp:** {manifest_meta['timestamp']} | **Experiment ID:** `{manifest_meta['experiment_id']}`")
            
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

# --- TAB 3: AI & GLOBAL ANALYTICS ---
with tab_ai:
    st.header("🧠 Global Intelligence & AI Analysis")
    
    # Load Aggregate Data
    all_data = load_all_experiments_data()
    
    if all_data.empty:
        st.warning("No experiment data found. Run some benchmarks first!")
    else:
        # --- 1. GLOBAL INTERACTIVE DASHBOARD ---
        st.subheader("📈 Global Performance Overview")
        
        # Filters
        c_filter1, c_filter2, c_filter3 = st.columns(3)
        with c_filter1:
            selected_libs = st.multiselect("Filter Libraries", options=all_data['library'].unique(), default=all_data['library'].unique())
        with c_filter2:
            selected_srcs = st.multiselect("Filter Source Formats", options=all_data['source_type'].unique(), default=all_data['source_type'].unique())
        with c_filter3:
            # Sort unique row counts
            row_options = sorted(all_data['file_rows'].unique())
            selected_rows = st.multiselect("Filter Dataset Size (Rows)", options=row_options, default=row_options)
            
        # Filter Data
        filtered_df = all_data[
            (all_data['library'].isin(selected_libs)) & 
            (all_data['source_type'].isin(selected_srcs)) &
            (all_data['file_rows'].isin(selected_rows))
        ]
        
        if filtered_df.empty:
            st.info("No data matches filters.")
        else:
            # --- 5 VISUALIZATIONS ---
            
            # Row 1: Speed & Memory
            r1c1, r1c2 = st.columns(2)
            
            with r1c1:
                # 1. Total Duration Ranking (Avg)
                avg_duration = filtered_df.groupby('library')['total_duration_sec'].mean().reset_index()
                fig1 = px.bar(
                    avg_duration, x='library', y='total_duration_sec', text_auto='.4f',
                    color='library', title="🏆 Top Speed (Avg Total Duration)",
                    labels={'total_duration_sec': 'Seconds (Lower is Better)'}
                )
                st.plotly_chart(fig1, use_container_width=True)
                
            with r1c2:
                # 2. Memory Distribution (Violin/Box)
                fig2 = px.box(
                    filtered_df, x='library', y='peak_memory_bytes', color='library',
                    title="🧠 Memory Efficiency Distribution",
                    labels={'peak_memory_bytes': 'Memory (Bytes)'}
                )
                st.plotly_chart(fig2, use_container_width=True)
                
            # Row 2: Scalability & Formats
            r2c1, r2c2 = st.columns(2)
            
            with r2c1:
                # 3. Scalability (Rows vs Time)
                fig3 = px.scatter(
                    filtered_df, x='file_rows', y='total_duration_sec', color='library',
                    trendline="lowess", title="📈 Scalability: Rows vs Duration",
                    labels={'file_rows': 'Number of Rows', 'total_duration_sec': 'Time (s)'}
                )
                st.plotly_chart(fig3, use_container_width=True)
                
            with r2c2:
                # 4. Format Impact (Grouped Bar)
                avg_fmt = filtered_df.groupby(['library', 'source_type'])['total_duration_sec'].mean().reset_index()
                fig4 = px.bar(
                    avg_fmt, x='source_type', y='total_duration_sec', color='library', barmode='group',
                    title="📂 Format Impact (CSV vs Parquet vs JSON)",
                    text_auto='.3f'
                )
                st.plotly_chart(fig4, use_container_width=True)
                
            # Row 3: Phase Breakdown
            # 5. Stacked Bar of Phases
            df_phases = filtered_df.melt(
                id_vars=['library'], 
                value_vars=['setup_sec', 'extract_sec', 'transform_sec', 'load_sec'],
                var_name='phase', value_name='seconds'
            )
            fig5 = px.bar(
                df_phases, x='library', y='seconds', color='phase', title="🏗️ ETL Phase Bottlenecks",
                labels={'seconds': 'Time Spent (s)'}
            )
            st.plotly_chart(fig5, use_container_width=True)

        st.markdown("---")
        
        # --- 2. AI ANALYSIS REPORT SECTION ---
        st.subheader("🤖 AI Executive Report Generation")
        
        if not AI_AVAILABLE:
            st.error("LangChain libraries missing.")
        else:
            # Auto-load key from environment
            env_key = os.getenv("GOOGLE_API_KEY")
            
            if not env_key:
                st.warning("⚠️ `GOOGLE_API_KEY` not found in environment variables. Please set it in `.env` or input below.")
                api_key = st.text_input("Google Gemini API Key", type="password")
            else:
                api_key = env_key
                st.success(f"🔑 API Key loaded from environment (Starts with: {env_key[:4]}...)")
            
            if st.button("🧠 Generate Full Analysis Report & Update README", type="primary"):
                if not api_key:
                    st.error("No API Key provided.")
                else:
                    try:
                        with st.spinner("Consulting Gemini 2.5 Flash..."):
                            # Aggregate data for LLM (Reduce tokens)
                            summary_df = all_data.groupby(['library', 'source_type', 'file_rows']).agg({
                                'total_duration_sec': 'mean',
                                'peak_memory_bytes': 'mean', 
                                'setup_sec': 'mean', 'extract_sec': 'mean',
                                'transform_sec': 'mean', 'load_sec': 'mean'
                            }).reset_index()
                            
                            data_str = summary_df.to_markdown(index=False)
                            
                            template = """
                            You are a Principal Data Architect. Analyze this ETL Benchmark Data.
                            
                            Data Summary:
                            {data}
                            
                            Produce a high-level executive report in Markdown:
                            1. **Executive Summary**: The winner and main insights.
                            2. **Detailed Comparison**: Speed and Memory profiles.
                            3. **Scalability Analysis**: Trends as rows increase.
                            4. **Recommendations**: When to use Pandas vs Polars vs DuckDB.
                            
                            Use clear headings and emojis.
                            """
                            
                            prompt = PromptTemplate(template=template, input_variables=["data"])
                            llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", google_api_key=api_key, temperature=0.3)
                            chain = prompt | llm | StrOutputParser()
                            result = chain.invoke({"data": data_str})
                            
                            # Display Result
                            st.markdown("### 📝 Generated Report")
                            st.markdown(result)
                            
                            # --- PERSISTENCE ---
                            timestamp = datetime.now().strftime("%Y-%m-%d")
                            analysis_dir = "global_analysis"
                            os.makedirs(analysis_dir, exist_ok=True)
                            
                            # Save MD
                            report_file = os.path.join(analysis_dir, f"report_{timestamp}.md")
                            with open(report_file, "w") as f:
                                f.write(f"# Analysis {timestamp}\n{result}")
                                
                            # Save Static Image for README (using Matplotlib for reliability)
                            try:
                                import matplotlib.pyplot as plt
                                import seaborn as sns
                                plt.figure(figsize=(10,6))
                                sns.barplot(data=summary_df, x='library', y='total_duration_sec', hue='source_type')
                                plt.title(f"Benchmark Summary {timestamp}")
                                plt.savefig(os.path.join(analysis_dir, "latest_benchmark_chart.png"))
                                plt.close()
                            except:
                                pass # Fallback if matplotlib fails
                                
                            # Update README using markers
                            readme_path = "README.md"
                            if os.path.exists(readme_path):
                                with open(readme_path, "r") as f: content = f.read()
                                start, end = "<!-- START_GLOBAL_RESULTS -->", "<!-- END_GLOBAL_RESULTS -->"
                                new_block = f"{start}\n## 📊 Latest Results ({timestamp})\n![Chart](global_analysis/latest_benchmark_chart.png)\n### AI Summary\n{result[:800]}...\n[Full Report](global_analysis/report_{timestamp}.md)\n{end}"
                                
                                import re
                                if start in content:
                                    content = re.sub(f"{re.escape(start)}.*?{re.escape(end)}", new_block, content, flags=re.DOTALL)
                                else:
                                    content += "\n" + new_block
                                    
                                with open(readme_path, "w") as f: f.write(content)
                                st.toast("README updated!", icon="✅")

                    except Exception as e:
                        st.error(f"AI Error: {e}")
                            
                    except Exception as e:
                        st.error(f"Analysis Failed: {e}")

# --- TAB 4: HISTORY ---
with tab_history:
    st.header("Experiment History")
    # Existing History logic or simplified view
    all_data = load_all_experiments_data()
    if not all_data.empty:
        fig_hist = px.scatter(
            all_data, x="timestamp", y="total_duration_sec", 
            color="library", size="file_rows",
            title="Performance Trends Over Time"
        )
        st.plotly_chart(fig_hist, use_container_width=True)
    else:
        st.info("No history available.")
