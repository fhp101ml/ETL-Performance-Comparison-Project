import yaml
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_framework.utils.configuration.config_manager import ConfigManager

def test_config_loading(config_path):
    print(f"--- Testing Configuration Loading from: {config_path} ---\n")
    
    # 1. Check if file exists
    if not os.path.exists(config_path):
        print(f"ERROR: File not found at {config_path}")
        return

    # 2. Raw YAML loading check
    try:
        with open(config_path, 'r') as f:
            raw_content = yaml.safe_load(f)
        print("✅ Raw YAML loaded successfully.")
        print(f"Content keys: {list(raw_content.keys())}\n")
    except Exception as e:
        print(f"❌ Failed to parse YAML: {e}")
        return

    # 3. Use ConfigManager class (Integration Test)
    try:
        manager = ConfigManager(config_path)
        print("✅ ConfigManager initialized.")
        
        # Test retrieving ETL config
        etl_config = manager.get_etl_config('etl')
        print(f"  - Library Type: {etl_config.get('library_type')}")
        print(f"  - Source Type: {etl_config.get('source_type')}")
        print(f"  - Extractor Params: {etl_config.get('extractor_params')}")
        print(f"  - Transformation General: {etl_config.get('transformations', {}).get('general')}")
        
        # Test retrieving Metadata
        metadata = manager.get_metadata_config()
        print("\n✅ Metadata loaded into DatasetMetadata object.")
        print(f"  - Attributes: {metadata.get_attributes()}")
        print(f"  - Rules: {metadata.get_rules()}")
        
    except Exception as e:
        print(f"❌ ConfigManager failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Path to the config file shown in context
    # Adjusting path to match workspace structure
    config_file = os.path.join(os.getcwd(), 'config', 'config_exp.yaml')
    test_config_loading(config_file)
