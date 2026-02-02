"""
Minimal script to update catalog and schema parameters in Databricks dashboard JSON files.
"""

import json
from typing import Dict, Any


def update_parameters(dashboard_data: Dict[str, Any], new_catalog: str = None, new_schema: str = None, new_workspace_url: str = None, new_dashboard_id: str = None, date_range_min: str = None, date_range_max: str = None, space_id_default: str = "ALL") -> Dict[str, Any]:
    """
    Update catalog, schema, workspace URL, dashboard ID, date ranges, and space ID parameters in dashboard JSON.
    
    Args:
        dashboard_data: The dashboard JSON data
        new_catalog: New catalog value (None to skip)
        new_schema: New schema value (None to skip)
        new_workspace_url: New workspace URL value (None to skip)
        new_dashboard_id: New dashboard ID value (None to skip)
        date_range_min: Minimum date for date range parameters (None to skip)
        date_range_max: Maximum date for date range parameters (None to skip)
        space_id_default: Default value for space_id/genie_space_id parameters (default: "ALL")
    
    Returns:
        Updated dashboard data
    """
    if 'datasets' not in dashboard_data:
        return dashboard_data
    
    # Track updates for reporting
    updates = []
    
    # Iterate through all datasets
    for dataset in dashboard_data['datasets']:
        dataset_name = dataset.get('displayName', 'unknown')
        
        if 'parameters' not in dataset:
            continue
        
        # Check each parameter
        for param in dataset['parameters']:
            keyword = param.get('keyword', '')
            
            # Skip if no defaultSelection
            if 'defaultSelection' not in param:
                continue
            
            # Initialize the structure if it's empty or missing required keys
            if not param['defaultSelection'] or 'values' not in param['defaultSelection']:
                param['defaultSelection'] = {
                    'values': {
                        'dataType': param.get('dataType', 'STRING'),
                        'values': [{'value': ''}]
                    }
                }
            elif 'values' not in param['defaultSelection']['values']:
                param['defaultSelection']['values']['values'] = [{'value': ''}]
            elif len(param['defaultSelection']['values']['values']) == 0:
                param['defaultSelection']['values']['values'] = [{'value': ''}]
            
            # Update catalog parameter
            if keyword == 'catalog' and new_catalog is not None:
                old_value = param['defaultSelection']['values']['values'][0]['value']
                param['defaultSelection']['values']['values'][0]['value'] = new_catalog
                updates.append(f"Dataset '{dataset_name}': catalog '{old_value}' -> '{new_catalog}'")
            
            # Update schema parameter
            elif keyword == 'schema' and new_schema is not None:
                old_value = param['defaultSelection']['values']['values'][0]['value']
                param['defaultSelection']['values']['values'][0]['value'] = new_schema
                updates.append(f"Dataset '{dataset_name}': schema '{old_value}' -> '{new_schema}'")
            
            # Update workspace URL parameter
            elif keyword == 'your_workspace_url' and new_workspace_url is not None:
                old_value = param['defaultSelection']['values']['values'][0]['value']
                param['defaultSelection']['values']['values'][0]['value'] = new_workspace_url
                updates.append(f"Dataset '{dataset_name}': your_workspace_url '{old_value}' -> '{new_workspace_url}'")
            
            # Update dashboard ID parameter
            elif keyword == 'dashboard_id' and new_dashboard_id is not None:
                old_value = param['defaultSelection']['values']['values'][0]['value']
                param['defaultSelection']['values']['values'][0]['value'] = new_dashboard_id
                updates.append(f"Dataset '{dataset_name}': dashboard_id '{old_value}' -> '{new_dashboard_id}'")
            
            # Update date range parameter (keyword: "param" with complexType: "RANGE")
            elif keyword == 'param' and param.get('complexType') == 'RANGE':
                if 'range' in param['defaultSelection'] and (date_range_min is not None or date_range_max is not None):
                    if date_range_min is not None:
                        old_min = param['defaultSelection']['range']['min']['value']
                        param['defaultSelection']['range']['min']['value'] = date_range_min
                        updates.append(f"Dataset '{dataset_name}': date_range_min '{old_min}' -> '{date_range_min}'")
                    if date_range_max is not None:
                        old_max = param['defaultSelection']['range']['max']['value']
                        param['defaultSelection']['range']['max']['value'] = date_range_max
                        updates.append(f"Dataset '{dataset_name}': date_range_max '{old_max}' -> '{date_range_max}'")
            
            # Update space_id or genie_space_id parameters
            elif keyword in ['space_id', 'genie_space_id'] and space_id_default is not None:
                if 'values' in param['defaultSelection']:
                    old_value = param['defaultSelection']['values']['values'][0]['value']
                    param['defaultSelection']['values']['values'][0]['value'] = space_id_default
                    updates.append(f"Dataset '{dataset_name}': {keyword} '{old_value}' -> '{space_id_default}'")
    
    # Print summary
    if updates:
        print(f"\nUpdated {len(updates)} parameter(s):")
        for update in updates:
            print(f"  - {update}")
    else:
        print("\nNo parameters were updated.")
    
    return dashboard_data


def main():
    """Example usage"""
    # Example: Load from file
    input_file = 'dashboard.json'
    output_file = 'dashboard_updated.json'
    
    # Read the dashboard JSON
    with open(input_file, 'r') as f:
        dashboard = json.load(f)
    
    # Update parameters
    updated_dashboard = update_parameters(
        dashboard,
        new_catalog='new_catalog_name',
        new_schema='new_schema_name',
        new_workspace_url='https://your-workspace.cloud.databricks.com',
        new_dashboard_id='your_dashboard_id'
    )
    
    # Write updated JSON
    with open(output_file, 'w') as f:
        json.dump(updated_dashboard, f, indent=2)
    
    print(f"\nUpdated dashboard saved to: {output_file}")


if __name__ == '__main__':
    # For direct script usage, modify these values
    import sys
    
    if len(sys.argv) == 6:
        # Usage: python update_dashboard_params.py input.json new_catalog new_schema new_workspace_url new_dashboard_id
        input_file = sys.argv[1]
        new_catalog = sys.argv[2]
        new_schema = sys.argv[3]
        new_workspace_url = sys.argv[4]
        new_dashboard_id = sys.argv[5]
        output_file = input_file.replace('.json', '_updated.json')
        
        with open(input_file, 'r') as f:
            dashboard = json.load(f)
        
        updated_dashboard = update_parameters(dashboard, new_catalog, new_schema, new_workspace_url, new_dashboard_id)
        
        with open(output_file, 'w') as f:
            json.dump(updated_dashboard, f, indent=2)
        
        print(f"\nUpdated dashboard saved to: {output_file}")
    else:
        print("Usage: python update_dashboard_params.py <input.json> <new_catalog> <new_schema> <new_workspace_url> <new_dashboard_id>")
        print("\nOr modify the main() function for custom usage.")

