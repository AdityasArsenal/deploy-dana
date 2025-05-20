import json
import sys
import os
import argparse

def view_json_structure(file_path, max_items=5, search_term=None, list_all=False, category=None):
    """
    View the structure of a JSON file with a limited number of array items
    
    Args:
        file_path (str): Path to the JSON file
        max_items (int): Maximum number of array items to display
        search_term (str): Optional search term to find specific KPIs
        list_all (bool): Whether to list all KPIs
        category (str): Optional category filter
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # If we are searching or listing all, skip the structure display
        if not search_term and not list_all and not category:
            # Get company info and basic structure
            print("=== JSON FILE STRUCTURE ===")
            print(f"File: {os.path.basename(file_path)}\n")
            
            if 'company_info' in data:
                print("COMPANY INFO:")
                print(f"  Identifier: {data['company_info']['company_identifier']}")
                if 'reporting_period' in data['company_info']:
                    period = data['company_info']['reporting_period']
                    print(f"  Reporting Period: {period.get('start_date', '')} to {period.get('end_date', '')}")
            
            # Display available sections
            print("\nAVAILABLE SECTIONS:")
            for key, value in data.items():
                if isinstance(value, dict):
                    print(f"  {key}: Dictionary with {len(value)} items")
                elif isinstance(value, list):
                    print(f"  {key}: List with {len(value)} items")
                else:
                    print(f"  {key}: {type(value).__name__}")
            
            # Show KPI sample
            if 'kpis' in data and data['kpis']:
                print("\nSAMPLE KPIs:")
                for kpi in data['kpis'][:max_items]:
                    print_kpi(kpi)
            
            # Count KPIs by category (prefix)
            if 'kpis' in data and data['kpis']:
                print("\nKPI CATEGORIES:")
                categories = {}
                for kpi in data['kpis']:
                    name = kpi.get('name', '')
                    prefix = name.split(':')[0] if ':' in name else 'other'
                    categories[prefix] = categories.get(prefix, 0) + 1
                
                for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
                    print(f"  {cat}: {count} KPIs")
        
        # Search for KPIs by name
        elif search_term and 'kpis' in data:
            print(f"\n=== SEARCH RESULTS FOR '{search_term}' ===")
            found = False
            for kpi in data['kpis']:
                name = kpi.get('name', '')
                if search_term.lower() in name.lower():
                    print_kpi(kpi)
                    found = True
            
            if not found:
                print(f"No KPIs found matching '{search_term}'")
        
        # List all KPIs in a specific category
        elif category and 'kpis' in data:
            category_prefix = category
            print(f"\n=== ALL KPIs IN CATEGORY '{category_prefix}' ===")
            
            # Count and collect KPIs in this category
            matching_kpis = []
            for kpi in data['kpis']:
                name = kpi.get('name', '')
                prefix = name.split(':')[0] if ':' in name else 'other'
                if prefix == category_prefix:
                    matching_kpis.append(kpi)
            
            print(f"Found {len(matching_kpis)} KPIs in category '{category_prefix}'")
            
            # Print all KPIs in this category
            for kpi in matching_kpis:
                print_kpi(kpi)
        
        # List all KPIs
        elif list_all and 'kpis' in data:
            print("\n=== ALL KPIs ===")
            for kpi in data['kpis']:
                print_kpi(kpi)
        
    except Exception as e:
        print(f"Error processing file: {str(e)}")

def print_kpi(kpi):
    """
    Print a KPI in a readable format
    
    Args:
        kpi (dict): KPI data to print
    """
    print(f"  Name: {kpi.get('name', 'N/A')}")
    print(f"  Raw Value: {kpi.get('raw_value', 'N/A')}")
    if 'numeric_value' in kpi:
        print(f"  Numeric Value: {kpi.get('numeric_value', 'N/A')}")
    print(f"  Context Ref: {kpi.get('context_ref', 'N/A')}")
    print(f"  Unit Ref: {kpi.get('unit_ref', 'N/A')}")
    if 'unit' in kpi:
        unit_info = kpi['unit']
        unit_type = unit_info.get('type', '')
        if unit_type == 'measure':
            print(f"  Unit: {unit_info.get('value', 'N/A')}")
        elif unit_type == 'divide':
            print(f"  Unit: {unit_info.get('numerator', 'N/A')}/{unit_info.get('denominator', 'N/A')}")
    if 'period_start' in kpi and 'period_end' in kpi:
        print(f"  Period: {kpi.get('period_start', '')} to {kpi.get('period_end', '')}")
    elif 'period_instant' in kpi:
        print(f"  Period: {kpi.get('period_instant', '')}")
    if 'decimals' in kpi and kpi['decimals']:
        print(f"  Decimals: {kpi['decimals']}")
    print("  ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='View and analyze XBRL JSON files')
    parser.add_argument('file_path', help='Path to the JSON file')
    parser.add_argument('-n', '--num-items', type=int, default=5, help='Number of sample items to display')
    parser.add_argument('-s', '--search', help='Search for KPIs by name')
    parser.add_argument('-a', '--all', action='store_true', help='List all KPIs')
    parser.add_argument('-c', '--category', help='List all KPIs in a specific category')
    
    args = parser.parse_args()
    
    view_json_structure(args.file_path, args.num_items, args.search, args.all, args.category) 