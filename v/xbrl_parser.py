import os
import logging
from lxml import etree
import json
import re
from collections import defaultdict
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='xbrl_parser.log'
)

def parse_xbrl_files(directory='new_xml', max_workers=4):
    """
    Parse all XBRL files in the specified directory and extract KPI data for database storage.
    
    Args:
        directory (str): Directory containing XBRL files
        max_workers (int): Maximum number of worker threads for parallel processing
        
    Returns:
        dict: Dictionary with company names as keys and extracted data as values
    """
    if not os.path.exists(directory):
        logging.error(f"Directory {directory} does not exist")
        return {}
    
    results = {}
    
    # Get all XML files in the directory
    files = [f for f in os.listdir(directory) if f.endswith('.xml')]
    
    # Process files in parallel for better performance
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(process_single_file, os.path.join(directory, file), file): file 
            for file in files
        }
        
        for future in as_completed(future_to_file):
            file = future_to_file[future]
            try:
                company_name, data = future.result()
                results[company_name] = data
                # Save individual file immediately to reduce memory usage
                save_single_file(company_name, data)
                # Clear the data from memory
                data = None
                gc.collect()
            except Exception as e:
                logging.error(f"Error processing {file}: {str(e)}")
    
    return results

def process_single_file(file_path, file_name):
    """
    Process a single XBRL file and return the extracted data
    
    Args:
        file_path (str): Path to the XBRL file
        file_name (str): Name of the file
        
    Returns:
        tuple: (company_name, extracted_data)
    """
    try:
        # Extract company name from filename
        company_name = file_name.replace('.xml', '')
        
        # Parse the XML file
        logging.info(f"Processing file: {file_name}")
        
        # Use iterparse for more memory-efficient parsing
        kpi_data = extract_kpi_data_iterative(file_path)
        
        logging.info(f"Successfully parsed {file_name}")
        return company_name, kpi_data
    except Exception as e:
        logging.error(f"Error parsing {file_name}: {str(e)}")
        raise

def extract_kpi_data_iterative(file_path):
    """
    Extract KPI data from XBRL document using iterative parsing for efficiency.
    
    Args:
        file_path: Path to the XBRL file
        
    Returns:
        dict: Structured KPI data with metadata
    """
    # Dictionary to store KPI data
    kpi_data = {
        'company_info': {},
        'kpis': [],
        'contexts': {},
        'units': {}
    }
    
    # First pass: extract contexts and units
    for event, elem in etree.iterparse(file_path, events=('end',)):
        tag = elem.tag
        
        # Extract contexts
        if tag.endswith('}context'):
            context_id = elem.get('id', '')
            if context_id:
                context_data = extract_single_context(elem)
                kpi_data['contexts'][context_id] = context_data
        
        # Extract units
        elif tag.endswith('}unit'):
            unit_id = elem.get('id', '')
            if unit_id:
                unit_data = extract_single_unit(elem)
                kpi_data['units'][unit_id] = unit_data
        
        # Clear element to save memory
        elem.clear()
        
        # Also eliminate now-empty references from the root node to elem
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    
    # Extract company info
    kpi_data['company_info'] = extract_company_info_from_file(file_path)
    
    # Second pass: extract KPI facts in batches
    batch_size = 1000
    kpi_batch = []
    
    for event, elem in etree.iterparse(file_path, events=('end',)):
        # Only process elements with contextRef attribute (these are the KPI facts)
        if 'contextRef' in elem.attrib:
            try:
                # Skip elements that have children (they're typically container elements)
                if len(elem) == 0:
                    # Process the KPI fact
                    kpi = extract_single_kpi(elem, kpi_data['contexts'], kpi_data['units'])
                    if kpi:
                        kpi_batch.append(kpi)
                        
                        # Process in batches to reduce memory usage
                        if len(kpi_batch) >= batch_size:
                            kpi_data['kpis'].extend(kpi_batch)
                            kpi_batch = []
            except Exception as e:
                logging.error(f"Error extracting KPI data for {elem.tag}: {str(e)}")
                
        # Clear element to save memory
        elem.clear()
        
        # Also eliminate now-empty references from the root node to elem
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    
    # Add any remaining KPIs
    if kpi_batch:
        kpi_data['kpis'].extend(kpi_batch)
    
    return kpi_data

def extract_single_context(context_elem):
    """
    Extract data from a single context element
    
    Args:
        context_elem: The context element
        
    Returns:
        dict: Context data
    """
    nsmap = context_elem.nsmap
    
    # Initialize context data structure
    context_data = {
        'entity': {
            'identifier': '',
            'scheme': ''
        },
        'period': {
            'type': '',
            'start_date': '',
            'end_date': '',
            'instant': ''
        },
        'scenario': {}
    }
    
    # Extract entity information
    entity = context_elem.xpath('./xbrli:entity', namespaces=nsmap)
    if entity and len(entity) > 0:
        identifier = entity[0].xpath('./xbrli:identifier', namespaces=nsmap)
        if identifier and len(identifier) > 0:
            context_data['entity']['identifier'] = identifier[0].text
            context_data['entity']['scheme'] = identifier[0].get('scheme', '')
    
    # Extract period information
    period = context_elem.xpath('./xbrli:period', namespaces=nsmap)
    if period and len(period) > 0:
        # Check if it's a duration (start/end date) or instant period
        start_date = period[0].xpath('./xbrli:startDate', namespaces=nsmap)
        end_date = period[0].xpath('./xbrli:endDate', namespaces=nsmap)
        instant = period[0].xpath('./xbrli:instant', namespaces=nsmap)
        
        if start_date and end_date and len(start_date) > 0 and len(end_date) > 0:
            context_data['period']['type'] = 'duration'
            context_data['period']['start_date'] = start_date[0].text
            context_data['period']['end_date'] = end_date[0].text
        elif instant and len(instant) > 0:
            context_data['period']['type'] = 'instant'
            context_data['period']['instant'] = instant[0].text
    
    # Extract scenario information
    scenario = context_elem.xpath('./xbrli:scenario', namespaces=nsmap)
    if scenario and len(scenario) > 0:
        # Process explicit members
        explicit_members = scenario[0].xpath('.//xbrldi:explicitMember', namespaces=nsmap)
        for member in explicit_members:
            dimension = member.get('dimension', '')
            if dimension:
                # Clean dimension name
                dim_name = dimension.split(':')[-1] if ':' in dimension else dimension
                context_data['scenario'][dim_name] = member.text
        
        # Process typed members
        typed_members = scenario[0].xpath('.//xbrldi:typedMember', namespaces=nsmap)
        for member in typed_members:
            dimension = member.get('dimension', '')
            if dimension:
                # Clean dimension name
                dim_name = dimension.split(':')[-1] if ':' in dimension else dimension
                # Get child element which contains the typed member value
                children = list(member)
                if children:
                    child_tag = clean_tag_name(children[0].tag, {})
                    context_data['scenario'][dim_name] = {
                        'domain': child_tag,
                        'value': children[0].text
                    }
    
    return context_data

def extract_single_unit(unit_elem):
    """
    Extract data from a single unit element
    
    Args:
        unit_elem: The unit element
        
    Returns:
        dict: Unit data
    """
    nsmap = unit_elem.nsmap
    
    # Extract measure
    measure = unit_elem.xpath('./xbrli:measure', namespaces=nsmap)
    if measure and len(measure) > 0:
        return {
            'type': 'measure',
            'value': measure[0].text
        }
    
    # Extract divide
    divide = unit_elem.xpath('./xbrli:divide', namespaces=nsmap)
    if divide and len(divide) > 0:
        numerator = divide[0].xpath('./xbrli:unitNumerator/xbrli:measure', namespaces=nsmap)
        denominator = divide[0].xpath('./xbrli:unitDenominator/xbrli:measure', namespaces=nsmap)
        
        if numerator and denominator and len(numerator) > 0 and len(denominator) > 0:
            return {
                'type': 'divide',
                'numerator': numerator[0].text,
                'denominator': denominator[0].text
            }
    
    return {'type': 'unknown'}

def extract_single_kpi(fact_elem, contexts, units):
    """
    Extract data from a single KPI fact element
    
    Args:
        fact_elem: The fact element
        contexts: Dictionary of contexts
        units: Dictionary of units
        
    Returns:
        dict: KPI data
    """
    # Create mapping between namespaces and prefixes for cleaner KPI names
    prefix_map = {url: prefix for prefix, url in fact_elem.nsmap.items() if prefix}
    
    # Get clean tag name
    tag_name = clean_tag_name(fact_elem.tag, prefix_map)
    
    # Skip technical tags that aren't KPIs
    if tag_name.startswith('xbrli:') or tag_name.startswith('xbrldi:') or tag_name.startswith('link:'):
        return None
        
    # Get context reference
    context_ref = fact_elem.get('contextRef', '')
    
    # Get KPI data
    kpi = {
        'name': tag_name,
        'raw_value': fact_elem.text.strip() if fact_elem.text else None,
        'context_ref': context_ref,
        'unit_ref': fact_elem.get('unitRef', ''),
        'decimals': fact_elem.get('decimals', None),
        'id': fact_elem.get('id', ''),
    }
    
    # Process the value based on type and unit
    if kpi['unit_ref']:
        # Attempt to convert to numeric value
        try:
            if kpi['raw_value']:
                kpi['numeric_value'] = float(kpi['raw_value'])
            else:
                kpi['numeric_value'] = None
        except (ValueError, TypeError):
            kpi['numeric_value'] = None
    
    # Add time period information for easier querying
    if context_ref in contexts:
        period = contexts[context_ref]['period']
        if period['type'] == 'duration':
            kpi['period_start'] = period['start_date']
            kpi['period_end'] = period['end_date']
        elif period['type'] == 'instant':
            kpi['period_instant'] = period['instant']
    
    return kpi

def extract_company_info_from_file(file_path):
    """
    Extract company information from an XBRL file
    
    Args:
        file_path: Path to the XBRL file
        
    Returns:
        dict: Company information
    """
    company_info = {
        'company_identifier': '',
        'reporting_period': {
            'start_date': '',
            'end_date': ''
        },
        'identifier_scheme': ''
    }
    
    # Use iterparse to efficiently extract just the needed information
    for event, elem in etree.iterparse(file_path, events=('end',)):
        # Look for the identifier element in the main context
        if elem.tag.endswith('}context') and elem.get('id') == 'DCYMain':
            # Extract company identifier
            identifier = elem.xpath('.//xbrli:identifier', namespaces=elem.nsmap)
            if identifier and len(identifier) > 0:
                company_info['company_identifier'] = identifier[0].text
                company_info['identifier_scheme'] = identifier[0].get('scheme', '')
            
            # Extract reporting period
            period = elem.xpath('.//xbrli:period', namespaces=elem.nsmap)
            if period and len(period) > 0:
                start_date = period[0].xpath('./xbrli:startDate', namespaces=elem.nsmap)
                end_date = period[0].xpath('./xbrli:endDate', namespaces=elem.nsmap)
                
                if start_date and len(start_date) > 0:
                    company_info['reporting_period']['start_date'] = start_date[0].text
                
                if end_date and len(end_date) > 0:
                    company_info['reporting_period']['end_date'] = end_date[0].text
            
            # We've found what we need, can break the loop
            break
        
        # Clear element to save memory
        elem.clear()
    
    return company_info

def clean_tag_name(tag, prefix_map):
    """
    Clean the tag name by removing namespace URI and using proper prefix
    
    Args:
        tag (str): Raw tag name with namespace URI
        prefix_map (dict): Map of namespace URIs to prefixes
    
    Returns:
        str: Cleaned tag name
    """
    # Handle namespaces in Clark notation {namespace}localname
    if tag.startswith('{'):
        namespace_end = tag.find('}')
        if namespace_end != -1:
            namespace = tag[1:namespace_end]
            localname = tag[namespace_end+1:]
            
            # Use prefix if available
            prefix = prefix_map.get(namespace, '')
            if prefix:
                return f"{prefix}:{localname}"
            else:
                # Just return the local name if no prefix
                return localname
    
    # Return original tag if not in Clark notation
    return tag

def save_single_file(company_name, data, output_dir='parsed_data'):
    """
    Save the parsed data for a single company to a JSON file.
    
    Args:
        company_name (str): Name of the company
        data (dict): Parsed data for the company
        output_dir (str): Output directory path
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create a clean file name
    clean_name = company_name.replace(' ', '_') + '.json'
    output_file = os.path.join(output_dir, clean_name)
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logging.info(f"Saved parsed data to {output_file}")
    except Exception as e:
        logging.error(f"Error saving data for {company_name}: {str(e)}")

def save_parsed_data(parsed_data, output_dir='parsed_data'):
    """
    Save the parsed data to separate JSON files for each company.
    
    Args:
        parsed_data (dict): Dictionary with parsed data
        output_dir (str): Output directory path
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Save each company's data to a separate file
    for company_name, data in parsed_data.items():
        save_single_file(company_name, data, output_dir)

def main_parser():
    """
    Main function to parse XBRL files and save the results.
    """
    print("Starting XBRL parsing...")
    parsed_data = parse_xbrl_files()
    
    if parsed_data:
        print(f"Successfully parsed {len(parsed_data)} files")
        print(f"Saved individual JSON files to the 'parsed_data' directory")
    else:
        print("No files were successfully parsed")
    return parsed_data

if __name__ == "__main__":
    main_parser()
