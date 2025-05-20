import os
import logging
from lxml import etree
import json
import re
from collections import defaultdict

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='xbrl_parser.log'
)

def parse_xbrl_files(directory='new_xml'):
    """
    Parse all XBRL files in the specified directory and extract KPI data for database storage.
    
    Args:
        directory (str): Directory containing XBRL files
        
    Returns:
        dict: Dictionary with company names as keys and extracted data as values
    """
    if not os.path.exists(directory):
        logging.error(f"Directory {directory} does not exist")
        return {}
    
    results = {}
    
    # Get all XML files in the directory
    files = [f for f in os.listdir(directory) if f.endswith('.xml')]
    
    for file in files:
        file_path = os.path.join(directory, file)
        try:
            # Extract company name from filename
            company_name = file.replace('.xml', '')
            
            # Parse the XML file
            logging.info(f"Processing file: {file}")
            tree = etree.parse(file_path)
            root = tree.getroot()
            
            # Extract all KPIs and their metadata
            data = extract_kpi_data(root)
            
            # Store the parsed data
            results[company_name] = data
            
            logging.info(f"Successfully parsed {file}")
        except Exception as e:
            logging.error(f"Error parsing {file}: {str(e)}")
    
    return results

def extract_kpi_data(root):
    """
    Extract KPI data from XBRL document in a database-friendly format.
    
    Args:
        root: Root element of the XBRL document
        
    Returns:
        dict: Structured KPI data with metadata
    """
    # Get all namespaces for proper element identification
    nsmap = root.nsmap
    
    # Create mapping between namespaces and prefixes for cleaner KPI names
    prefix_map = {url: prefix for prefix, url in nsmap.items() if prefix}
    
    # Extract contexts and units first for lookup reference
    contexts = extract_contexts(root)
    units = extract_units(root)
    
    # Dictionary to store KPI data
    kpi_data = {
        'company_info': extract_company_info(root),
        'kpis': [],
        'contexts': contexts,
        'units': units
    }
    
    # Extract actual KPI facts (elements with context references and numerical/text values)
    facts = root.xpath('//*[@contextRef]', namespaces=nsmap)
    
    for fact in facts:
        try:
            # Skip elements that have children (they're typically container elements)
            if len(fact) > 0:
                continue
                
            # Get clean tag name
            tag_name = clean_tag_name(fact.tag, prefix_map)
            
            # Skip technical tags that aren't KPIs
            if tag_name.startswith('xbrli:') or tag_name.startswith('xbrldi:') or tag_name.startswith('link:'):
                continue
                
            # Get context reference
            context_ref = fact.get('contextRef', '')
            
            # Get KPI data
            kpi = {
                'name': tag_name,
                'raw_value': fact.text.strip() if fact.text else None,
                'context_ref': context_ref,
                'context': contexts.get(context_ref, {}),
                'unit_ref': fact.get('unitRef', ''),
                'decimals': fact.get('decimals', None),
                'id': fact.get('id', ''),
                'metadata': {}
            }
            
            # Add unit information if available
            if kpi['unit_ref'] and kpi['unit_ref'] in units:
                kpi['unit'] = units[kpi['unit_ref']]
            
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
            
            # Add additional attributes
            for attr_name, attr_value in fact.attrib.items():
                if attr_name not in ['contextRef', 'unitRef', 'decimals', 'id']:
                    kpi['metadata'][attr_name] = attr_value
            
            # Add time period information for easier querying
            if context_ref in contexts:
                period = contexts[context_ref]['period']
                if period['type'] == 'duration':
                    kpi['period_start'] = period['start_date']
                    kpi['period_end'] = period['end_date']
                elif period['type'] == 'instant':
                    kpi['period_instant'] = period['instant']
            
            kpi_data['kpis'].append(kpi)
            
        except Exception as e:
            logging.error(f"Error extracting KPI data for {fact.tag}: {str(e)}")
    
    return kpi_data

def extract_company_info(root):
    """
    Extract company information from the XBRL document
    
    Args:
        root: Root element of the XBRL document
        
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
    
    # Extract company identifier
    try:
        # Look for the identifier element in the main context
        identifier = root.xpath('//xbrli:context[@id="DCYMain"]//xbrli:identifier', 
                               namespaces=root.nsmap)
        if identifier and len(identifier) > 0:
            company_info['company_identifier'] = identifier[0].text
            company_info['identifier_scheme'] = identifier[0].get('scheme', '')
    except Exception as e:
        logging.error(f"Error extracting company identifier: {str(e)}")
    
    # Extract reporting period
    try:
        # Get start and end dates from the main context
        start_date = root.xpath('//xbrli:context[@id="DCYMain"]//xbrli:startDate', 
                              namespaces=root.nsmap)
        end_date = root.xpath('//xbrli:context[@id="DCYMain"]//xbrli:endDate', 
                             namespaces=root.nsmap)
        
        if start_date and len(start_date) > 0:
            company_info['reporting_period']['start_date'] = start_date[0].text
        
        if end_date and len(end_date) > 0:
            company_info['reporting_period']['end_date'] = end_date[0].text
    except Exception as e:
        logging.error(f"Error extracting reporting period: {str(e)}")
    
    return company_info

def extract_contexts(root):
    """
    Extract context information for proper interpretation of metrics
    
    Args:
        root: Root element of the XBRL document
        
    Returns:
        dict: Context information with context IDs as keys
    """
    contexts = {}
    
    try:
        # Get all context elements
        context_elements = root.xpath('//xbrli:context', namespaces=root.nsmap)
        
        for context in context_elements:
            context_id = context.get('id', '')
            if not context_id:
                continue
                
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
            entity = context.xpath('./xbrli:entity', namespaces=root.nsmap)
            if entity and len(entity) > 0:
                identifier = entity[0].xpath('./xbrli:identifier', namespaces=root.nsmap)
                if identifier and len(identifier) > 0:
                    context_data['entity']['identifier'] = identifier[0].text
                    context_data['entity']['scheme'] = identifier[0].get('scheme', '')
            
            # Extract period information
            period = context.xpath('./xbrli:period', namespaces=root.nsmap)
            if period and len(period) > 0:
                # Check if it's a duration (start/end date) or instant period
                start_date = period[0].xpath('./xbrli:startDate', namespaces=root.nsmap)
                end_date = period[0].xpath('./xbrli:endDate', namespaces=root.nsmap)
                instant = period[0].xpath('./xbrli:instant', namespaces=root.nsmap)
                
                if start_date and end_date and len(start_date) > 0 and len(end_date) > 0:
                    context_data['period']['type'] = 'duration'
                    context_data['period']['start_date'] = start_date[0].text
                    context_data['period']['end_date'] = end_date[0].text
                elif instant and len(instant) > 0:
                    context_data['period']['type'] = 'instant'
                    context_data['period']['instant'] = instant[0].text
            
            # Extract scenario information
            scenario = context.xpath('./xbrli:scenario', namespaces=root.nsmap)
            if scenario and len(scenario) > 0:
                # Process explicit members
                explicit_members = scenario[0].xpath('.//xbrldi:explicitMember', namespaces=root.nsmap)
                for member in explicit_members:
                    dimension = member.get('dimension', '')
                    if dimension:
                        # Clean dimension name
                        dim_name = dimension.split(':')[-1] if ':' in dimension else dimension
                        context_data['scenario'][dim_name] = member.text
                
                # Process typed members
                typed_members = scenario[0].xpath('.//xbrldi:typedMember', namespaces=root.nsmap)
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
            
            contexts[context_id] = context_data
    
    except Exception as e:
        logging.error(f"Error extracting contexts: {str(e)}")
    
    return contexts

def extract_units(root):
    """
    Extract unit information for metrics
    
    Args:
        root: Root element of the XBRL document
        
    Returns:
        dict: Unit information with unit IDs as keys
    """
    units = {}
    
    try:
        # Get all unit elements
        unit_elements = root.xpath('//xbrli:unit', namespaces=root.nsmap)
        
        for unit in unit_elements:
            unit_id = unit.get('id', '')
            if not unit_id:
                continue
                
            # Extract measure
            measure = unit.xpath('./xbrli:measure', namespaces=root.nsmap)
            if measure and len(measure) > 0:
                units[unit_id] = {
                    'type': 'measure',
                    'value': measure[0].text
                }
                continue
            
            # Extract divide
            divide = unit.xpath('./xbrli:divide', namespaces=root.nsmap)
            if divide and len(divide) > 0:
                numerator = divide[0].xpath('./xbrli:unitNumerator/xbrli:measure', namespaces=root.nsmap)
                denominator = divide[0].xpath('./xbrli:unitDenominator/xbrli:measure', namespaces=root.nsmap)
                
                if numerator and denominator and len(numerator) > 0 and len(denominator) > 0:
                    units[unit_id] = {
                        'type': 'divide',
                        'numerator': numerator[0].text,
                        'denominator': denominator[0].text
                    }
    
    except Exception as e:
        logging.error(f"Error extracting units: {str(e)}")
    
    return units

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
        # Create a clean file name
        clean_name = company_name.replace(' ', '_') + '.json'
        output_file = os.path.join(output_dir, clean_name)
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logging.info(f"Saved parsed data to {output_file}")
        except Exception as e:
            logging.error(f"Error saving data for {company_name}: {str(e)}")

def main_parser():
    """
    Main function to parse XBRL files and save the results.
    """
    print("Starting XBRL parsing...")
    parsed_data = parse_xbrl_files()
    
    if parsed_data:
        print(f"Successfully parsed {len(parsed_data)} files")
        save_parsed_data(parsed_data)
        print(f"Saved individual JSON files to the 'parsed_data' directory")
    else:
        print("No files were successfully parsed")
    return parsed_data

if __name__ == "__main__":
    main_parser()
