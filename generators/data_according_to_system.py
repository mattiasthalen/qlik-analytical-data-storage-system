#!/usr/bin/env python3
"""
Script to generate Qlik data loading scripts based on Swagger API endpoints.
This script reads a Swagger JSON file and generates QVS file with loading blocks
for each endpoint defined in the Swagger specification.
"""

import json
import os
import re
from pathlib import Path


def camel_to_snake(name):
    """Convert CamelCase to snake_case."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def format_display_name(name):
    """Format a name for display in trace messages with proper spacing and capitalization."""
    # Convert camelCase or snake_case to space-separated words
    if '_' in name:
        # If it's already snake_case, split by underscore
        words = name.split('_')
    else:
        # Otherwise, apply camelCase splitting
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1 \2', name)
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1 \2', s1)
        words = s2.split()
    
    # Capitalize each word
    return ' '.join(word.capitalize() for word in words)


def compound_to_snake(name):
    """Convert compound words to snake_case."""
    # Special case handling for common compound words
    common_compounds = {
        'billofmaterials': 'bill_of_materials',
        'businessentity': 'business_entity',
        'creditcard': 'credit_card',
        'countryregion': 'country_region',
        'currencyrate': 'currency_rate',
        'emailaddress': 'email_address',
        'personphone': 'person_phone',
        'phonenumber': 'phone_number',
        'productcategory': 'product_category',
        'productsubcategory': 'product_subcategory',
        'productmodel': 'product_model',
        'productinventory': 'product_inventory',
        'purchaseorder': 'purchase_order',
        'salesorder': 'sales_order',
        'salesperson': 'sales_person',
        'salesreason': 'sales_reason',
        'salesterritory': 'sales_territory',
        'shipmethod': 'ship_method',
        'shoppingcart': 'shopping_cart',
        'specialoffer': 'special_offer',
        'stateprovince': 'state_province',
        'unitmeasure': 'unit_measure',
        'workorder': 'work_order',
        'workorderrouting': 'work_order_routing'
    }
    
    # Check for exact matches in our common compounds dictionary
    if name.lower() in common_compounds:
        return common_compounds[name.lower()]
    
    # If it's already in snake_case format, return as is
    if '_' in name.lower():
        return name.lower()
    
    # Otherwise apply default camelCase to snake_case conversion
    return camel_to_snake(name)


def extract_resource_name(path):
    """Extract the resource name from an API path."""
    # Remove the API version prefix and any trailing parameters
    components = path.split('/')
    # Try to extract a reasonable name from the path
    for component in components:
        if component and not component.startswith('{') and component != 'adventureworks' and component != 'api' and component != 'v1':
            return component
    return components[-1] if components else "unknown"


def main():
    # Paths
    base_dir = Path(os.path.dirname(os.path.abspath(__file__))).parent
    swagger_path = base_dir / "schemas" / "restful_swagger.json"
    output_path = base_dir / "qlik_scripts" / "data_according_to_system.qvs"
    
    # Read the Swagger file
    with open(swagger_path, 'r') as f:
        swagger_data = json.load(f)
    
    # Begin constructing the output
    output = []
    
    # Add header trace with === 
    output.append("Trace")
    output.append("===================================================================")
    output.append("    Data According To System")
    output.append("===================================================================")
    output.append(";")
    output.append("")
    
    output.append("LIB CONNECT TO 'adventure_works__odata';")
    output.append("")
    
    # Process each endpoint
    processed_tags = set()
    
    for path, path_item in swagger_data['paths'].items():
        # For simplicity, we'll only handle GET endpoints
        if 'get' in path_item:
            get_operation = path_item['get']
            
            # Get the tag (resource type) if available
            tags = get_operation.get('tags', [])
            if not tags:
                continue
                
            tag = tags[0]
            
            # We only want to process each tag once
            if tag in processed_tags:
                continue
                
            processed_tags.add(tag)
            
            # Get a sensible resource name and convert to snake_case
            resource_name = tag
            resource_name_snake = compound_to_snake(resource_name)
            
            # Format the display name for the trace message
            display_name = format_display_name(resource_name)
            
            # Extract response schema if available
            schema_ref = None
            try:
                responses = get_operation.get('responses', {})
                success_response = responses.get('200', {})
                content = success_response.get('content', {})
                json_content = content.get('application/json', {})
                schema = json_content.get('schema', {})
                
                # Check if it's an array
                if 'type' in schema and schema['type'] == 'array':
                    schema_ref = schema.get('items', {}).get('$ref')
                else:
                    schema_ref = schema.get('$ref')
                    
            except (KeyError, AttributeError):
                # If we can't find a schema, we'll continue anyway
                pass
            
            # Add trace comment with --- instead of ===
            output.append("Trace")
            output.append("-------------------------------------------------------------------")
            output.append(f"    Extracting {display_name}")
            output.append("-------------------------------------------------------------------")
            output.append(";")
            output.append("LIB CONNECT TO 'adventure_works__odata';")
            output.append("")
            
            # Define the table name
            table_name = f"raw__adventure_works__{resource_name_snake}"
            
            # Add the loading block with snake_case table name
            output.append(f"[{table_name}]:")
            output.append("LOAD")
            
            # If we found a schema reference, try to get the model properties
            field_mappings = []
            
            if schema_ref:
                # Extract the model name from the reference
                model_name = schema_ref.split('/')[-1]
                
                # Look up the model definition
                if 'components' in swagger_data and 'schemas' in swagger_data['components']:
                    model = swagger_data['components']['schemas'].get(model_name, {})
                    properties = model.get('properties', {})
                    
                    # Generate field mappings
                    for prop_name, prop_details in properties.items():
                        snake_case = camel_to_snake(prop_name)
                        field_mappings.append(f"    Text([{prop_name}]) AS [{snake_case}]")
            
            # If we didn't find any fields, add a placeholder
            if not field_mappings:
                field_mappings.append("    *")
            
            # Add field mappings with appropriate punctuation
            for i, mapping in enumerate(field_mappings):
                if i < len(field_mappings) - 1:
                    output.append(f"{mapping}, ")
                else:
                    output.append(f"{mapping}")
            
            output.append(";")
            output.append("")
            
            # Add SELECT section
            output.append("SELECT")
            
            # Add field list
            select_fields = []
            for mapping in field_mappings:
                if mapping.strip() == "*":
                    select_fields.append("    *")
                else:
                    # Extract the field name before AS
                    # Need to handle the Text() wrapper now
                    match = re.search(r'Text\(\[(.*?)\]\)', mapping.strip())
                    if match:
                        field_name = match.group(1)
                        select_fields.append(f"    {field_name}")
                    else:
                        # Fallback for any cases without Text()
                        field_name = mapping.strip().split(' AS ')[0].strip('[]')
                        select_fields.append(f"    {field_name}")
            
            # Add select fields with appropriate punctuation
            for i, field in enumerate(select_fields):
                if i < len(select_fields) - 1:
                    output.append(f"{field},")
                else:
                    output.append(f"{field}")
            
            output.append("")
            output.append("FROM")
            output.append("    JsonV4GetData")
            output.append("")
            output.append("WITH PROPERTIES (")
            output.append(f"    oDataResourcePath='{tag}',")
            output.append("    oDataQueryOptions='',")
            output.append("    maxDepth='',")
            output.append("    maxResults=''")
            output.append(");")
            output.append("")
            
            # Define the variable name for QVD path
            var_name = f"val__qvd_path__{resource_name_snake}"
            
            # Add augmented STORE and DROP statements
            output.append(f"LET {var_name} = '$(val__qvd_path__das)/{table_name}.qvd';")
            output.append(f"TRACE Stored [{table_name}] into [$({var_name})];")
            output.append(f"STORE [{table_name}] INTO [$({var_name})] (qvd);")
            output.append(f"DROP TABLE [{table_name}];")
            output.append("")
            output.append("")

    # Write the output file
    with open(output_path, 'w') as f:
        f.write('\n'.join(output))
    
    print(f"Generated QVS file at: {output_path}")


if __name__ == "__main__":
    main()
