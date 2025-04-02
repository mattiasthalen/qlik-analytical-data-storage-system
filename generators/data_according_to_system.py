#!/usr/bin/env python3
"""
Script to generate Qlik data loading scripts based on YAML schema file.
This script reads a YAML schema file and generates a QVS file with loading blocks
for each table defined in the schema file.
"""

import yaml
import os
import re
from pathlib import Path

def generate_das_qvs():
    # Paths
    base_dir = Path(os.path.dirname(os.path.abspath(__file__))).parent
    schema_path = base_dir / "schemas" / "raw_schema.yaml"
    output_path = base_dir / "qlik_scripts" / "data_according_to_system.qvs"
    
    # Read the YAML schema file
    with open(schema_path, 'r') as f:
        schema_data = yaml.safe_load(f)
    
    # Begin constructing the output
    output = []
    
    # Add header trace with === 
    output.append("Trace")
    output.append("===================================================================")
    output.append("    Data According To System")
    output.append("===================================================================")
    output.append(";")
    output.append("")
    
    # Process each table in the schema
    if 'tables' in schema_data:
        for table_name, table_info in schema_data['tables'].items():
            
            # Extract entity name from table name for primary key detection
            entity_name = table_name.split('__')[-1]
            
            # Add trace comment with --- instead of ===
            output.append("Trace")
            output.append("-------------------------------------------------------------------")
            output.append(f"    Extracting {table_name}")
            output.append("-------------------------------------------------------------------")
            output.append(";")
            output.append("")
            
            # Add the loading block for Parquet file
            output.append(f"[{table_name}]:")
            output.append("LOAD")
            
            # Add explicit column selects with Text() cast, sorted by category
            if 'columns' in table_info:
                # Initialize field categories
                primary_keys = []
                foreign_keys = []
                regular_fields = []
                system_fields = []  # For rowguid and modified_date
                dlt_fields = []
                
                # Categorize fields
                for column_name in table_info['columns'].keys():
                    if column_name.startswith('_dlt_'):
                        dlt_fields.append(column_name)
                    elif column_name in ['rowguid', 'modified_date']:
                        system_fields.append(column_name)
                    elif column_name.endswith('_id') and entity_name in column_name:
                        # Primary key - if entity name is in the column name and it ends with _id
                        primary_keys.append(column_name)
                    elif column_name.endswith('_id'):
                        # Foreign key - any other column ending with _id
                        foreign_keys.append(column_name)
                    else:
                        # Regular fields - everything else
                        regular_fields.append(column_name)
                
                # Sort each category
                primary_keys.sort()
                foreign_keys.sort()
                regular_fields.sort()
                system_fields.sort()
                dlt_fields.sort()
                
                # Combine all fields in the desired order:
                # 1. Primary keys
                # 2. Foreign keys
                # 3. Regular fields
                # 4. System fields (rowguid, modified_date)
                # 5. DLT fields
                sorted_columns = primary_keys + foreign_keys + regular_fields + system_fields + dlt_fields
                
                # Generate the LOAD statement with sorted fields
                for i, column_name in enumerate(sorted_columns):
                    if i < len(sorted_columns) - 1:
                        output.append(f"    Text([{column_name}]) AS [{column_name}],")
                    else:
                        output.append(f"    Text([{column_name}]) AS [{column_name}]")
            else:
                # Fallback to * if no columns defined
                output.append("    *")
                
            output.append("FROM")
            output.append(f"    [lib://OneDrive - mattias.thalen@two.se/Qlik/Analytical Data Storage System/data/das.{table_name}.parquet] (parquet)")
            output.append(";")
            output.append("")
            
            # Add table description if available
            if 'description' in table_info:
                output.append(f"COMMENT TABLE [{table_name}] WITH '{table_info['description']}';")
                output.append("")
            
            # Add field comments in the same order as the fields in the LOAD statement
            if 'columns' in table_info:
                for field_name in sorted_columns:
                    field_info = table_info['columns'][field_name]
                    if 'description' in field_info:
                        output.append(f"COMMENT FIELD [{field_name}] WITH '{field_info['description']}';")
                
                # Add extra newline after field comments
                output.append("")
            
            # Add STORE and DROP statements
            output.append(f"STORE [{table_name}] INTO [$(val__qvd_path__das)/{table_name}.qvd] (qvd);")
            output.append(f"DROP TABLE [{table_name}];")
            output.append("")

    # Write the output file
    with open(output_path, 'w') as f:
        f.write('\n'.join(output))
    
    print(f"Generated QVS file at: {output_path}")

if __name__ == "__main__":
    generate_das_qvs()
