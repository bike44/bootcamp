#!/usr/bin/env python3

import csv
import requests
import json
import os
import argparse
import hashlib
import sys
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration from environment variables
INDYKITE_HOST = os.getenv("INDYKITE_HOST", "https://api.indykite.com")
INDYKITE_TOKEN = os.getenv("INDYKITE_TOKEN")
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "250"))
MAX_THREADS = int(os.getenv("MAX_THREADS", "6"))


def create_date_string(year: str, month: str, day: str) -> str:
    """Combine year, month, day into ISO 8601 date string."""
    try:
        # Handle string inputs and pad with zeros if needed
        year = str(year).strip()
        month = str(month).strip().zfill(2)
        day = str(day).strip().zfill(2)
        return f"{year}-{month}-{day}"
    except Exception as e:
        raise ValueError(f"Invalid date values: year={year}, month={month}, day={day}") from e


def generate_external_id(key: str, emission_type: str, date: str) -> str:
    """Generate a 12-byte hash-based external ID from key, emission type, and date.
    Uses first 12 bytes (24 hex characters) of SHA-256 hash for uniqueness.
    """
    combined = f"{key}_{emission_type}_{date}"
    hash_obj = hashlib.sha256(combined.encode('utf-8'))
    # Return first 12 bytes (24 hex characters) for a shorter, unique ID
    return hash_obj.hexdigest()[:24]


def convert_properties_to_array(properties: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert properties dictionary to array format with type and value."""
    props_array = []
    for prop_type, prop_value in properties.items():
        if prop_type.startswith('_') and prop_type.endswith('_metadata'):
            # Skip metadata placeholders - they'll be handled when processing the related property
            continue
        if prop_value is not None and prop_value != "":
            prop_dict = {
                "type": prop_type,
                "value": prop_value
            }
            # Add metadata if it exists (e.g., _volume_metadata for volume property)
            metadata_key = f"_{prop_type}_metadata"
            if metadata_key in properties:
                prop_dict["metadata"] = properties[metadata_key]
            props_array.append(prop_dict)
    return props_array


def create_nodes_batch(token: str, nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Create multiple nodes in a batch using the IndyKite capture API."""
    url = f"{INDYKITE_HOST}/capture/v1/nodes"
    
    headers = {
        "X-IK-ClientKey": token,
        "Content-Type": "application/json"
    }
    
    # Wrap nodes in the required format
    payload = {
        "nodes": nodes
    }
    
    print("\n" + "="*80)
    print("FULL POST REQUEST DETAILS - NODES")
    print("="*80)
    print(f"URL: {url}")
    print(f"\nHeaders:")
    print(json.dumps(headers, indent=2))
    print(f"\nPayload (Full - {len(nodes)} nodes):")
    print(json.dumps(payload, indent=2))
    print("="*80 + "\n")
    
    if DEBUG_MODE:
        print("    [DEBUG MODE] Skipping actual POST request")
        return {"status": "debug", "nodes_processed": len(nodes)}
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        print(f"    Response status: {response.status_code}")
        
        if response.status_code != 200 and response.status_code != 201:
            print(f"    Response body: {response.text[:500]}")
        
        response.raise_for_status()
        result = response.json()
        print(f"    Successfully created batch")
        return result
    except Exception as e:
        print(f"    Error: {e}")
        # Write failed payload to STDERR
        print(f"    Failed payload written to STDERR", file=sys.stderr)
        print("="*80, file=sys.stderr)
        print("FAILED NODES BATCH PAYLOAD", file=sys.stderr)
        print("="*80, file=sys.stderr)
        print(f"URL: {url}", file=sys.stderr)
        print(f"Headers: {json.dumps(headers, indent=2)}", file=sys.stderr)
        print(f"Payload:", file=sys.stderr)
        print(json.dumps(payload, indent=2), file=sys.stderr)
        print("="*80, file=sys.stderr)
        import traceback
        traceback.print_exc()
        raise


def create_relationships_batch(token: str, relationships: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Create multiple relationships in a batch using the IndyKite capture API."""
    url = f"{INDYKITE_HOST}/capture/v1/relationships"
    
    headers = {
        "X-IK-ClientKey": token,
        "Content-Type": "application/json"
    }
    
    # Relationships are sent as an array
    payload = relationships
    
    print("\n" + "="*80)
    print("FULL POST REQUEST DETAILS - RELATIONSHIPS")
    print("="*80)
    print(f"URL: {url}")
    print(f"\nHeaders:")
    print(json.dumps(headers, indent=2))
    print(f"\nPayload (Full - {len(relationships)} relationships):")
    print(json.dumps(payload, indent=2))
    print("="*80 + "\n")
    
    if DEBUG_MODE:
        print("    [DEBUG MODE] Skipping actual POST request")
        return {"status": "debug", "relationships_processed": len(relationships)}
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        print(f"    Response status: {response.status_code}")
        
        if response.status_code != 200 and response.status_code != 201:
            print(f"    Response body: {response.text[:500]}")
        
        response.raise_for_status()
        result = response.json()
        print(f"    Successfully created batch")
        return result
    except Exception as e:
        print(f"    Error: {e}")
        # Write failed payload to STDERR
        print(f"    Failed payload written to STDERR", file=sys.stderr)
        print("="*80, file=sys.stderr)
        print("FAILED RELATIONSHIPS BATCH PAYLOAD", file=sys.stderr)
        print("="*80, file=sys.stderr)
        print(f"URL: {url}", file=sys.stderr)
        print(f"Headers: {json.dumps(headers, indent=2)}", file=sys.stderr)
        print(f"Payload:", file=sys.stderr)
        print(json.dumps(payload, indent=2), file=sys.stderr)
        print("="*80, file=sys.stderr)
        import traceback
        traceback.print_exc()
        raise


# BATCH_SIZE and MAX_THREADS are now set from environment variables above
# Defaults: BATCH_SIZE=250, MAX_THREADS=6


def process_batch(batch_num, total_batches, batch, batch_type, token):
    """Process a single batch of nodes or relationships."""
    try:
        if batch_type == "nodes":
            create_nodes_batch(token, batch)
            return (batch_num, True, f"Batch {batch_num}/{total_batches} ({len(batch)} nodes) completed")
        else:
            create_relationships_batch(token, batch)
            return (batch_num, True, f"Batch {batch_num}/{total_batches} ({len(batch)} relationships) completed")
    except Exception as e:
        import traceback
        error_msg = f"Error creating {batch_type} batch {batch_num}: {e}"
        traceback.print_exc()
        return (batch_num, False, error_msg)


def read_csv_data(file_path: str) -> List[Dict[str, str]]:
    """Read CSV file and return list of dictionaries."""
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)  # Skip header row
        
        for row in reader:
            if len(row) < 21:
                print(f"Warning: Row has fewer than 21 columns, skipping: {row}")
                continue
            
            # Map columns (0-indexed)
            # Column A (index 0): name
            # Column B (index 1): id/key
            # Column C (index 2): year
            # Column D (index 3): month
            # Column E (index 4): day
            data.append({
                'name': row[0].strip() if len(row) > 0 else '',
                'well_key': row[1].strip() if len(row) > 1 else '',
                'year': row[2].strip() if len(row) > 2 else '',
                'month': row[3].strip() if len(row) > 3 else '',
                'day': row[4].strip() if len(row) > 4 else '',
                'row_data': row  # Keep full row for accessing other columns
            })
    
    return data


def group_data_by_well(data: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    """Group data by well key (Column B)."""
    grouped = defaultdict(list)
    for row in data:
        well_key = row['well_key']
        if well_key:
            grouped[well_key].append(row)
    return dict(grouped)


def get_emission_type_columns(emission_type: str) -> Dict[str, int]:
    """Map emission type to column indices.
    This is a placeholder - you'll need to adjust based on your actual CSV structure.
    Assuming columns are grouped: Flaring (5-8), ColdVentilation (9-12), DieselFuel (13-16), FuelGas (17-20)
    """
    # Adjust these indices based on your actual CSV structure
    column_maps = {
        'Flaring': {
            'volume': 5,
            'volume_uom': 6,
            'mass': 7,
            'mass_uom': 8
        },
        'ColdVentilation': {
            'volume': 9,
            'volume_uom': 10,
            'mass': 11,
            'mass_uom': 12
        },
        'DieselFuel': {
            'volume': 13,
            'volume_uom': 14,
            'mass': 15,
            'mass_uom': 16
        },
        'FuelGas': {
            'volume': 17,
            'volume_uom': 18,
            'mass': 19,
            'mass_uom': 20
        }
    }
    return column_maps.get(emission_type, {})


def get_emission_type_display_name(emission_type: str) -> str:
    """Get the human-readable display name for an emission type."""
    display_names = {
        'Flaring': 'Flaring',
        'ColdVentilation': 'Cold Ventilation',
        'DieselFuel': 'Diesel Fuel',
        'FuelGas': 'Fuel Gas'
    }
    return display_names.get(emission_type, emission_type)


def process_emissions_data(token: str, csv_file_path: str, batch_size: int = None, max_threads: int = None):
    """Main function to process CSV and load into IndyKite."""
    
    # Use provided values or defaults from environment
    actual_batch_size = batch_size if batch_size is not None else BATCH_SIZE
    actual_max_threads = max_threads if max_threads is not None else MAX_THREADS
    
    # Extract spreadsheet name from file path
    spreadsheet_name = os.path.basename(csv_file_path)
    
    # Get current timestamp for verified_time
    verified_time = datetime.now().isoformat() + "Z"
    
    # Read entire CSV file first
    print("Reading CSV file...")
    csv_data = read_csv_data(csv_file_path)
    print(f"Read {len(csv_data)} rows from CSV")
    
    # Process all data and collect nodes/relationships
    emission_types = ['Flaring', 'ColdVentilation', 'DieselFuel', 'FuelGas']
    all_nodes = []
    all_relationships = []
    
    # Track well names and structure
    well_info = {}  # well_key -> {name, emissions_id, emission_type_ids}
    emission_type_ids = {}  # (well_key, emission_type) -> external_id
    
    # Collect all date entries with their data
    date_entries = []  # List of dicts with well_key, emission_type, date, properties
    
    print("Processing data and preparing nodes...")
    
    # Group by well
    wells_data = group_data_by_well(csv_data)
    print(f"Found {len(wells_data)} unique wells")
    
    # Process all rows to collect date entries
    for well_key, well_rows in wells_data.items():
        well_name = well_rows[0]['name'] if well_rows else well_key
        well_info[well_key] = {
            'name': well_name,
            'emissions_id': generate_external_id(well_key, 'emissions', ''),
            'emission_type_ids': {}
        }
        
        # Process each emission type
        for emission_type in emission_types:
            emission_type_id = generate_external_id(well_key, f'emission_type_{emission_type}', '')
            well_info[well_key]['emission_type_ids'][emission_type] = emission_type_id
            emission_type_ids[(well_key, emission_type)] = emission_type_id
            
            column_map = get_emission_type_columns(emission_type)
            
            for row in well_rows:
                try:
                    date_str = create_date_string(row['year'], row['month'], row['day'])
                    
                    # Extract values from row based on column map
                    row_data = row['row_data']
                    volume_str = row_data[column_map['volume']].strip() if len(row_data) > column_map['volume'] else None
                    volume_uom = row_data[column_map['volume_uom']].strip() if len(row_data) > column_map['volume_uom'] else None
                    mass_str = row_data[column_map['mass']].strip() if len(row_data) > column_map['mass'] else None
                    mass_uom = row_data[column_map['mass_uom']].strip() if len(row_data) > column_map['mass_uom'] else None
                    
                    # Convert volume and mass to numbers
                    volume = None
                    if volume_str:
                        try:
                            volume = float(volume_str)
                        except ValueError:
                            print(f"    Warning: Could not convert volume '{volume_str}' to number, skipping")
                            volume = None
                    
                    mass = None
                    if mass_str:
                        try:
                            mass = float(mass_str)
                        except ValueError:
                            print(f"    Warning: Could not convert mass '{mass_str}' to number, skipping")
                            mass = None
                    
                    # Only add if we have valid data
                    if volume is not None or mass is not None:
                        # Build properties
                        properties = {"date": date_str}
                        if volume is not None:
                            properties['volume'] = volume
                            # Add volume_uom as metadata on volume property
                            if volume_uom:
                                properties['_volume_metadata'] = {
                                    "custom_metadata": {
                                        "units": volume_uom
                                    },
                                    "source": spreadsheet_name,
                                    "assurance_level": 3,
                                    "verified_time": verified_time
                                }
                        if mass is not None:
                            properties['mass'] = mass
                            # Add mass_uom as metadata on mass property
                            if mass_uom:
                                properties['_mass_metadata'] = {
                                    "custom_metadata": {
                                        "units": mass_uom
                                    },
                                    "source": spreadsheet_name,
                                    "assurance_level": 3,
                                    "verified_time": verified_time
                                }
                        
                        date_entries.append({
                            'well_key': well_key,
                            'emission_type': emission_type,
                            'date': date_str,
                            'properties': properties
                        })
                except Exception as e:
                    print(f"    Error processing row: {e}")
                    continue
    
    # Sort all date entries in reverse chronological order (most recent first)
    print("Sorting data in reverse chronological order...")
    date_entries.sort(key=lambda x: (x['well_key'], x['emission_type'], x['date']), reverse=True)
    
    # Build node list
    print("Building node list...")
    
    # Add Well nodes
    for well_key, info in well_info.items():
        all_nodes.append({
            "external_id": well_key,
            "type": "Well",
            "properties": convert_properties_to_array({"name": info['name']})
        })
    
    # Add Emissions nodes
    for well_key, info in well_info.items():
        all_nodes.append({
            "external_id": info['emissions_id'],
            "type": "Emissions",
            "properties": convert_properties_to_array({"name": "Emissions"})
        })
    
    # Add EmissionType nodes
    for (well_key, emission_type), external_id in emission_type_ids.items():
        all_nodes.append({
            "external_id": external_id,
            "type": "EmissionType",
            "properties": convert_properties_to_array({
                "name": emission_type
            })
        })
    
    # Add date-specific nodes with hash-based external IDs
    for entry in date_entries:
        external_id = generate_external_id(entry['well_key'], entry['emission_type'], entry['date'])
        all_nodes.append({
            "external_id": external_id,
            "type": entry['emission_type'],
            "labels": ["Emission"],
            "properties": convert_properties_to_array(entry['properties'])
        })
    
    print(f"Prepared {len(all_nodes)} nodes")
    
    # Debug: Show breakdown of node types
    if all_nodes:
        node_types = {}
        for node in all_nodes:
            node_type = node.get('type', 'Unknown')
            node_types[node_type] = node_types.get(node_type, 0) + 1
        print(f"  Node breakdown: {node_types}")
    else:
        print("  WARNING: No nodes prepared! Check CSV data processing.")
    
    # Build relationship list
    print("Building relationship list...")
    
    # Well -> Emissions relationships
    for well_key, info in well_info.items():
        all_relationships.append({
            "source": {
                "type": "Well",
                "external_id": well_key
            },
            "target": {
                "type": "Emissions",
                "external_id": info['emissions_id']
            },
            "type": "HAS_EMISSIONS"
        })
    
    # Emissions -> EmissionType relationships
    for (well_key, emission_type), emission_type_id in emission_type_ids.items():
        all_relationships.append({
            "source": {
                "type": "Emissions",
                "external_id": well_info[well_key]['emissions_id']
            },
            "target": {
                "type": "EmissionType",
                "external_id": emission_type_id
            },
            "type": "HAS_TYPE"
        })
    
    # EmissionType -> DateNode and DateNode -> DateNode relationships (maintaining order)
    # Group date entries by (well_key, emission_type) and process in order
    current_groups = {}
    for entry in date_entries:
        key = (entry['well_key'], entry['emission_type'])
        date_node_id = generate_external_id(entry['well_key'], entry['emission_type'], entry['date'])
        
        if key not in current_groups:
            # First date node for this emission type - connect to EmissionType
            emission_type_id = emission_type_ids[key]
            emission_type_name = entry['emission_type']  # Get the emission type name
            all_relationships.append({
                "source": {
                    "type": "EmissionType",
                    "external_id": emission_type_id
                },
                "target": {
                    "type": emission_type_name,
                    "external_id": date_node_id
                },
                "type": "HAS_DATA"
            })
            current_groups[key] = date_node_id
        else:
            # Subsequent date node - connect to previous date node
            previous_node_id = current_groups[key]
            emission_type_name = entry['emission_type']  # Get the emission type name
            all_relationships.append({
                "source": {
                    "type": emission_type_name,
                    "external_id": previous_node_id
                },
                "target": {
                    "type": emission_type_name,
                    "external_id": date_node_id
                },
                "type": "NEXT_DATE"
            })
            current_groups[key] = date_node_id
    
    print(f"Prepared {len(all_relationships)} relationships")
    
    # Debug: Show breakdown of relationship types
    if all_relationships:
        rel_types = {}
        for rel in all_relationships:
            rel_type = rel.get('type', 'Unknown')
            rel_types[rel_type] = rel_types.get(rel_type, 0) + 1
        print(f"  Relationship breakdown: {rel_types}")
    else:
        print("  WARNING: No relationships prepared! Check CSV data processing.")
    
    # Create nodes in batches
    print(f"\nCreating nodes in batches of {actual_batch_size}...")
    total_nodes = len(all_nodes)
    
    if total_nodes == 0:
        print("  ERROR: No nodes to create! Skipping node creation.")
    else:
        # Prepare all batches
        batches = []
        for i in range(0, total_nodes, actual_batch_size):
            batch = all_nodes[i:i + actual_batch_size]
            batch_num = (i // actual_batch_size) + 1
            total_batches = (total_nodes + actual_batch_size - 1) // actual_batch_size
            batches.append((batch_num, total_batches, batch))
        
        # Process batches concurrently using ThreadPoolExecutor
        max_workers = min(actual_max_threads, len(batches))
        print(f"  Processing {len(batches)} batches with up to {max_workers} concurrent threads (BATCH_SIZE={actual_batch_size}, MAX_THREADS={actual_max_threads})...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all batches
            future_to_batch = {
                executor.submit(process_batch, batch_num, total_batches, batch, "nodes", token): (batch_num, total_batches)
                for batch_num, total_batches, batch in batches
            }
            
            # Process completed batches
            completed = 0
            failed = 0
            for future in as_completed(future_to_batch):
                batch_num, total_batches = future_to_batch[future]
                try:
                    result_batch_num, success, message = future.result()
                    if success:
                        completed += 1
                        print(f"  ✓ {message}")
                    else:
                        failed += 1
                        print(f"  ✗ {message}")
                except Exception as e:
                    failed += 1
                    print(f"  ✗ Batch {batch_num} failed with exception: {e}")
        
        print(f"\nNodes: {completed} batches completed, {failed} batches failed")
    
    # Create relationships in batches
    print(f"\nCreating relationships in batches of {actual_batch_size}...")
    total_relationships = len(all_relationships)
    
    if total_relationships == 0:
        print("  ERROR: No relationships to create! Skipping relationship creation.")
    else:
        # Prepare all batches
        batches = []
        for i in range(0, total_relationships, actual_batch_size):
            batch = all_relationships[i:i + actual_batch_size]
            batch_num = (i // actual_batch_size) + 1
            total_batches = (total_relationships + actual_batch_size - 1) // actual_batch_size
            batches.append((batch_num, total_batches, batch))
        
        # Process batches concurrently using ThreadPoolExecutor
        max_workers = min(actual_max_threads, len(batches))
        print(f"  Processing {len(batches)} batches with up to {max_workers} concurrent threads (BATCH_SIZE={actual_batch_size}, MAX_THREADS={actual_max_threads})...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all batches
            future_to_batch = {
                executor.submit(process_batch, batch_num, total_batches, batch, "relationships", token): (batch_num, total_batches)
                for batch_num, total_batches, batch in batches
            }
            
            # Process completed batches
            completed = 0
            failed = 0
            for future in as_completed(future_to_batch):
                batch_num, total_batches = future_to_batch[future]
                try:
                    result_batch_num, success, message = future.result()
                    if success:
                        completed += 1
                        print(f"  ✓ {message}")
                    else:
                        failed += 1
                        print(f"  ✗ {message}")
                except Exception as e:
                    failed += 1
                    print(f"  ✗ Batch {batch_num} failed with exception: {e}")
        
        print(f"\nRelationships: {completed} batches completed, {failed} batches failed")


def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Load emissions data from CSV into IndyKite using the Capture API"
    )
    parser.add_argument(
        "csv_file",
        nargs="?",
        default=None,
        help="Path to the CSV file containing emissions data (takes precedence over CSV_FILE_PATH env var)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help=f"Number of items per batch (default: {BATCH_SIZE} from env or 250)"
    )
    parser.add_argument(
        "--max-threads",
        type=int,
        default=None,
        help=f"Maximum number of concurrent threads (default: {MAX_THREADS} from env or 6)"
    )
    
    args = parser.parse_args()
    
    # Get batch size and max threads from command line or environment
    batch_size = args.batch_size if args.batch_size else BATCH_SIZE
    max_threads = args.max_threads if args.max_threads else MAX_THREADS
    
    print(f"Configuration: BATCH_SIZE={batch_size}, MAX_THREADS={max_threads}")
    
    # Determine CSV file path: command line argument takes precedence
    csv_file_path = args.csv_file if args.csv_file else os.getenv("CSV_FILE_PATH", "emissions_data.csv")
    
    if not csv_file_path:
        print("Error: CSV file path not provided. Specify it as a command line argument or set CSV_FILE_PATH in .env")
        parser.print_help()
        return
    
    if not os.path.exists(csv_file_path):
        print(f"Error: CSV file not found: {csv_file_path}")
        return
    
    print("Starting emissions data import to IndyKite...")
    print(f"Using CSV file: {csv_file_path}")
    
    # Validate required environment variables
    if not INDYKITE_TOKEN:
        print("Error: Missing required environment variable: INDYKITE_TOKEN")
        print("Please update your .env file with the required value.")
        return
    
    if not INDYKITE_HOST:
        print("Error: Missing required environment variable: INDYKITE_HOST")
        print("Please update your .env file with the required value.")
        return
    
    print(f"Using IndyKite host: {INDYKITE_HOST}")
    if DEBUG_MODE:
        print("*** DEBUG MODE ENABLED - Requests will be printed but not sent ***")
    
    # Process data
    try:
        process_emissions_data(INDYKITE_TOKEN, csv_file_path, batch_size=batch_size, max_threads=max_threads)
        print("\nImport completed successfully!")
    except Exception as e:
        print(f"\nImport failed: {e}")
        raise


if __name__ == "__main__":
    main()

