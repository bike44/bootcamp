#!/usr/bin/env python3

import json
import os
import sys
import argparse
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration from environment variables
INDYKITE_HOST = os.getenv("INDYKITE_HOST", "https://api.indykite.com")
INDYKITE_TOKEN = os.getenv("INDYKITE_TOKEN")
SSL_VERIFY = os.getenv("SSL_VERIFY", "false").lower() != "false"

# Disable SSL warnings if verification is disabled
if not SSL_VERIFY:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    print("⚠️  WARNING: SSL certificate verification is DISABLED")


def post_nodes(token: str, nodes: list):
    """Post nodes to IndyKite capture API."""
    url = f"{INDYKITE_HOST}/capture/v1/nodes"
    
    headers = {
        "X-IK-ClientKey": token,
        "Content-Type": "application/json"
    }
    
    payload = {
        "nodes": nodes
    }
    
    print(f"POST {url}")
    print(f"Sending {len(nodes)} nodes...")
    
    response = requests.post(url, headers=headers, json=payload, verify=SSL_VERIFY)
    print(f"Response status: {response.status_code}")
    
    if response.status_code != 200 and response.status_code != 201:
        print(f"\n{'='*80}")
        print(f"ERROR DETAILS (Status {response.status_code})")
        print(f"{'='*80}")
        
        # Print response headers (useful for debugging)
        print("\nResponse Headers:")
        for key, value in response.headers.items():
            print(f"  {key}: {value}")
        
        # Try to parse and pretty-print JSON error response
        print("\nResponse Body:")
        try:
            error_json = response.json()
            print(json.dumps(error_json, indent=2))
        except (json.JSONDecodeError, ValueError):
            # If not JSON, just print the raw text
            print(response.text)
        
        # For 500 errors, provide additional context
        if response.status_code == 500:
            print("\n⚠️  Server Error (500) detected!")
            print("This indicates an internal server error. Common causes:")
            print("  - Invalid data format or structure")
            print("  - Missing required fields")
            print("  - Server-side validation failure")
            print("  - Backend service issue")
        
        print("\nFailed payload written to STDERR", file=sys.stderr)
        print("="*80, file=sys.stderr)
        print("FAILED NODES PAYLOAD", file=sys.stderr)
        print("="*80, file=sys.stderr)
        print(f"URL: {url}", file=sys.stderr)
        print(f"Headers: {json.dumps(headers, indent=2)}", file=sys.stderr)
        print(f"Payload:", file=sys.stderr)
        print(json.dumps(payload, indent=2), file=sys.stderr)
        print("="*80, file=sys.stderr)
        response.raise_for_status()
    
    return response.json()


def post_relationships(token: str, payload):
    """Post relationships to IndyKite capture API."""
    url = f"{INDYKITE_HOST}/capture/v1/relationships"
    
    headers = {
        "X-IK-ClientKey": token,
        "Content-Type": "application/json"
    }
    
    # Determine count for logging
    if isinstance(payload, dict) and "relationships" in payload:
        count = len(payload["relationships"])
    elif isinstance(payload, list):
        count = len(payload)
    else:
        count = 1
    
    print(f"POST {url}")
    print(f"Sending {count} relationships...")
    
    response = requests.post(url, headers=headers, json=payload, verify=SSL_VERIFY)
    print(f"Response status: {response.status_code}")
    
    if response.status_code != 200 and response.status_code != 201:
        print(f"\n{'='*80}")
        print(f"ERROR DETAILS (Status {response.status_code})")
        print(f"{'='*80}")
        
        # Print response headers (useful for debugging)
        print("\nResponse Headers:")
        for key, value in response.headers.items():
            print(f"  {key}: {value}")
        
        # Try to parse and pretty-print JSON error response
        print("\nResponse Body:")
        try:
            error_json = response.json()
            print(json.dumps(error_json, indent=2))
        except (json.JSONDecodeError, ValueError):
            # If not JSON, just print the raw text
            print(response.text)
        
        # For 500 errors, provide additional context
        if response.status_code == 500:
            print("\n⚠️  Server Error (500) detected!")
            print("This indicates an internal server error. Common causes:")
            print("  - Invalid data format or structure")
            print("  - Missing required fields")
            print("  - Server-side validation failure")
            print("  - Backend service issue")
        
        print("\nFailed payload written to STDERR", file=sys.stderr)
        print("="*80, file=sys.stderr)
        print("FAILED RELATIONSHIPS PAYLOAD", file=sys.stderr)
        print("="*80, file=sys.stderr)
        print(f"URL: {url}", file=sys.stderr)
        print(f"Headers: {json.dumps(headers, indent=2)}", file=sys.stderr)
        print(f"Payload:", file=sys.stderr)
        print(json.dumps(payload, indent=2), file=sys.stderr)
        print("="*80, file=sys.stderr)
        response.raise_for_status()
    
    return response.json()


def detect_type(data):
    """Detect if data is nodes or relationships based on structure."""
    if isinstance(data, dict):
        # Check if it's a nodes payload with "nodes" key
        if "nodes" in data and isinstance(data["nodes"], list):
            return "nodes"
        # Check if it has node-like structure (external_id, type, properties)
        if "external_id" in data and "type" in data:
            return "nodes"
    elif isinstance(data, list):
        if len(data) > 0:
            # Check first item to determine type
            first_item = data[0]
            if isinstance(first_item, dict):
                # Nodes have external_id, type, properties
                if "external_id" in first_item and "type" in first_item and "properties" in first_item:
                    return "nodes"
                # Relationships have source, target, type
                elif "source" in first_item and "target" in first_item and "type" in first_item:
                    return "relationships"
    
    return None


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Post nodes or relationships from a JSON file to IndyKite"
    )
    parser.add_argument(
        "json_file",
        help="Path to the JSON file containing nodes or relationships"
    )
    parser.add_argument(
        "--type",
        choices=["nodes", "relationships"],
        default=None,
        help="Explicitly specify type (nodes or relationships). If not provided, will auto-detect."
    )
    
    args = parser.parse_args()
    
    # Validate required environment variables
    if not INDYKITE_TOKEN:
        print("Error: Missing required environment variable: INDYKITE_TOKEN")
        print("Please update your .env file with the required value.")
        return 1
    
    if not INDYKITE_HOST:
        print("Error: Missing required environment variable: INDYKITE_HOST")
        print("Please update your .env file with the required value.")
        return 1
    
    if not os.path.exists(args.json_file):
        print(f"Error: JSON file not found: {args.json_file}")
        return 1
    
    # Read JSON file
    try:
        with open(args.json_file, 'r') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON file: {e}")
        return 1
    except Exception as e:
        print(f"Error reading file: {e}")
        return 1
    
    # Determine type
    data_type = args.type
    if not data_type:
        data_type = detect_type(data)
        if not data_type:
            print("Error: Could not determine if data is nodes or relationships.")
            print("Please specify --type nodes or --type relationships")
            return 1
        print(f"Auto-detected type: {data_type}")
    
    # Extract the actual list
    if data_type == "nodes":
        items = data
        # Handle nested structures: {"nodes": [...]} or {"nodes": {"nodes": [...]}}
        while isinstance(items, dict) and "nodes" in items:
            items = items["nodes"]
        
        # If still not a list, wrap it
        if not isinstance(items, list):
            items = [items] if items else []
        
        # Ensure items is a list
        if not isinstance(items, list):
            print(f"Error: Expected a list of nodes, got {type(items)}")
            return 1
        
        try:
            result = post_nodes(INDYKITE_TOKEN, items)
            print(f"\nSuccessfully posted {len(items)} nodes")
            print(f"Response: {json.dumps(result, indent=2)}")
            return 0
        except Exception as e:
            print(f"\nError posting nodes: {e}")
            return 1
    
    elif data_type == "relationships":
        # Send data as-is (could be wrapped {"relationships": [...]} or array [...])
        try:
            result = post_relationships(INDYKITE_TOKEN, data)
            # Determine count for success message
            if isinstance(data, dict) and "relationships" in data:
                count = len(data["relationships"])
            elif isinstance(data, list):
                count = len(data)
            else:
                count = 1
            print(f"\nSuccessfully posted {count} relationships")
            print(f"Response: {json.dumps(result, indent=2)}")
            return 0
        except Exception as e:
            print(f"\nError posting relationships: {e}")
            return 1


if __name__ == "__main__":
    sys.exit(main())

