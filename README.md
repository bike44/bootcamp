# bootcamp

## Setup

1. Copy `bin/env.template` to `.env` and fill in your IndyKite credentials:
   ```bash
   cp bin/env.template .env
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Scripts

These scripts are for loading data into IndyKite. `post_nodes_rels.py` just loads the framework data. `load_emissions.py` is used to load
the emissions files.

### post_nodes_rels.py

Posts nodes or relationships from a JSON file to IndyKite.

**Usage:**
```bash
python bin/post_nodes_rels.py <json_file> [--type nodes|relationships]
```

**Examples:**
```bash
# Auto-detect type (nodes or relationships)
python bin/post_nodes_rels.py data/nodes.json
python bin/post_nodes_rels.py data/relationships.json

# Explicitly specify type
python bin/post_nodes_rels.py data/nodes.json --type nodes
python bin/post_nodes_rels.py data/relationships.json --type relationships
```

### load_emissions.py

Loads emissions data from a CSV file into IndyKite.

**Usage:**
```bash
python bin/load_emissions.py [csv_file] [--batch-size N] [--max-threads N]
```

**Examples:**
```bash
# Use CSV file path from .env file
python bin/load_emissions.py

# Specify CSV file directly
python bin/load_emissions.py data/some_emissions_file.csv

# Customize batch size and thread count
python bin/load_emissions.py data/sample.csv --batch-size 100 --max-threads 4
```

**Requirements:**
- CSV file with emissions data
- `INDYKITE_HOST` and `INDYKITE_TOKEN` set in `.env` file

**Requirements:**
- JSON file with nodes or relationships
- `INDYKITE_HOST` and `INDYKITE_TOKEN` set in `.env` file
- For relationships: nodes must exist in IndyKite first
