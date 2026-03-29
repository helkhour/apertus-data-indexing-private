# Apertus Pre-Training data indexing

An ElasticSearch Deployment on Clariden ALPS CSCS Cluster. 
This is the supporting code for the paper "Getting Your Indices in a Row: Full-Text Search for LLM Training Data for Real World" by Ines Altemir Marinas, Anastasiia Kucherenko, Alexander Sternfeld, Andrei Kucharavy, available at http://arxiv.org/abs/2510.09471. 

## Repository structure :books:
-- **scripts** \
&nbsp;&nbsp;&nbsp;&nbsp;|--- *detokenize* \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```megatron_detokenizer.py```: Helper functions. \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```batch_detokenize.py```: Script to batch process Megatron datasets. \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```detokenize.sh```: Megatron dataset detokenization script for SLURM. \
&nbsp;&nbsp;&nbsp;&nbsp;|--- *index* \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```index.py```: Indexer script for Elasticsearch with multi-process support. \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```index_with_id.py```: Script with added content-based SHA256 document IDs for deduplication. \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```index_web_export.py```: Thin wrapper that indexes exported `web-search` parquet with web-specific flags. \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```index.sh```: Script to run indexing for Slurm. \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```index_with_id.sh```: Script to run indexing with id for Slurm. \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```indexing_job_status.py```: Script to automatically evaluate the indexing jobs, detect failures and output statistics. \
&nbsp;&nbsp;&nbsp;&nbsp;|--- *merge* \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```merge.py```: Remote reindex merge script for Elasticsearch. \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```merge.sh```: Slurm script to run merge operation. \
&nbsp;&nbsp;&nbsp;&nbsp;|--- *search* \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```search.py```: Script to perform various search query types across multiple CSV files. \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```search_web_index.py```: Thin wrapper that searches a web dataset index with URL-aware rendering. \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```search.sh```: . \
&nbsp;&nbsp;&nbsp;&nbsp;|--- *web_integration* \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```run_web_search_ingest.py```: Thin integration wrapper around the standalone `web-search` package. \
-- **results** \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```Indexing_performance```: Results for the indexing operation on the Apertus pre-training data. \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```Search```: Results for the search queries upon constructed indexes. \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```Merge```: Results for the merging operation of source indexes into unified target indexes. \
-- **container_image** \
&nbsp;&nbsp;&nbsp;&nbsp;|--- ```Dockerfile```: Dockerfile for the Container Image. \
-- **search_queries** \
&nbsp;&nbsp;&nbsp;&nbsp;|--- ```chemicals```: Contains the test datasets for chemical queries. \
&nbsp;&nbsp;&nbsp;&nbsp;|--- ```ObsceneWords```: Contains the test dataset for Obscene Words. \
&nbsp;&nbsp;&nbsp;&nbsp;|--- ```WeaponizedWords```: Contains the test dataset for Weaponized Words. \
&nbsp;&nbsp;&nbsp;&nbsp;|--- ```Verbatim```: Contains the test dataset and code needed to reproduce verbatim samples. \

## Container Creation
The container_image folder contains the Dockerfile for the Container Image. Exact instructions on how to proceed are:
1. There is a created Dockerfile with desired packages, in “container_image/" folder 
2. Ensure you're in the directory containing the `Dockerfile` and run:
    podman build -t image:tag
3. enroot import -x mount -o <image_name.sqsh> podman://image:tag
4. Create .toml file in home directory /.edf path. 
    ```python
    image = "/container_image/<image_name.sqsh>"
    workdir = "/capstor/scratch/cscs/<username>"
    writable = true
    mounts = [
        "/iopsstor/scratch/cscs/<username>:/iopsstor/scratch/cscs/<username>",
        "/capstor/scratch/cscs/<username>:/capstor/scratch/cscs/<username>",
        "/iopsstor/scratch/cscs/<username>/es-data:/usr/share/elasticsearch/data",
        "/iopsstor/scratch/cscs/<username>/es-logs:/usr/share/elasticsearch/logs"
    ]
    [annotations.com.hooks.ssh]
    enabled = "true"
    ```
    Mount every directory you wish to be able to work from with this container.
        
5. Open the container with: srun -A a-<account_number> --environment=<image_name> --pty bash

# Scripts

## Index

The main code part running the indexing of the data. 

## Search 
We explain the different types of search queries implemented in this Elasticsearch search queries pipeline. Each query type serves different search scenarios and has specific use cases, limitations, and performance characteristics.

### Query Types Overview

The pipeline implements **6 different query types** that can be selectively enabled/disabled through configuration flags:

### 1. Match Query (`match_query`)
**Purpose**: the standard query for performing a (general) full-text search

**How it works**:
- Performs an OR query for all individual terms in the search phrase by default
- Analyzes the input text and searches for each term separately
- Terms can appear in any order within the document
- Can be configured to use AND operator (all terms must be present, set <operator> parameter)
- Finding documents that contain most/all search terms (higher score - explain in detail how computed)
- Handling synonyms and stemming through analyzers (depends on analyzer used - can parametrize this too to have +- harsh analyzer)

### 2. Match Phrase Query (`match_phrase_query`)
**Purpose**: Exact phrase matching with word order preservation

**How it works**:
- Searches for the exact phrase where word order matters
- Functions as an AND clause between all individual terms with positional information
- Supports slop parameter for allowing word gaps


### 3. Wildcard Query (`wildcard_query`) 
**Purpose**: Pattern matching with wildcards on single tokens

**How it works**:
- Performs single token matching with wildcard patterns (`*text*`)
- Works on individual tokens, not across multiple tokens
- **Limitation**: Only executes on single words - skips multi-word phrases
- More expensive than other query types - slower due to pattern matching complexity
- Performs partial word matching, finding variations of a root word (same as match_phrase with stemmer?)

### 4. Fuzzy Query (`fuzzy_query`)
**Purpose**: Handling typos and spelling variations

**How it works**:
- Designed for single words with typos or spelling variations
- Uses edit distance (Levenshtein distance) with "AUTO" fuzziness
- **Fallback**: For multi-word phrases, uses `multi_match` with fuzziness and AND operator
- Tries to match against single tokens

### 5. Boolean Must Query (`bool_must_query`)
**Purpose**: Complex boolean combinations with multiple conditions

**How it works**:
- Combines multiple match conditions using boolean logic
- All conditions must be satisfied (AND logic)
- If single word provided, duplicates it for the boolean structure
- Can construct advanced filtering scenarios for complex search requirements


**Output and Analysis**

Each query execution provides:
- **Response time**: Query execution time in milliseconds
- **Hit count**: Number of matching documents
- **Score information**: Relevance scoring details
- **Hit snippets**: Top 5 results with highlighted matches

**Configuration Options**

```python
# Query execution flags - set to False to skip query types
execute_match_query = True
execute_match_phrase_query = True
execute_wildcard_query = True
execute_fuzzy_query = True
execute_bool_must_query = False  
```

**Notes**
When coding on windows and doing scp, use this to remove Windows CLRF tokens:
```bash
sed -i 's/\r$//' /path/to/index.sh
```

## Web Integration
Web crawling and extraction now live in the standalone `web-search` repo, which should sit beside this repo in the parent directory:

```text
semesterProject/
├── apertus-pretraining-data-indexing/
└── web-search/
```

- `web-search`: URL discovery, `robots.txt`, fetching, extraction, address-tree metadata, snippet metadata, duplicate-page aggregation, and duplicate-chunk aggregation
- `apertus-pretraining-data-indexing`: Elasticsearch indexing, search, merge, and integration commands for web and non-web datasets

### What Apertus Expects From `web-search`
The exported web records stay URL-based and tree-aware. Important fields include:
- `requested_url`, `url`, `requested_urls`, `urls`
- `domain`, `path`, `path_segments`, `path_prefixes`, `address_prefixes`
- `snippet`, `query`, `source`, `title`, `lang`, `date`
- `robots_allowed`, `train_allowed`, `matched_rule_prefix`, `matched_rule_address_prefix`, `robots_txt_url`
- `content_hash`, `content_hashes`, `chunk_hashes`

This preserves the requirements that came out of the web-indexing design:
- addresses behave like a tree under `domain/dir0/dir1/...`
- `robots.txt` decisions propagate down the tree and stay attached as metadata
- multiple URLs can point to one shared content entry
- repeated passages can point to shared chunk entries
- search-origin snippets remain attached as provenance

### Integration Commands
Install the standalone crawler package into the same Python environment once:

```bash
python3 -m pip install -e ../web-search
```

If you do not install it, `scripts/web_integration/run_web_search_ingest.py` also tries to import directly from the sibling `../web-search` checkout. You can override that location with `WEB_SEARCH_REPO=/path/to/web-search`.

Use the standalone crawler through the Apertus wrapper:

```bash
python3 scripts/web_integration/run_web_search_ingest.py \
  --seed-urls-file seed_urls.txt \
  --output-parquet /tmp/web_fixture.parquet \
  --chunk-output /tmp/web_fixture_chunks.parquet \
  --status-output /tmp/web_fixture_status.jsonl \
  --robots-policy-output /tmp/web_fixture_robots.jsonl
```

That wrapper keeps Apertus thin by reusing the exact `web-search` CLI arguments and output schema.

Index the exported parquet with the Apertus web wrapper:

```bash
python3 scripts/index/index_web_export.py \
  --data-dir /tmp/web_fixture.parquet \
  --index-name web_fixture
```

This delegates to `scripts/index/index_with_metadata.py` while pinning the required web flags:
- `--dataset-type web`
- `--metadata-fields web`

Search the resulting index with the Apertus web wrapper:

```bash
python3 scripts/search/search_web_index.py \
  --es-url http://localhost:9200 \
  --index-name web_fixture \
  --input-string "transformer architecture" \
  --output-dir-base /tmp/web_search_results
```

This delegates to `scripts/search/search.py` while pinning `--dataset web`, so result output keeps URL provenance visible.

### Notes
- The generic Apertus indexer remains multi-dataset. Web records are just one dataset type alongside the existing offline corpora.
- Pages can still be indexed even when not trainable, because the permission decision is carried in metadata rather than being enforced only at query time.
- `scripts/web_ingest/` remains as feature-branch reference code, but the intended modular path is now the sibling `web-search` repo plus these Apertus integration wrappers.
