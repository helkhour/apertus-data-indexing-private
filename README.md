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
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```index.sh```: Script to run indexing for Slurm. \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```index_with_id.sh```: Script to run indexing with id for Slurm. \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```indexing_job_status.py```: Script to automatically evaluate the indexing jobs, detect failures and output statistics. \
&nbsp;&nbsp;&nbsp;&nbsp;|--- *merge* \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```merge.py```: Remote reindex merge script for Elasticsearch. \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```merge.sh```: Slurm script to run merge operation. \
&nbsp;&nbsp;&nbsp;&nbsp;|--- *search* \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```search.py```: Script to perform various search query types across multiple CSV files. \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--- ```search.sh```: . \
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

## Web Ingestion
The repository now includes a URL-preserving web ingestion path under `scripts/web_ingest/` that is shaped by the `rl-webindex` design while keeping Apertus as the final Elasticsearch index/search system.

### Relationship To `rl-webindex`
This implementation does not depend on `rl-webindex` directly.
It reuses the conceptual design from `rl-webindex` such as URL identity, robots handling, and extraction flow, while integrating those ideas into the Apertus indexing pipeline.

### Web Document Schema
Each web page is indexed as one Elasticsearch document per normalized URL.

Each web document includes:
- `document_id`
- `url`
- `domain`
- `path`
- `path_depth`
- `path_prefixes`
- `content_hash`
- `robots_allowed`
- `matched_rule_prefix`
- `query`
- `source`
- `title`
- `lang`
- `date`
- `http_status`
- `fetch_timestamp`
- `text`

`text` remains the main searchable field.
`document_id` is derived from the normalized URL and is stored as a stable hash of that normalized URL by default in web mode.
An optional content-hash-based document id can be selected when intentional duplicate collapsing is desired.
`content_hash` is stored separately for duplicate-content analysis.

Example exported record:

```json
{
  "document_id": "7b5c1b2d...sha256(normalized_url)",
  "url": "https://example.com/article/123",
  "domain": "example.com",
  "path": "/article/123",
  "path_prefixes": ["/", "/article", "/article/123"],
  "content_hash": "abc123...sha256(text)",
  "robots_allowed": true,
  "matched_rule_prefix": "/",
  "text": "This is the extracted article content..."
}
```

### Ingestion Flow

```text
Query
  ↓
RightDao
  ↓
Candidate URLs
  ↓
Robots Evaluation (Protego)
  ↓
Fetch + Extract (Trafilatura)
  ↓
content_hash Computation
  ↓
Export (JSONL/Parquet)
  ↓
Metadata-Aware Apertus Indexing
  ↓
Elasticsearch
```

In short:
`RightDao -> candidate URLs -> Protego robots evaluation -> fetch -> Trafilatura extraction -> content_hash computation -> JSONL/Parquet export -> metadata-aware Apertus indexing -> Elasticsearch`

The metadata-aware indexing step is performed by `scripts/index/index_with_metadata.py`, not the plain text-only `scripts/index/index.py` path.

### Components
- `scripts/web_ingest/rightdao.py`: RightDao adapter with conservative per-query caps and retry-aware request pacing.
- `scripts/web_ingest/robots.py`: Protego-based `robots.txt` fetch, cache, and evaluation with strict multi-agent checks inspired by `rl-webindex`.
- `scripts/web_ingest/fetch_extract.py`: HTTP fetch plus Trafilatura extraction for page text and metadata.
- `scripts/web_ingest/export_web_parquet.py`: Explicit parquet and JSONL writers for indexable rows and audit trails.
- `scripts/web_ingest/run_web_ingest.py`: End-to-end orchestrator for query-driven or seed-URL-driven ingestion.

### Deduplication And Robots Policy
Duplicate-content detection is exposed through `content_hash`.
The default web mode does not deduplicate identical text across different URLs, because one Elasticsearch document maps to one normalized URL.
If collapsing identical content is explicitly desired, `DOCUMENT_ID_MODE=content_hash` can be used instead.

Documents disallowed by `robots.txt` are not indexed, but metadata such as the URL, domain, and matched rule can be retained in the status or audit export.

### Export Format
The canonical web export record is one row per normalized URL with explicit columns such as `document_id`, `url`, `domain`, `path`, `path_depth`, `path_prefixes`, `query`, `text`, `title`, `lang`, `date`, `content_hash`, `robots_allowed`, `matched_rule_prefix`, `http_status`, `fetch_timestamp`, and `source`.

A separate status export can persist `DISALLOWED`, `FAILED`, `EMPTY_EXTRACTION`, and successful `FOUND` outcomes, and a separate robots-policy export can persist domain-level `robots.txt` cache records.

### Failure And Retry Handling
- Failed fetches are skipped from the indexable export and recorded in the status export as `FAILED`.
- Robots-denied URLs are recorded with metadata as `DISALLOWED` and are not fetched.
- Empty or unusable extractions are recorded as `EMPTY_EXTRACTION` and are not indexed.
- The RightDao and fetch layers use bounded retries and conservative caps.
- Raw HTML fallback is not implemented in the current version.

### How To Run
Example query-driven export:

```bash
python3 scripts/web_ingest/run_web_ingest.py \
  --query "climate change news" \
  --rightdao-url "https://<rightdao-endpoint>" \
  --output-parquet /tmp/web_fixture.parquet \
  --status-output /tmp/web_fixture_status.jsonl \
  --robots-policy-output /tmp/web_fixture_robots.jsonl
```

Example indexing step:

```bash
python3 scripts/index/index_with_metadata.py \
  --data-dir /tmp/web_fixture.parquet \
  --index-name web_fixture \
  --dataset-type web \
  --metadata-fields web \
  --document-id-mode url
```

The same indexing path can also be triggered through `scripts/index/index.sh` by setting `DATASET_TYPE=web`.

### Search And Merge
Use `scripts/search/search.py` with `--dataset web` to render URL provenance in search output.
Use `scripts/merge/merge.py --index-config <path>` or let it clone the first source mapping so merged web indexes preserve the explicit metadata fields instead of falling back to the text-only mapping.

### Scale And Execution Model
The ingestion pipeline is batch-oriented and designed to be partitioned across independent query sets or seed URL files.
The downstream Apertus indexing stage already supports large parquet-based batch indexing on SLURM.

### Why This Matters
This web-ingest path is the first step toward:
- provenance-preserving web retrieval
- future web grounding workflows
- incremental URL-based reingestion
- duplicate-content analysis without losing URL identity
- agent-facing search results that include source URLs and robots metadata

### Validation Status
Validated in code:
- Python modules compile successfully.
- Shell wrappers pass syntax checks.
- Web mappings, search rendering, and merge mapping preservation are wired through.

End-to-end ingestion was not executed in the current local shell because the required runtime dependencies were not installed there, but the individual components and integration points were validated for syntax and indexing compatibility.

Still pending validation in the target Clariden environment:
- live RightDao querying
- live fetch/extract runs
- full SLURM/container execution on Clariden
- end-to-end web ingest -> index -> search -> merge smoke tests

### Rate Limits
The web ingest path is intentionally conservative.
Keep low RightDao query rates, cap results per query, cap URLs per domain, cache `robots.txt`, and persist skip or failure states so the system does not repeatedly hit the same domains.
