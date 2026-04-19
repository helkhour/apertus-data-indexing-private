#!/usr/bin/env python3
"""
FineWeb Dataset Indexer for Elasticsearch with Multi-Process Support
Multi-process architecture: N parser workers -> shared queue -> 1 ES consumer
Now with metadata fields support (text, url, lang)
"""

import os
import sys
import time
import logging
import gc
from pathlib import Path
from typing import Generator, Dict, Any, List, Optional
import argparse
import json

import pandas as pd
import pyarrow.parquet as pq
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import RequestError, ConnectionError,  TransportError
import psutil
import tracemalloc


from multiprocessing import Process, Queue, Event, Value, Manager
from queue import Empty
import multiprocessing as mp
from threading import Thread
import signal

from memory_record import (
    build_dataset_memory_record,
    build_web_memory_record,
    infer_web_artifact_kind,
    memory_record_to_es_action,
)

def log_memory_usage(logger, context: str = ""):
    """Improved monitoring focused on your process"""
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        system_memory = psutil.virtual_memory()
        
        logger.info(f"=== METRICS {context} ===")
        
        # Process Memory (what you control)
        logger.info(f"Process Memory:")
        logger.info(f"  RSS: {memory_info.rss / (1024**3):.2f} GB")
        logger.info(f"  VMS: {memory_info.vms / (1024**3):.2f} GB")
        logger.info(f"  % of System: {process.memory_percent():.1f}%")
        
        # System Memory (for awareness)
        logger.info(f"System Memory:")
        logger.info(f"  Total Used: {system_memory.percent:.1f}%")
        logger.info(f"  Available: {system_memory.available / (1024**3):.2f} GB")
        logger.info(f"  Cached: {system_memory.cached / (1024**3):.2f} GB")
       
        # CPU - Process-focused
        cpu_percent = process.cpu_percent(interval=1)
        num_threads = process.num_threads()
        logger.info(f"CPU:")
        logger.info(f"  Process usage: {cpu_percent:.1f}%")
        logger.info(f"  Threads: {num_threads}")
        
        # Try to show affinity
        try:
            affinity = process.cpu_affinity()
            logger.info(f"  Allocated cores: {len(affinity)} (IDs: {affinity[:5]}...)")
        except:
            logger.info(f"  Allocated cores: Unable to determine")
        
        # Disk I/O (your process only)
        try:
            io_counters = process.io_counters()
            logger.info(f"Process I/O:")
            logger.info(f"  Read: {io_counters.read_bytes / (1024**3):.2f} GB")
            logger.info(f"  Write: {io_counters.write_bytes / (1024**3):.2f} GB")
        except:
            pass
        
        logger.info("=" * 50)
        
    except Exception as e:
        logger.warning(f"Could not log memory usage: {e}")


def setup_logging(log_level: str = "INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


def get_elasticsearch_client(host: str = "localhost", port: int = 9200) -> Elasticsearch:
    max_retries = 5
    retry_delay = 10
    
    for attempt in range(max_retries):
        try:
            es = Elasticsearch(
                hosts=[{"host": host, "port": port}],
                timeout=30,
                max_retries=5,
                retry_on_timeout=True
            )
            
            info = es.info()
            logging.info(f"Successfully connected to Elasticsearch at {host}:{port}")
            logging.info(f"Elasticsearch version: {info['version']['number']}")
            return es
            
        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(f"Connection attempt {attempt + 1} failed: {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logging.error(f"Failed to connect to Elasticsearch after {max_retries} attempts")
                raise

def count_documents_in_parquet_files(file_list: List[Path]) -> int:
    """
    Count total documents across artifact files without loading full data.
    """
    logger = logging.getLogger(__name__)
    logger.info("=== Counting Ground Truth Documents ===")
    
    total_docs = 0
    
    for i, file_path in enumerate(file_list):
        try:
            file_path = Path(file_path)
            if file_path.suffix.lower() == ".jsonl":
                with file_path.open("r", encoding="utf-8") as handle:
                    num_rows = sum(1 for line in handle if line.strip())
            else:
                parquet_file = pq.ParquetFile(file_path)
                num_rows = parquet_file.metadata.num_rows
            total_docs += num_rows
            
            if (i + 1) % 10 == 0:
                logger.info(f"Counted {i+1}/{len(file_list)} files: {total_docs:,} docs so far")
                
        except Exception as e:
            logger.error(f"Failed to read metadata from {file_path.name}: {e}")
            raise
    
    logger.info(f"=== Ground Truth: {total_docs:,} total documents in {len(file_list)} files ===")
    return total_docs

def validate_indexing_results(ground_truth_count: int, stats: Dict[str, int], 
                              es: Elasticsearch, index_name: str):
    """
    Validate indexing results against ground truth.
    """
    logger = logging.getLogger(__name__)
    
    indexed_count = stats['indexed']
    failed_count = stats['failed']
    
    # Get actual ES index count
    try:
        index_stats = es.indices.stats(index=index_name)
        es_doc_count = index_stats['indices'][index_name]['total']['docs']['count']
    except Exception as e:
        logger.error(f"Could not query ES index count: {e}")
        es_doc_count = None
    
    logger.info("=" * 70)
    logger.info("=== INDEXING VALIDATION RESULTS ===")
    logger.info(f"Ground truth (raw files):  {ground_truth_count:>15,} documents")
    logger.info(f"Reported indexed:          {indexed_count:>15,} documents")
    logger.info(f"Reported failed:           {failed_count:>15,} documents")
    
    if es_doc_count is not None:
        logger.info(f"ES index actual count:     {es_doc_count:>15,} documents")
    
    logger.info("-" * 70)
    
    # Calculate discrepancies
    success = True
    
    # Check 1: Indexed vs Ground Truth
    indexed_diff = indexed_count - ground_truth_count
    indexed_ratio = indexed_count / ground_truth_count if ground_truth_count > 0 else 0
    
    if abs(indexed_diff) > ground_truth_count * 0.05:  # 5% tolerance for empty docs
        logger.error(f"❌ INDEXED COUNT MISMATCH:")
        logger.error(f"   Difference: {indexed_diff:+,} documents ({(indexed_ratio - 1) * 100:+.1f}%)")
        logger.error(f"   Expected ~{ground_truth_count:,}, got {indexed_count:,}")
        success = False
    else:
        logger.info(f"✓ Indexed count matches ground truth (within 5% tolerance)")
    
    # Check 2: ES Index vs Ground Truth
    if es_doc_count is not None:
        es_diff = es_doc_count - ground_truth_count
        es_ratio = es_doc_count / ground_truth_count if ground_truth_count > 0 else 0
        
        if abs(es_diff) > ground_truth_count * 0.05:
            logger.error(f"❌ ES INDEX COUNT MISMATCH:")
            logger.error(f"   Difference: {es_diff:+,} documents ({(es_ratio - 1) * 100:+.1f}%)")
            logger.error(f"   Expected ~{ground_truth_count:,}, got {es_doc_count:,}")
            
            if es_doc_count > ground_truth_count * 1.1:
                logger.error(f"   ⚠️  SEVERE DUPLICATION DETECTED ({es_ratio:.2f}x)")
                logger.error(f"   This indicates documents were indexed multiple times!")
            
            success = False
        else:
            logger.info(f"✓ ES index count matches ground truth (within 5% tolerance)")
    
    # Check 3: Indexed vs ES Index
    if es_doc_count is not None and abs(indexed_count - es_doc_count) > 1000:
        logger.warning(f"⚠️  WARNING: Reported indexed ({indexed_count:,}) != ES count ({es_doc_count:,})")
        logger.warning(f"   Difference: {es_doc_count - indexed_count:+,} documents")
        logger.warning(f"   This suggests counting errors or duplicate submissions")
    
    logger.info("=" * 70)
    
    if success:
        logger.info("✓✓✓ VALIDATION PASSED - No significant discrepancies detected")
        return True
    else:
        logger.error("❌❌❌ VALIDATION FAILED - Data integrity issues detected!")
        logger.error("Recommended actions:")
        logger.error("  1. Delete the corrupted index")
        logger.error("  2. Add document IDs to prevent duplicates (see hash-based solution)")
        logger.error("  3. Rerun indexing with fixed code")
        return False

def get_file_range(data_dir_pattern: str, file_range_start: int = None, file_range_end: int = None) -> tuple:
    """
    Get parquet files from multiple directories matching a pattern or from single data directory. 
    Optional range selection
    """
    logger = logging.getLogger(__name__)
    
    # Find all directories matching the pattern
    import glob
    matching_dirs = sorted(glob.glob(data_dir_pattern))
    
    if not matching_dirs:
        raise FileNotFoundError(f"No directories found matching pattern: {data_dir_pattern}")
    
    logger.info(f"Found {len(matching_dirs)} directories matching pattern")
    
    # Collect all parquet files from all matching directories
    all_parquet_files = []
    for dir_path in matching_dirs:
        dir_parquet_files = sorted(list(Path(dir_path).glob("*.parquet")))
        # logger.info(f"Found {len(dir_parquet_files)} parquet files in {Path(dir_path).name}")
        all_parquet_files.extend(dir_parquet_files)
    
    all_parquet_files = sorted(all_parquet_files)
    total_files = len(all_parquet_files)
    
    if total_files == 0:
        raise FileNotFoundError(f"No parquet files found in any matching directories")
    
    logger.info(f"Total parquet files across all directories: {total_files}")
    
    # Apply file range if specified
    if file_range_start is not None and file_range_end is not None:
        if file_range_start < 0 or file_range_start >= total_files:
            raise ValueError(f"file_range_start ({file_range_start}) is out of bounds [0, {total_files})")
        
        if file_range_end <= file_range_start or file_range_end > total_files:
            raise ValueError(f"file_range_end ({file_range_end}) must be > start ({file_range_start}) and <= {total_files}")
        
        selected_files = all_parquet_files[file_range_start:file_range_end]
        logger.info(f"Selected file range {file_range_start}:{file_range_end}")
    else:
        selected_files = all_parquet_files
        logger.info(f"Processing all {total_files} files")
    
    # Calculate total size
    total_size_bytes = 0
    for file_path in selected_files:
        try:
            total_size_bytes += file_path.stat().st_size
        except OSError as e:
            logger.warning(f"Could not get size for {file_path.name}: {e}")
    
    total_size_gb = total_size_bytes / (1024**3)
    
    logger.info(f"Total raw data size: {total_size_gb:.2f} GB")
    logger.info(f"First file: {selected_files[0]}")
    logger.info(f"Last file: {selected_files[-1]}")
    
    return selected_files, total_size_gb


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if isinstance(row, dict):
        return row
    if hasattr(row, "to_dict"):
        return row.to_dict()
    return dict(row)


def _parse_document(
    row,
    index_name: str,
    metadata_fields: str,
    record_schema: str = "legacy",
    input_mode: str = "dataset",
    web_artifact_hint: Optional[str] = None,
):
    """Parse a source row into an Elasticsearch bulk action."""
    row_dict = _row_to_dict(row)

    if record_schema == "memory_v1":
        if input_mode == "web-artifacts":
            record = build_web_memory_record(row_dict, artifact_hint=web_artifact_hint)
        else:
            record = build_dataset_memory_record(row_dict)
        if record is None:
            return None
        return memory_record_to_es_action(record, index_name=index_name)

    text_str = row_dict.get("text")
    if pd.isna(text_str) or not str(text_str).strip():
        return None

    text_str = str(text_str).strip()
    if len(text_str) > 100000:
        text_str = text_str[:100000] + "... [TRUNCATED]"

    doc_source = {"text": text_str}

    if metadata_fields in ["text-url", "text-url-lang"]:
        try:
            if isinstance(row_dict.get("metadata"), dict):
                metadata_dict = row_dict["metadata"]
            else:
                metadata_dict = json.loads(str(row_dict.get("metadata", "{}")))

            doc_source["url"] = metadata_dict.get("url", "")

            if metadata_fields == "text-url-lang":
                doc_source["lang"] = metadata_dict.get("lang", "")
        except Exception:
            doc_source["url"] = ""
            if metadata_fields == "text-url-lang":
                doc_source["lang"] = ""

    return {
        "_index": index_name,
        "_source": doc_source,
    }


def create_index_config(
    num_shards: int = 5, num_replicas: int = 0, metadata_fields: str = "text"
) -> Dict[str, Any]:
    """
    Create index configuration based on metadata field selection.
    
    Args:
        num_shards: Number of shards
        num_replicas: Number of replicas
        metadata_fields: Which fields to include:
            - "text": Only text field
            - "text-url": Text and URL (stored but not indexed)
            - "text-url-lang": Text, URL, and language (stored but not indexed)
    """
    
    # Build properties based on metadata_fields
    properties = {
        "text": {
            "type": "text",
            "analyzer": "web_content_analyzer",
            "index_options": "positions",
            "norms": True,
            "store": False
        }
    }
    
    # Add URL field (stored, not indexed)
    if metadata_fields in ["text-url", "text-url-lang"]:
        properties["url"] = {
            "type": "keyword",
            "index": False,
            "store": False
        }
    
    # Add language field (stored, not indexed)
    if metadata_fields == "text-url-lang":
        properties["lang"] = {
            "type": "keyword",
            "index": False,
            "store": False
        }
    
    # Build _source includes based on metadata_fields
    source_includes = ["text"]
    if metadata_fields in ["text-url", "text-url-lang"]:
        source_includes.append("url")
    if metadata_fields == "text-url-lang":
        source_includes.append("lang")
    
    return {
        "settings": {
            "number_of_shards": num_shards,
            "number_of_replicas": num_replicas,
            "refresh_interval": "60s",
            "index": {
                "codec": "best_compression",
                "max_result_window": 50000
            },
            "analysis": {
                "analyzer": {
                    "web_content_analyzer": {
                        "type": "custom",
                        "char_filter": ["html_strip"],
                        "tokenizer": "standard",
                        "filter": ["lowercase", "asciifolding"]
                    }
                }
            }
        },
        "mappings": {
            "dynamic": "false",
            "_source": {
                "includes": source_includes,
                "excludes": []
            },
            "properties": properties
        }
    }


def create_memory_v1_index_config(
    num_shards: int = 5, num_replicas: int = 0
) -> Dict[str, Any]:
    return {
        "settings": {
            "number_of_shards": num_shards,
            "number_of_replicas": num_replicas,
            "refresh_interval": "60s",
            "index": {
                "codec": "best_compression",
                "max_result_window": 50000,
            },
            "analysis": {
                "analyzer": {
                    "memory_text_analyzer": {
                        "type": "custom",
                        "char_filter": ["html_strip"],
                        "tokenizer": "standard",
                        "filter": ["lowercase", "asciifolding"],
                    },
                    "memory_exact_analyzer": {
                        "type": "custom",
                        "char_filter": ["html_strip"],
                        "tokenizer": "standard",
                        "filter": ["lowercase", "asciifolding"],
                    },
                }
            },
        },
        "mappings": {
            "dynamic": "false",
            "_source": {
                "includes": [
                    "record_id",
                    "parent_id",
                    "record_type",
                    "source_type",
                    "search_text",
                    "data",
                    "representations",
                    "agent",
                ],
                "excludes": [],
            },
            "properties": {
                "record_id": {"type": "keyword"},
                "parent_id": {"type": "keyword"},
                "record_type": {"type": "keyword"},
                "source_type": {"type": "keyword"},
                "search_text": {
                    "type": "text",
                    "analyzer": "memory_text_analyzer",
                    "index_options": "positions",
                    "norms": True,
                    "store": False,
                    "fields": {
                        "exact": {
                            "type": "text",
                            "analyzer": "memory_exact_analyzer",
                            "index_options": "positions",
                        }
                    },
                },
                "data": {"type": "object"},
                "representations": {"type": "object"},
                "agent": {"type": "object"},
            },
        },
    }


def calculate_optimal_shards_by_size(data_size_gb: float, 
                                   min_shard_size_gb: float = 10, 
                                   max_shard_size_gb: float = 50,
                                   es_expansion_factor: float = 3.0) -> int:
    """
    Calculate optimal number of shards based only on data size constraints.
    
    Args:
        data_size_gb: Raw data size in GB (parquet files)
        min_shard_size_gb: Minimum shard size in GB (default 10GB)
        max_shard_size_gb: Maximum shard size in GB (default 50GB)  
        es_expansion_factor: Factor for ES indexed size vs raw size (default 3x)
        
    Returns:
        Optimal number of shards
    """
    logger = logging.getLogger(__name__)
    
    # Estimate indexed size (ES typically 2-4x larger than compressed parquet)
    estimated_indexed_size_gb = data_size_gb * es_expansion_factor
    
    logger.info(f"=== SHARD CALCULATION (SIZE-BASED ONLY) ===")
    logger.info(f"Raw data size: {data_size_gb:.2f} GB")
    logger.info(f"Estimated ES indexed size: {estimated_indexed_size_gb:.2f} GB (factor: {es_expansion_factor}x)")
    logger.info(f"Target shard size range: {min_shard_size_gb}-{max_shard_size_gb} GB")
    
    # Use target of 30GB per shard (middle of 10-50GB range)
    target_shard_size_gb = (min_shard_size_gb + max_shard_size_gb) / 2
    
    # Calculate shards needed - ensure at least 1 shard
    optimal_shards = max(1, int(estimated_indexed_size_gb / target_shard_size_gb))
    
    # CRITICAL: Enforce minimum 2 shards for datasets > 15GB to avoid single-shard bottleneck
    if data_size_gb > 15 and optimal_shards < 2:
        optimal_shards = 2
        logger.info(f"Enforcing minimum 2 shards for dataset > 15GB (single-shard bottleneck prevention)")
    
    # Validate the result doesn't create shards too large
    size_per_shard_gb = estimated_indexed_size_gb / optimal_shards
    
    # If shards would be too large, increase shard count
    if size_per_shard_gb > max_shard_size_gb:
        # optimal_shards = max(2, int(estimated_indexed_size_gb / max_shard_size_gb)) + 1
        optimal_shards = max(2 if data_size_gb > 15 else 1, int(estimated_indexed_size_gb / max_shard_size_gb) + 1)
        size_per_shard_gb = estimated_indexed_size_gb / optimal_shards
        logger.info(f"Adjusted shard count to prevent oversized shards")
    
    logger.info(f"Calculation results:")
    logger.info(f"  Target shard size: {target_shard_size_gb:.1f} GB")
    logger.info(f"  Calculated shards: {optimal_shards}")
    logger.info(f"  Actual size per shard: {size_per_shard_gb:.1f} GB")
    
    # Validation warnings
    if size_per_shard_gb > max_shard_size_gb:
        logger.warning(f"WARNING: Size per shard ({size_per_shard_gb:.1f}GB) exceeds max ({max_shard_size_gb}GB)")
    
    if size_per_shard_gb < min_shard_size_gb:
        logger.warning(f"INFO: Size per shard ({size_per_shard_gb:.1f}GB) below min ({min_shard_size_gb}GB) - this is OK for smaller datasets")
    
    logger.info("=" * 45)
    
    return optimal_shards

def create_index_with_size_based_shards(es: Elasticsearch, index_name: str, 
                                       total_data_size_gb: float, config_file: str = None,
                                       min_shard_size_gb: float = 10, max_shard_size_gb: float = 50,
                                       es_expansion_factor: float = 3.0, metadata_fields: str = "text",
                                       record_schema: str = "legacy") -> bool:
    """Create Elasticsearch index with size-based dynamic shard count."""
    try:
        if es.indices.exists(index=index_name):
            logging.warning(f"Index '{index_name}' already exists. Deleting...")
            es.indices.delete(index=index_name)

        # Calculate optimal shard count based on size only
        optimal_shards = calculate_optimal_shards_by_size(
            total_data_size_gb, min_shard_size_gb, max_shard_size_gb, es_expansion_factor
        )
        
        if config_file and os.path.exists(config_file):
            logging.info(f"Loading index configuration from: {config_file}")
            with open(config_file, 'r') as f:
                config = json.load(f)
            # Override shard count in loaded config
            config["settings"]["number_of_shards"] = optimal_shards
            logging.info(f"Overrode shard count in config file to: {optimal_shards}")
        
        else:
            logging.info("Using default index configuration")
            if record_schema == "memory_v1":
                config = create_memory_v1_index_config(num_shards=optimal_shards)
            else:
                config = create_index_config(num_shards=optimal_shards, metadata_fields=metadata_fields)
        
        
        # Log final configuration
        logging.info(f"Creating index '{index_name}' with:")
        logging.info(f"  Shards: {config['settings']['number_of_shards']}")
        logging.info(f"  Replicas: {config['settings']['number_of_replicas']}")
        logging.info(f"  Refresh interval: {config['settings']['refresh_interval']}")
        logging.info(f"  Metadata fields: {metadata_fields}")
        logging.info(f"  Record schema: {record_schema}")
        
        es.indices.create(index=index_name, body=config)
        logging.info(f"Created index '{index_name}' with configuration")
        return True
        
    except Exception as e:
        logging.error(f"Failed to create index: {e}")
        return False



def force_memory_cleanup():
    """Aggressive memory cleanup"""
    logger = logging.getLogger(__name__)
    
    try:
        for i in range(3):
            collected = gc.collect()
            logger.debug(f"GC pass {i+1}: collected {collected} objects")
        
        try:
            import pyarrow as pa
            pool = pa.default_memory_pool()
            pool.release_unused()
        except Exception as e:
            logger.debug(f"PyArrow cleanup failed: {e}")
            
    except Exception as e:
        logger.warning(f"Memory cleanup failed: {e}")


def _artifact_hint_from_path(file_path: Path) -> str:
    name = file_path.name.lower()
    if "status" in name:
        return "status"
    if "chunk" in name:
        return "chunk"
    return "content"


def _build_web_columns_to_read(parquet_file: pq.ParquetFile) -> List[str]:
    available = set(parquet_file.schema.names)
    preferred = [
        "document_id",
        "content",
        "content_hash",
        "content_hashes",
        "chunk_hashes",
        "url",
        "path",
        "query",
        "title",
        "lang",
        "date",
        "robots_allowed",
        "http_status",
        "fetch_timestamp",
        "redirect_url",
        "status",
        "error",
        "text",
    ]
    return [col for col in preferred if col in available]


# ============================================================================
# SINGLE-PROCESS FUNCTIONS (for backward compatibility with num_workers=1)
# ============================================================================


def process_single_parquet_file_streaming(file_path: Path, chunk_size: int, 
                                        index_name: str, metadata_fields: str = "text",
                                        record_schema: str = "legacy",
                                        input_mode: str = "dataset") -> Generator[Dict[str, Any], None, None]:
    """
    Process a single parquet file using streaming approach to prevent memory leaks.
    Now includes metadata fields support.
    """
    logger = logging.getLogger(__name__)
    parquet_file = None
    
    try:
        logger.info(f"Opening parquet file for streaming: {file_path.name}")
        
        # Use PyArrow ParquetFile for streaming access
        parquet_file = pq.ParquetFile(file_path)
        
        # Get metadata without loading data
        metadata = parquet_file.metadata
        num_rows = metadata.num_rows
        num_row_groups = metadata.num_row_groups
        
        logger.info(f"File has {num_rows:,} rows in {num_row_groups} row groups")
        
        processed_rows = 0
        
        if input_mode == "web-artifacts":
            columns_to_read = _build_web_columns_to_read(parquet_file)
            artifact_hint = _artifact_hint_from_path(file_path)
        else:
            columns_to_read = ['text']
            if metadata_fields in ["text-url", "text-url-lang"]:
                columns_to_read.append('metadata')
            artifact_hint = None
        
        # Process each row group individually
        for row_group_idx in range(num_row_groups):
            try:
                # Read required columns
                row_group = parquet_file.read_row_group(row_group_idx, columns=columns_to_read)
                
                # Convert to pandas DataFrame
                df = row_group.to_pandas()
                
                # Check for required columns
                if input_mode != "web-artifacts" and 'text' not in df.columns:
                    logger.warning(f"Missing 'text' column in row group {row_group_idx}, skipping...")
                    continue
                
                row_group_size = len(df)
                logger.debug(f"Processing row group {row_group_idx}: {row_group_size:,} rows")
                
                # Process in smaller chunks within the row group
                for chunk_start in range(0, row_group_size, chunk_size):
                    chunk_end = min(chunk_start + chunk_size, row_group_size)
                    chunk = df.iloc[chunk_start:chunk_end].copy()
                    
                    # Process chunk and yield documents
                    for _, row in chunk.iterrows():
                        row_dict = _row_to_dict(row)
                        web_kind = (
                            infer_web_artifact_kind(row_dict, artifact_hint=artifact_hint)
                            if input_mode == "web-artifacts"
                            else None
                        )
                        doc = _parse_document(
                            row_dict,
                            index_name,
                            metadata_fields,
                            record_schema=record_schema,
                            input_mode=input_mode,
                            web_artifact_hint=web_kind,
                        )
                        if doc:
                            yield doc
                    
                    processed_rows += len(chunk)
                    
                    # Cleanup chunk immediately
                    del chunk
                    
                    # Force garbage collection every 10k rows
                    if processed_rows % 10000 == 0:
                        gc.collect()
                        logger.debug(f"Processed {processed_rows:,}/{num_rows:,} rows")
                
                # Cleanup row group data
                del df
                del row_group
                
                # Force garbage collection after each row group
                gc.collect()
                
            except Exception as e:
                logger.error(f"Error processing row group {row_group_idx}: {e}")
                continue
        
        logger.info(f"Completed streaming processing of {file_path.name}: {processed_rows:,} rows")
        
    except Exception as e:
        logger.error(f"Error opening parquet file {file_path.name}: {e}")
        raise
    finally:
        # Critical: Cleanup parquet file handle
        if parquet_file is not None:
            parquet_file = None
        
        # Force PyArrow memory cleanup
        try:
            import pyarrow as pa
            pool = pa.default_memory_pool()
            logger.debug(f"PyArrow memory pool bytes allocated: {pool.bytes_allocated()}")
            pool.release_unused()
        except Exception as e:
            logger.warning(f"Could not cleanup PyArrow memory pool: {e}")
        
        # Final garbage collection
        gc.collect()



def process_single_jsonl_file_streaming(
    file_path: Path,
    index_name: str,
    metadata_fields: str = "text",
    record_schema: str = "legacy",
    input_mode: str = "web-artifacts",
) -> Generator[Dict[str, Any], None, None]:
    """Process a JSONL artifact file line-by-line."""
    logger = logging.getLogger(__name__)
    logger.info(f"Opening jsonl file for streaming: {file_path.name}")

    with file_path.open("r", encoding="utf-8") as handle:
        for line_num, line in enumerate(handle, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                row_dict = json.loads(raw)
            except json.JSONDecodeError as exc:
                logger.warning(f"Skipping malformed json line {line_num} in {file_path.name}: {exc}")
                continue

            web_kind = infer_web_artifact_kind(row_dict, artifact_hint=_artifact_hint_from_path(file_path))
            doc = _parse_document(
                row_dict,
                index_name,
                metadata_fields,
                record_schema=record_schema,
                input_mode=input_mode,
                web_artifact_hint=web_kind,
            )
            if doc:
                yield doc


def bulk_index_documents(es: Elasticsearch, doc_generator: Generator, batch_size: int, 
                        max_chunk_bytes: int, thread_count: int, queue_size: int,
                        global_stats: Dict[str, int], start_time: float) -> Dict[str, int]:
    """Bulk index documents with progress tracking"""
    logger = logging.getLogger(__name__)
    stats = {"indexed": 0, "failed": 0}
    
    try:
        for success, info in helpers.parallel_bulk(
            es,
            doc_generator,
            chunk_size=batch_size,
            max_chunk_bytes=max_chunk_bytes * 1024 * 1024,
            thread_count=thread_count,
            queue_size=queue_size,
            request_timeout=300,
        ):
            if success:
                stats["indexed"] += 1
                global_stats["indexed"] += 1
            else:
                stats["failed"] += 1
                global_stats["failed"] += 1
                logger.error(f"Failed to index document: {info}")
            
            # Log progress every 10,000 documents
            if global_stats["indexed"] % 10000 == 0:
                current_time = time.time()
                elapsed = current_time - start_time
                rate = global_stats["indexed"] / elapsed if elapsed > 0 else 0
                
                logger.info(
                    f"Progress: {global_stats['indexed']:,} indexed, {global_stats['failed']:,} failed, "
                    f"Rate: {rate:.1f} docs/sec, Elapsed: {elapsed:.1f}s"
                )
                
                # Log memory usage every 10k docs
                log_memory_usage(logger, f"BULK INDEXING AT {global_stats['indexed']} DOCS")
                
                # Force garbage collection during bulk indexing
                gc.collect()
        
    except Exception as e:
        logger.error(f"Bulk indexing error: {e}")
        raise
    
    return stats


# ============================================================================
# MULTI-PROCESS FUNCTIONS (NEW)
# ============================================================================

def parse_parquet_worker(file_path, chunk_size, index_name, doc_queue, 
                        docs_parsed_counter, worker_id, stop_event, metadata_fields="text",
                        record_schema="legacy", input_mode="dataset"):
    """Compatibility wrapper that forwards one file to the batch worker."""
    parse_parquet_worker_batch(
        [file_path],
        chunk_size,
        index_name,
        doc_queue,
        docs_parsed_counter,
        worker_id,
        stop_event,
        metadata_fields=metadata_fields,
        record_schema=record_schema,
        input_mode=input_mode,
    )


def parse_parquet_worker_batch(file_list, chunk_size, index_name, doc_queue, 
                               docs_parsed_counter, worker_id, stop_event, metadata_fields="text",
                               record_schema="legacy", input_mode="dataset"):
    """Worker that processes multiple artifact files and sends parsed actions to queue."""
    logger = logging.getLogger(f"parser-{worker_id}")
    logger.info(f"Worker {worker_id} starting to parse {len(file_list)} files")
    
    total_docs_parsed = 0
    
    try:
        for file_idx, file_path in enumerate(file_list):
            if stop_event.is_set():
                logger.warning(f"Worker {worker_id} received stop signal")
                break

            file_path = Path(file_path)
            logger.info(f"Worker {worker_id}: Processing file {file_idx+1}/{len(file_list)}: {file_path.name}")
            
            docs_parsed = 0
            batch_buffer = []
            batch_size = 1000
            
            try:
                if input_mode == "web-artifacts" and file_path.suffix.lower() == ".jsonl":
                    with file_path.open("r", encoding="utf-8") as handle:
                        for line_num, line in enumerate(handle, start=1):
                            if stop_event.is_set():
                                break
                            raw = line.strip()
                            if not raw:
                                continue
                            try:
                                row_dict = json.loads(raw)
                            except json.JSONDecodeError as exc:
                                logger.warning(
                                    f"Worker {worker_id}: skipping malformed json line {line_num} "
                                    f"in {file_path.name}: {exc}"
                                )
                                continue

                            web_kind = infer_web_artifact_kind(
                                row_dict, artifact_hint=_artifact_hint_from_path(file_path)
                            )
                            doc = _parse_document(
                                row_dict,
                                index_name,
                                metadata_fields,
                                record_schema=record_schema,
                                input_mode=input_mode,
                                web_artifact_hint=web_kind,
                            )
                            if doc:
                                batch_buffer.append(doc)
                                docs_parsed += 1
                                if len(batch_buffer) >= batch_size:
                                    doc_queue.put(batch_buffer)
                                    with docs_parsed_counter.get_lock():
                                        docs_parsed_counter.value += len(batch_buffer)
                                    batch_buffer = []

                else:
                    parquet_file = pq.ParquetFile(file_path)
                    total_rows = parquet_file.metadata.num_rows
                    num_row_groups = parquet_file.metadata.num_row_groups
                    logger.info(
                        f"Worker {worker_id}: File has {total_rows:,} rows in {num_row_groups} row groups"
                    )

                    if input_mode == "web-artifacts":
                        columns_to_read = _build_web_columns_to_read(parquet_file)
                        artifact_hint = _artifact_hint_from_path(file_path)
                    else:
                        columns_to_read = ["text"]
                        if metadata_fields in ["text-url", "text-url-lang"]:
                            columns_to_read.append("metadata")
                        artifact_hint = None

                    for row_group_idx in range(num_row_groups):
                        if stop_event.is_set():
                            logger.warning(f"Worker {worker_id} received stop signal")
                            break

                        try:
                            row_group = parquet_file.read_row_group(row_group_idx, columns=columns_to_read)
                            df = row_group.to_pandas()

                            if input_mode != "web-artifacts" and "text" not in df.columns:
                                logger.warning(f"Missing 'text' column in row group {row_group_idx}, skipping...")
                                continue

                            row_group_size = len(df)

                            for start_idx in range(0, row_group_size, chunk_size):
                                if stop_event.is_set():
                                    break

                                chunk_end = min(start_idx + chunk_size, row_group_size)
                                chunk = df.iloc[start_idx:chunk_end].copy()

                                for _, row in chunk.iterrows():
                                    row_dict = _row_to_dict(row)
                                    web_kind = (
                                        infer_web_artifact_kind(row_dict, artifact_hint=artifact_hint)
                                        if input_mode == "web-artifacts"
                                        else None
                                    )
                                    doc = _parse_document(
                                        row_dict,
                                        index_name,
                                        metadata_fields,
                                        record_schema=record_schema,
                                        input_mode=input_mode,
                                        web_artifact_hint=web_kind,
                                    )
                                    if doc:
                                        batch_buffer.append(doc)
                                        docs_parsed += 1

                                        if len(batch_buffer) >= batch_size:
                                            doc_queue.put(batch_buffer)
                                            with docs_parsed_counter.get_lock():
                                                docs_parsed_counter.value += len(batch_buffer)
                                            batch_buffer = []

                                del chunk

                                if docs_parsed > 0 and docs_parsed % 50000 == 0:
                                    logger.info(
                                        f"Worker {worker_id}: Parsed {docs_parsed:,} documents from current file"
                                    )
                                    gc.collect()

                            del df, row_group
                            gc.collect()

                        except Exception as e:
                            logger.error(f"Worker {worker_id} error processing row group {row_group_idx}: {e}")
                            continue

                if batch_buffer:
                    doc_queue.put(batch_buffer)
                    with docs_parsed_counter.get_lock():
                        docs_parsed_counter.value += len(batch_buffer)
                    batch_buffer = []

                total_docs_parsed += docs_parsed
                logger.info(f"Worker {worker_id}: Completed file {file_path.name} - {docs_parsed:,} documents")

            except Exception as e:
                logger.error(f"Worker {worker_id} failed on file {file_path.name}: {e}")
                continue

            finally:
                force_memory_cleanup()
        
        doc_queue.put(None)
        logger.info(f"Worker {worker_id} completed all files: Parsed {total_docs_parsed:,} total documents")
        
    except Exception as e:
        logger.error(f"Worker {worker_id} failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        doc_queue.put(None)
    
    finally:
        force_memory_cleanup()

def elasticsearch_consumer(doc_queue, es, batch_size, max_chunk_bytes, 
                          thread_count, queue_size, num_workers, 
                          stats_dict, stop_event):
    """
    Consumer that pulls documents from queue and bulk indexes to ES
    This runs in the main process and manages the single ES client
    """
    logger = logging.getLogger("es-consumer")
    logger.info("ES Consumer started")
    
    indexed = 0
    failed = 0
    workers_completed = 0
    start_time = time.time()
    
    def document_generator():
        """Generator that yields documents from the queue"""
        nonlocal workers_completed
        
        while True:
            try:
                batch = doc_queue.get(timeout=5)
                
                if batch is None:
                    workers_completed += 1
                    logger.info(f"Worker completed ({workers_completed}/{num_workers})")
                    
                    if workers_completed >= num_workers:
                        logger.info("All workers completed, finishing indexing")
                        break
                    continue
                
                for doc in batch:
                    yield doc
                    
            except Empty:
                if stop_event.is_set():
                    logger.warning("Stop event set, finishing indexing")
                    break
                continue
            except Exception as e:
                logger.error(f"Error getting documents from queue: {e}")
                break
    
    try:
        logger.info(f"Starting bulk indexing with {thread_count} threads")
        
        for success, info in helpers.parallel_bulk(
            es,
            document_generator(),
            chunk_size=batch_size,
            max_chunk_bytes=max_chunk_bytes * 1024 * 1024,
            thread_count=thread_count,
            queue_size=queue_size,
            request_timeout=300, 
        ):
            if success:
                indexed += 1
            else:
                failed += 1
                logger.error(f"Failed to index document: {info}")
            
            if indexed % 10000 == 0:
                elapsed = time.time() - start_time
                rate = indexed / elapsed if elapsed > 0 else 0
                queue_depth = doc_queue.qsize()
                
                logger.info(
                    f"Progress: {indexed:,} indexed, {failed:,} failed, "
                    f"Rate: {rate:.1f} docs/sec, Queue: {queue_depth}, "
                    f"Elapsed: {elapsed:.1f}s"
                )
                
                if indexed % 100000 == 0:
                    log_memory_usage(logger, f"ES CONSUMER AT {indexed} DOCS")
                    gc.collect()
        
        elapsed = time.time() - start_time
        rate = indexed / elapsed if elapsed > 0 else 0
        
        logger.info(f"=== ES Consumer Completed ===")
        logger.info(f"Total indexed: {indexed:,}")
        logger.info(f"Total failed: {failed:,}")
        logger.info(f"Final rate: {rate:.1f} docs/sec")
        logger.info(f"Total time: {elapsed:.1f}s")
        
        stats_dict['indexed'] = indexed
        stats_dict['failed'] = failed
        stats_dict['elapsed'] = elapsed
        
    except Exception as e:
        logger.error(f"ES Consumer error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        stats_dict['indexed'] = indexed
        stats_dict['failed'] = failed
        stats_dict['error'] = str(e)


def process_file_list(file_list, chunk_size, index_name, es, batch_size,
                     max_chunk_bytes, thread_count, queue_size, num_workers=1, metadata_fields="text",
                     record_schema="legacy", input_mode="dataset"):
    """
    Process files with multi-process parsing and single ES client.
    Now supports metadata fields.
    """
    logger = logging.getLogger(__name__)
    
    if not file_list:
        raise ValueError("No files provided to process")
    
    logger.info(
        f"Processing {len(file_list)} files with metadata_fields='{metadata_fields}', "
        f"record_schema='{record_schema}', input_mode='{input_mode}'"
    )
    
    # Single-process mode (backward compatible)
    if num_workers == 1:
        logger.info("Running in single-process mode (backward compatible)")
        global_stats = {"indexed": 0, "failed": 0, "files_processed": 0}
        start_time = time.time()
        
        for i, file_path in enumerate(file_list):
            file_path = Path(file_path)
            logger.info(f"Processing file {i+1}/{len(file_list)}: {file_path.name}")
            log_memory_usage(logger, f"BEFORE FILE {i+1}")
            
            try:
                if input_mode == "web-artifacts" and file_path.suffix.lower() == ".jsonl":
                    doc_generator = process_single_jsonl_file_streaming(
                        file_path,
                        index_name,
                        metadata_fields,
                        record_schema=record_schema,
                        input_mode=input_mode,
                    )
                else:
                    doc_generator = process_single_parquet_file_streaming(
                        file_path,
                        chunk_size,
                        index_name,
                        metadata_fields,
                        record_schema=record_schema,
                        input_mode=input_mode,
                    )
                
                file_stats = bulk_index_documents(
                    es, doc_generator, batch_size, max_chunk_bytes,
                    thread_count, queue_size, global_stats, start_time
                )
                
                global_stats["files_processed"] += 1
                logger.info(f"File {i+1} completed: {file_stats['indexed']:,} indexed")
                
            except Exception as e:
                logger.error(f"Failed to process file {file_path.name}: {e}")
                continue
            finally:
                force_memory_cleanup()
                log_memory_usage(logger, f"AFTER FILE {i+1}")
        
        return global_stats
    
    # Multi-process mode with shared queue
    actual_workers = min(num_workers, len(file_list))

    logger.info(f"=== Starting Multi-Process Mode ===")
    logger.info(f"Requested workers: {num_workers}")
    logger.info(f"Files: {len(file_list)}")
    logger.info(f"Actual workers: {actual_workers}")
    logger.info(f"Metadata fields: {metadata_fields}")
    logger.info(f"Record schema: {record_schema}")
    logger.info(f"Input mode: {input_mode}")
    logger.info(f"Architecture: {actual_workers} parser workers -> shared queue -> 1 ES consumer")
    
    doc_queue = Queue(maxsize=300) 
    docs_parsed_counter = Value('i', 0)
    stop_event = Event()
    
    manager = mp.Manager()
    stats_dict = manager.dict()
    stats_dict['indexed'] = 0
    stats_dict['failed'] = 0
    
    overall_start = time.time()
    
    try:
        workers = []

        # Distribute files across workers using round-robin
        for worker_id in range(actual_workers):
            # Each worker gets every Nth file starting from its ID
            worker_files = file_list[worker_id::actual_workers]
            
            logger.info(f"Worker {worker_id} will process {len(worker_files)} files")
            
            #Pass list of files instead of single file
            worker = Process(
                target=parse_parquet_worker_batch,  
                args=(worker_files, chunk_size, index_name, doc_queue, 
                      docs_parsed_counter, worker_id, stop_event, metadata_fields, record_schema, input_mode)
            )
            worker.start()
            workers.append(worker)
        
        logger.info(f"All {len(workers)} parser workers started")
        
        consumer_thread = Thread(
            target=elasticsearch_consumer,
            args=(doc_queue, es, batch_size, max_chunk_bytes, 
                  thread_count, queue_size, len(workers), stats_dict, stop_event)
        )
        consumer_thread.start()
        logger.info("ES consumer thread started")
        
        last_count = 0
        while consumer_thread.is_alive():
            consumer_thread.join(timeout=10)
            
            current_parsed = docs_parsed_counter.value
            current_indexed = stats_dict.get('indexed', 0)
            
            if current_parsed > last_count:
                logger.info(f"Overall progress: Parsed {current_parsed:,}, "
                          f"Indexed {current_indexed:,}, Queue: {doc_queue.qsize()}")
                last_count = current_parsed
        
        logger.info("Waiting for parser workers to complete...")
        for i, worker in enumerate(workers):
            worker.join(timeout=30)
            if worker.is_alive():
                logger.warning(f"Worker {i} did not finish, terminating")
                worker.terminate()
        
        logger.info("All workers completed")
        
        elapsed = time.time() - overall_start
        final_indexed = stats_dict.get('indexed', 0)
        final_failed = stats_dict.get('failed', 0)
        final_parsed = docs_parsed_counter.value
        
        logger.info(f"=== Multi-Process Completion ===")
        logger.info(f"Total documents parsed: {final_parsed:,}")
        logger.info(f"Total documents indexed: {final_indexed:,}")
        logger.info(f"Total failed: {final_failed:,}")
        logger.info(f"Overall time: {elapsed:.1f}s")
        logger.info(f"Overall rate: {final_indexed/elapsed:.1f} docs/sec")
        
        return {
            "indexed": final_indexed,
            "failed": final_failed,
            "files_processed": len(workers),
            "elapsed": elapsed
        }
        
    except KeyboardInterrupt:
        logger.warning("Received interrupt, stopping workers...")
        stop_event.set()
        for worker in workers:
            worker.terminate()
        raise
        
    except Exception as e:
        logger.error(f"Multi-process execution failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        stop_event.set()
        for worker in workers:
            worker.terminate()
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Index FineWeb dataset into Elasticsearch - Multi-process shared queue architecture with metadata support"
    )
    parser.add_argument("--data-dir", required=False, help="Directory containing parquet files (dataset mode)")
    parser.add_argument(
        "--record-schema",
        type=str,
        default="memory_v1",
        choices=["legacy", "memory_v1"],
        help="Record schema to index. 'memory_v1' enables canonical MemoryRecord indexing.",
    )
    parser.add_argument(
        "--input-mode",
        type=str,
        default="dataset",
        choices=["dataset", "web-artifacts"],
        help="Source input mode: dataset parquet files or web-search artifacts.",
    )
    parser.add_argument("--web-content-parquet", help="Path to web-search content.parquet")
    parser.add_argument("--web-chunks-parquet", help="Path to web-search chunks.parquet")
    parser.add_argument("--web-status", help="Path to web-search status artifact (.jsonl or .parquet)")
    
    parser.add_argument("--file-range-start", type=int, help="Starting file index (0-based, inclusive)")
    parser.add_argument("--file-range-end", type=int, help="Ending file index (exclusive)")
    
    parser.add_argument("--min-shard-size", type=float, default=10.0, 
                       help="Minimum shard size in GB (default: 10)")
    parser.add_argument("--max-shard-size", type=float, default=50.0,
                       help="Maximum shard size in GB (default: 50)")
    parser.add_argument("--es-expansion-factor", type=float, default=3.0,
                       help="ES size expansion factor vs parquet (default: 3.0)")
    
    parser.add_argument("--batch-size", type=int, default=12500, help="Batch size for bulk indexing")
    parser.add_argument("--chunk-size", type=int, default=5000, help="Chunk size for reading parquet files")
    parser.add_argument("--es-host", default="localhost", help="Elasticsearch host")
    parser.add_argument("--es-port", type=int, default=9200, help="Elasticsearch port")
    parser.add_argument("--index-name", default="fineweb", help="Elasticsearch index name")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--index-config", help="Path to JSON file with index configuration")
    parser.add_argument("--max-chunk-bytes", type=int, default=75, help="Max chunk size in MB for parallel_bulk")
    parser.add_argument("--thread-count", type=int, default=8, help="Number of threads for parallel_bulk")
    parser.add_argument("--queue-size", type=int, default=8, help="Queue size for parallel_bulk")
    
    # Multi-process argument
    parser.add_argument("--num-workers", type=int, default=1,
                       help="Number of parallel parser workers (default: 1). "
                            "Recommended: 6-8 for 10 CPUs")
    
    # Metadata fields argument
    parser.add_argument("--metadata-fields", type=str, default="text",
                       choices=["text", "text-url", "text-url-lang"],
                       help="Which metadata fields to include in index: "
                            "'text' (text only, default), "
                            "'text-url' (add URL), or "
                            "'text-url-lang' (add URL and language). "
                            "URLs and language are stored but NOT indexed (not searchable).")
    
    args = parser.parse_args()
    
    logger = setup_logging(args.log_level)
    
    tracemalloc.start()
    log_memory_usage(logger, "AT START")

    if args.input_mode == "web-artifacts" and args.record_schema != "memory_v1":
        logger.error("web-artifacts mode requires --record-schema memory_v1")
        sys.exit(1)

    files_to_process: List[Path] = []
    data_dir_pattern = args.data_dir or ""

    if args.input_mode == "web-artifacts":
        for candidate in [args.web_content_parquet, args.web_chunks_parquet, args.web_status]:
            if not candidate:
                continue
            artifact_path = Path(candidate)
            if not artifact_path.exists():
                logger.error(f"Web artifact path does not exist: {artifact_path}")
                sys.exit(1)
            files_to_process.append(artifact_path)

        if not files_to_process:
            logger.error(
                "In web-artifacts mode, provide at least one of: "
                "--web-content-parquet, --web-chunks-parquet, --web-status"
            )
            sys.exit(1)

        total_size_bytes = sum(path.stat().st_size for path in files_to_process)
        total_data_size_gb = total_size_bytes / (1024**3)
    else:
        if not data_dir_pattern:
            logger.error("--data-dir is required in dataset mode")
            sys.exit(1)

        # Check if it's a pattern, single/multiple files, or single directory
        if "*" in data_dir_pattern:
            logger.info(f"Using directory pattern: {data_dir_pattern}")
            files_to_process, total_data_size_gb = get_file_range(
                data_dir_pattern,
                args.file_range_start if args.file_range_start is not None else None,
                args.file_range_end if args.file_range_end is not None else None,
            )
        elif " " in data_dir_pattern:
            logger.info(f"Processing multiple paths: {data_dir_pattern}")
            paths = data_dir_pattern.split()

            for path_str in paths:
                path = Path(path_str)
                if not path.exists():
                    logger.error(f"Path does not exist: {path}")
                    sys.exit(1)

                if path.is_file():
                    if path.suffix == ".parquet":
                        files_to_process.append(path)
                    else:
                        logger.error(f"File is not a parquet file: {path}")
                        sys.exit(1)
                elif path.is_dir():
                    dir_parquet_files = sorted(list(path.glob("*.parquet")))
                    if not dir_parquet_files:
                        logger.warning(f"No parquet files found in directory: {path}")
                    else:
                        files_to_process.extend(dir_parquet_files)

            if not files_to_process:
                logger.error("No parquet files found in any of the specified paths")
                sys.exit(1)

            total_size_bytes = sum(f.stat().st_size for f in files_to_process)
            total_data_size_gb = total_size_bytes / (1024**3)
            logger.info(f"Found {len(files_to_process)} parquet files across all paths")
        else:
            data_path = Path(data_dir_pattern)
            if not data_path.exists():
                logger.error(f"Path does not exist: {data_path}")
                sys.exit(1)

            if data_path.is_file():
                if data_path.suffix != ".parquet":
                    logger.error(f"File is not a parquet file: {data_path}")
                    sys.exit(1)

                logger.info(f"Processing single parquet file: {data_path}")
                files_to_process = [data_path]
                total_data_size_gb = data_path.stat().st_size / (1024**3)

            elif data_path.is_dir():
                if args.file_range_start is not None and args.file_range_end is not None:
                    files_to_process, total_data_size_gb = get_file_range(
                        str(data_path), args.file_range_start, args.file_range_end
                    )
                else:
                    all_parquet_files = sorted(list(data_path.glob("*.parquet")))
                    if not all_parquet_files:
                        logger.error(f"No parquet files found in {data_path}")
                        sys.exit(1)
                    files_to_process = all_parquet_files
                    total_size_bytes = sum(f.stat().st_size for f in files_to_process)
                    total_data_size_gb = total_size_bytes / (1024**3)

            
    logger.info(f"Total raw data size: {total_data_size_gb:.2f} GB")

    logger.info("=== Indexing Started with Multi-Process Support ===")
    if args.input_mode == "dataset":
        logger.info(f"Data directory: {data_dir_pattern}")
    else:
        logger.info("Input files:")
        for artifact in files_to_process:
            logger.info(f"  - {artifact}")
    logger.info(f"Files to process: {len(files_to_process)}")
    logger.info(f"Batch size: {args.batch_size}")
    logger.info(f"Chunk size: {args.chunk_size}")
    logger.info(f"Max chunk bytes: {args.max_chunk_bytes}")
    logger.info(f"Elasticsearch: {args.es_host}:{args.es_port}")
    logger.info(f"Index name: {args.index_name}")
    logger.info(f"Thread count: {args.thread_count}")
    logger.info(f"Queue size: {args.queue_size}")
    logger.info(f"Num workers: {args.num_workers}")
    logger.info(f"Metadata fields: {args.metadata_fields}")
    logger.info(f"Record schema: {args.record_schema}")
    logger.info(f"Input mode: {args.input_mode}")
    logger.info(f"Shard size range: {args.min_shard_size}-{args.max_shard_size} GB")
    logger.info(f"ES expansion factor: {args.es_expansion_factor}x")
    
    logger.info("Step 1: Counting documents in source files...")
    try:
        ground_truth_count = count_documents_in_parquet_files(files_to_process)
    except Exception as e:
        logger.error(f"Failed to count documents: {e}")
        sys.exit(1)

    total_start_time = time.time()
    
    try:
        # Connect to Elasticsearch
        es = get_elasticsearch_client(args.es_host, args.es_port)
        log_memory_usage(logger, "AFTER ES CONNECTION")

        # Create index with size-based dynamic sharding
        logger.info("Creating Elasticsearch index with size-based dynamic sharding...")
        if not create_index_with_size_based_shards(
            es, args.index_name, total_data_size_gb, args.index_config,
            args.min_shard_size, args.max_shard_size, args.es_expansion_factor,
            metadata_fields=args.metadata_fields,
            record_schema=args.record_schema,
        ):
            sys.exit(1)
        
        log_memory_usage(logger, "AFTER INDEX CREATION")
        
        # Process files with multi-process support
        logger.info("Starting document indexing...")
        indexing_start_time = time.time()
        
        
        stats = process_file_list(
            files_to_process, 
            args.chunk_size, 
            args.index_name, 
            es,
            args.batch_size, 
            args.max_chunk_bytes, 
            args.thread_count, 
            args.queue_size,
            args.num_workers,
            metadata_fields=args.metadata_fields,
            record_schema=args.record_schema,
            input_mode=args.input_mode,
        )
        
        indexing_end_time = time.time()
        log_memory_usage(logger, "AFTER INDEXING")
        
        # Final statistics
        total_time = time.time() - total_start_time
        indexing_time = indexing_end_time - indexing_start_time
        
        logger.info("=== Indexing Completed ===")
        logger.info(f"Total documents indexed: {stats['indexed']:,}")
        logger.info(f"Failed documents: {stats['failed']:,}")
        logger.info(f"Files processed: {stats['files_processed']:,}")
        logger.info(f"Indexing time: {indexing_time:.2f} seconds")
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        
        if indexing_time > 0:
            logger.info(f"Average indexing rate: {stats['indexed'] / indexing_time:.1f} docs/sec")
        
        # Log final memory usage and peak usage
        log_memory_usage(logger, "FINAL")
        try:
            current, peak = tracemalloc.get_traced_memory()
            logger.info(f"Peak memory usage during execution: {peak / (1024**3):.2f} GB")
            tracemalloc.stop()
        except Exception as e:
            logger.warning(f"Could not get tracemalloc info: {e}")

        # Force an immediate refresh to ensure all documents are searchable
        try:
            logger.info("Indexing complete - performing final refresh")
            es.indices.refresh(index=args.index_name)
            logger.info("Final refresh complete - all documents are now searchable")
        except Exception as e:
            logger.error(f"Failed to perform final refresh: {e}")

        # Get final index stats
        try:
            index_stats = es.indices.stats(index=args.index_name)
            doc_count = index_stats['indices'][args.index_name]['total']['docs']['count']
            index_size = index_stats['indices'][args.index_name]['total']['store']['size_in_bytes']
            
            logger.info(f"Final index document count: {doc_count:,}")
            logger.info(f"Index size: {index_size / (1024*1024):.2f} MB")
            
        except Exception as e:
            logger.warning(f"Could not get final index stats: {e}")
        
        logger.info("\nStep 2: Validating indexing results...")
        validation_passed = validate_indexing_results(
            ground_truth_count=ground_truth_count,
            stats=stats,
            es=es,
            index_name=args.index_name
        )
        
        if not validation_passed:
            logger.error("Indexing completed with validation errors!")
            sys.exit(1)
            
        logger.info("=== Indexing Job Completed Successfully ===")
        
    except KeyboardInterrupt:
        logger.info("Indexing interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Indexing failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
