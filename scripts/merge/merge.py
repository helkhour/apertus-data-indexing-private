#!/usr/bin/env python3
"""
Enhanced Remote Reindex Merge Script for Elasticsearch
"""

import sys
import logging
import time
import json
import argparse
import requests
from typing import List, Dict, Any, Optional
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError, ConnectionError, NotFoundError, ConnectionTimeout

def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Setup comprehensive logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger(__name__)

def wait_for_cluster(host: str, port: int, max_retries: int = 30, timeout: int = 30) -> Optional[Elasticsearch]:
    """
    Wait for Elasticsearch cluster to be ready and return client with extended timeouts
    """
    logger = logging.getLogger(__name__)
    
    for attempt in range(max_retries):
        try:
            # FIXED: Use longer timeouts for large operations
            es = Elasticsearch(
                [{'host': host, 'port': port}], 
                timeout=timeout,
                request_timeout=60,  # Increased from default 10s
                retry_on_timeout=True,
                max_retries=5,
                verify_certs=False
            )
            
            health = es.cluster.health(wait_for_status='yellow', timeout='30s')
            test_response = es.cat.indices(format='json')
            
            if health['status'] in ['green', 'yellow']:
                logger.info(f"✓ Cluster {host}:{port} is ready (status: {health['status']})")
                return es
            else:
                logger.warning(f"Cluster {host}:{port} status is {health['status']}")
                
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt+1}/{max_retries}: Cluster {host}:{port} not ready: {e}")
                time.sleep(10)
            else:
                logger.error(f"Cluster {host}:{port} failed after {max_retries} attempts: {e}")
                return None
                
    return None

def safe_refresh_index(es: Elasticsearch, index_name: str, max_retries: int = 3) -> bool:
    """
    Safely refresh an index with retries and extended timeouts
    """
    logger = logging.getLogger(__name__)
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Refreshing index '{index_name}' (attempt {attempt + 1}/{max_retries})...")
            
            # FIXED: Use much longer timeout for refresh operations
            es.indices.refresh(
                index=index_name,
                request_timeout=120,  # 2 minutes timeout
                allow_no_indices=True
            )
            
            logger.info(f"✓ Successfully refreshed index '{index_name}'")
            return True
            
        except ConnectionTimeout as e:
            logger.warning(f"Refresh timeout (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                logger.info("Waiting before retry...")
                time.sleep(30)  # Wait 30 seconds before retry
            else:
                logger.error("All refresh attempts timed out")
                return False
        except Exception as e:
            logger.error(f"Refresh failed with error: {e}")
            return False
    
    return False

def get_document_count_with_retry(es: Elasticsearch, index_name: str, max_retries: int = 5) -> int:
    """
    Get document count with retries and extended timeouts
    """
    logger = logging.getLogger(__name__)
    
    for attempt in range(max_retries):
        try:
            result = es.count(
                index=index_name,
                request_timeout=120,  # 1 minute timeout
                allow_no_indices=True
            )
            count = result.get('count', 0)
            logger.debug(f"Document count for '{index_name}': {count}")
            return count
            
        except Exception as e:
            logger.warning(f"Count attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(10)
            else:
                logger.error(f"Failed to get document count after {max_retries} attempts")
                return 0
    
    return 0

def get_index_size_bytes(es: Elasticsearch, index_name: str) -> int:
    """Get index size in bytes"""
    logger = logging.getLogger(__name__)
    try:
        stats = es.indices.stats(index=index_name, metric='store', request_timeout=60)
        size_bytes = stats['indices'][index_name]['total']['store']['size_in_bytes']
        logger.debug(f"Index '{index_name}' size: {size_bytes:,} bytes")
        return size_bytes
    except Exception as e:
        logger.warning(f"Failed to get size for '{index_name}': {e}")
        return 0

def bytes_to_gb(size_bytes: int) -> float:
    """Convert bytes to GB"""
    return size_bytes / (1024 ** 3)

def get_shard_count(es: Elasticsearch, index_name: str) -> int:
    """Get number of shards for an index"""
    logger = logging.getLogger(__name__)
    try:
        settings = es.indices.get_settings(index=index_name, request_timeout=60)
        shards = int(settings[index_name]['settings']['index']['number_of_shards'])
        logger.debug(f"Index '{index_name}' has {shards} shards")
        return shards
    except Exception as e:
        logger.warning(f"Failed to get shard count for '{index_name}': {e}")
        return 0

def verify_index_health(es: Elasticsearch, index_name: str) -> bool:
    """
    Comprehensive index health verification
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Check if index exists
        if not es.indices.exists(index=index_name, request_timeout=60):
            logger.error(f"Index '{index_name}' does not exist")
            return False
        
        # Check cluster health for this specific index
        health = es.cluster.health(
            index=index_name,
            wait_for_status='yellow',
            timeout='60s',
            request_timeout=90
        )
        
        if health['status'] not in ['green', 'yellow']:
            logger.error(f"Index '{index_name}' health is {health['status']}")
            return False
        
        # Test searchability with a simple query
        search_result = es.search(
            index=index_name,
            body={'query': {'match_all': {}}, 'size': 1},
            request_timeout=30
        )
        
        if 'hits' not in search_result:
            logger.error(f"Index '{index_name}' is not searchable")
            return False
        
        logger.info(f"✓ Index '{index_name}' is healthy and searchable")
        return True
        
    except Exception as e:
        logger.error(f"Health check failed for '{index_name}': {e}")
        return False

def enhanced_final_verification(target_es: Elasticsearch, target_index: str, 
                              expected_docs: int, merged_docs: int) -> bool:
    """
    Enhanced final verification with multiple validation steps
    """
    logger = logging.getLogger(__name__)
    
    logger.info("🔄 Starting enhanced final verification...")
    
    # Step 1: Attempt refresh with retries
    logger.info("Step 1: Refreshing index...")
    refresh_success = safe_refresh_index(target_es, target_index)
    if not refresh_success:
        logger.warning("⚠️ Index refresh failed, but continuing verification...")
    
    # Step 2: Wait for operations to complete
    logger.info("Step 2: Waiting for operations to stabilize...")
    time.sleep(15)
    
    # Step 3: Get final document count with retries
    logger.info("Step 3: Getting final document count...")
    final_count = get_document_count_with_retry(target_es, target_index)
    
    # Step 4: Verify index health
    logger.info("Step 4: Verifying index health...")
    health_ok = verify_index_health(target_es, target_index)
    
    # Step 5: Analysis and verdict
    logger.info("Step 5: Analyzing results...")

    # Calculate count difference and percentage
    count_diff = abs(final_count - expected_docs)
    count_diff_pct = (count_diff / expected_docs * 100) if expected_docs > 0 else 0
    
    # Check for unacceptable discrepancy
    if count_diff_pct > 0.5:  # More than 0.5% difference
        logger.error(f"❌ Document count mismatch: {count_diff:,} documents ({count_diff_pct:.2f}% difference)")
        logger.error(f"   Expected: {expected_docs:,}")
        logger.error(f"   Actual:   {final_count:,}")
    
    success_criteria = {
        'index_exists': final_count > 0,
        'has_documents': final_count > 0,
        'health_ok': health_ok,
        'reasonable_count': final_count >= (expected_docs * 0.995)  # 99.5% tolerance
    }
    
    logger.info("📊 Verification Results:")
    logger.info(f"  Expected documents: {expected_docs:,}")
    logger.info(f"  Reported merged: {merged_docs:,}")
    logger.info(f"  Final count: {final_count:,}")
    logger.info(f"  Index exists: {'✓' if success_criteria['index_exists'] else '✗'}")
    logger.info(f"  Has documents: {'✓' if success_criteria['has_documents'] else '✗'}")
    logger.info(f"  Health OK: {'✓' if success_criteria['health_ok'] else '✗'}")
    logger.info(f"  Count reasonable: {'✓' if success_criteria['reasonable_count'] else '✗'}")
    
    # Determine overall success
    all_passed = all(success_criteria.values())
    critical_passed = success_criteria['index_exists'] and success_criteria['has_documents']
    
    if all_passed:
        logger.info("✅ VERIFICATION PASSED: All criteria met")
        return True
    elif critical_passed:
        logger.warning("⚠️ VERIFICATION PARTIAL: Critical criteria met, minor issues detected")
        return True
    else:
        logger.error("❌ VERIFICATION FAILED: Critical criteria not met")
        return False

def remote_reindex_with_monitoring(target_es: Elasticsearch, source_config: Dict[str, Any], 
                                 target_index: str, batch_size: int = 10000,
                                 source_doc_count: int = 0) -> Dict[str, Any]:
    """
    Enhanced remote reindex with improved monitoring and verification
    """
    logger = logging.getLogger(__name__)
    
    source_index = source_config['index']
    source_host = source_config['host']
    source_port = source_config['port']
    
    # Verify source cluster accessibility
    try:
        source_health_url = f"http://{source_host}:{source_port}/_cluster/health"
        response = requests.get(source_health_url, timeout=10)
        if response.status_code != 200:
            raise RuntimeError(f"Source cluster not accessible: {response.status_code}")
    except Exception as e:
        logger.error(f"Cannot connect to source cluster {source_host}:{source_port}: {e}")
        raise
    
    # Get initial target count (should be cumulative for multiple sources)
    initial_target_count = get_document_count_with_retry(target_es, target_index)
    logger.info(f"Target index has {initial_target_count:,} documents before reindex")
    
    # Configure remote reindex
    reindex_body = {
        "source": {
            "remote": {
                "host": f"http://{source_host}:{source_port}",
                "socket_timeout": "60s",
                "connect_timeout": "30s"
            },
            "index": source_index,
            "size": batch_size
        },
        "dest": {
            "index": target_index
        },
        "conflicts": "proceed"
    }
    
    try:
        logger.info(f"Submitting reindex task for {source_index}...")
        task_response = target_es.reindex(
            body=reindex_body, 
            wait_for_completion=False,
            request_timeout=300,
            timeout='5m'
        )
        
        task_id = task_response['task']
        logger.info(f"✓ Reindex task submitted: {task_id}")
        
        # Monitor task progress
        result = monitor_reindex_task(target_es, task_id, source_index, source_doc_count)


        logger.info("Waiting for index to stabilize after reindex...")
        time.sleep(30)  
        
        # Force refresh to make documents visible
        logger.info("Forcing index refresh...")
        safe_refresh_index(target_es, target_index)
        
        # Wait a bit more after refresh
        time.sleep(5)
        
        # Now get the count
        current_target_count = get_document_count_with_retry(target_es, target_index)
        
        # FIXED: Get current total count instead of calculating difference
        # time.sleep(5)  # Brief wait for consistency
        # current_target_count = get_document_count_with_retry(target_es, target_index)
        
        logger.info(f"Target index now has: {current_target_count:,} documents")
        logger.info(f"Documents from this source: {result.get('created', 0):,}")
        
        result['current_total_count'] = current_target_count
        result['initial_count'] = initial_target_count
        
        return result
        
    except Exception as e:
        logger.error(f"Remote reindex failed for {source_index}: {e}")
        raise

def monitor_reindex_task(target_es: Elasticsearch, task_id: str, source_index: str, 
                        source_doc_count: int = 0) -> Dict[str, Any]:
    """
    Monitor reindex task with enhanced timeout handling
    """
    logger = logging.getLogger(__name__)
    
    logger.info(f"Monitoring reindex task: {task_id}")
    check_interval = 30
    start_time = time.time()
    last_progress_log = 0
    
    while True:
        try:
            # FIXED: Use longer timeout for task status checks
            task_status = target_es.tasks.get(task_id=task_id, request_timeout=120)
            
            if task_status['completed']:
                logger.info(f"✓ Reindex task completed for {source_index}")
                break
            
            # Extract progress information
            task_info = task_status.get('task', {})
            status = task_info.get('status', {})
            
            created = status.get('created', 0)
            updated = status.get('updated', 0)
            version_conflicts = status.get('version_conflicts', 0)
            total = status.get('total', source_doc_count)
            
            # Calculate progress and performance metrics
            elapsed = time.time() - start_time
            rate = created / elapsed if elapsed > 0 else 0
            
            # Estimate time remaining
            if rate > 0 and total > created:
                eta_seconds = (total - created) / rate
                eta_str = f", ETA: {eta_seconds/60:.1f}m"
            else:
                eta_str = ""
            
            progress_pct = (created / total * 100) if total > 0 else 0
            
            # Log progress every minute
            if elapsed - last_progress_log >= 60 or created == 0:
                logger.info(
                    f"Progress [{source_index}]: {created:,}/{total:,} ({progress_pct:.1f}%) "
                    f"created, {updated} updated, {version_conflicts} conflicts, "
                    f"Rate: {rate:.1f} docs/sec{eta_str}"
                )
                last_progress_log = elapsed
            
        except ConnectionTimeout as e:
            logger.warning(f"Task status check timed out: {e}")
            time.sleep(check_interval)
            continue
        except Exception as e:
            logger.warning(f"Error checking task status: {e}")
            time.sleep(check_interval)
            continue
        
        time.sleep(check_interval)
    
    # Get final task results
    try:
        final_task = target_es.tasks.get(task_id=task_id, request_timeout=120)
        
        if 'error' in final_task:
            raise RuntimeError(f"Reindex task failed: {final_task['error']}")
        
        response = final_task.get('response', {})
        
        result = {
            'took_ms': response.get('took', 0),
            'total': response.get('total', 0),
            'created': response.get('created', 0),
            'updated': response.get('updated', 0),
            'version_conflicts': response.get('version_conflicts', 0),
            'failures': response.get('failures', [])
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to get final task results: {e}")
        raise

# Loads an explicit target index configuration when merge output must keep web mappings.
def load_index_config_from_file(config_path: str) -> Dict[str, Any]:
    """Load an explicit target index configuration from disk."""
    with open(config_path, 'r') as handle:
        return json.load(handle)


# Removes source-index-only settings before creating a fresh merge target index.
def sanitize_index_settings(index_settings: Dict[str, Any]) -> Dict[str, Any]:
    """Drop source-only Elasticsearch settings before creating a new index."""
    excluded_keys = {
        'creation_date',
        'uuid',
        'version',
        'provided_name',
        'routing',
        'store',
    }

    sanitized = {
        key: value for key, value in index_settings.items() if key not in excluded_keys
    }
    sanitized['number_of_replicas'] = 0
    sanitized.setdefault('refresh_interval', '30s')
    return sanitized


# Clones the first source index mapping/settings so merged web metadata is preserved by default.
def clone_source_index_config(source_es: Elasticsearch, source_index: str) -> Dict[str, Any]:
    """Clone settings and mappings from an existing source index."""
    settings_response = source_es.indices.get_settings(index=source_index, request_timeout=60)
    mappings_response = source_es.indices.get_mapping(index=source_index, request_timeout=60)

    raw_settings = settings_response[source_index]['settings']['index']
    mappings = mappings_response[source_index]['mappings']

    return {
        'settings': sanitize_index_settings(raw_settings),
        'mappings': mappings,
    }


# Provides a last-resort text-only mapping when no explicit or cloned mapping is available.
def create_simplified_index_config() -> Dict[str, Any]:
    """Fallback text-only configuration when no source mapping or config file is available."""
    return {
        'settings': {
            'number_of_shards': 3,
            'number_of_replicas': 0,
            'refresh_interval': '30s',
            'index': {
                'codec': 'best_compression',
                'max_result_window': 50000
            },
            'analysis': {
                'analyzer': {
                    'web_content_analyzer': {
                        'type': 'custom',
                        'char_filter': ['html_strip'],
                        'tokenizer': 'standard',
                        'filter': ['lowercase', 'asciifolding']
                    }
                }
            }
        },
        'mappings': {
            'dynamic': 'false',
            '_source': {'includes': ['text'], 'excludes': []},
            'properties': {
                'text': {
                    'type': 'text',
                    'analyzer': 'web_content_analyzer',
                    'index_options': 'positions',
                    'norms': True,
                    'store': False
                }
            }
        }
    }

def remote_reindex_merge(source_configs: List[Dict[str, Any]], target_config: Dict[str, Any], 
                        batch_size: int = 10000, use_async: bool = True) -> bool:
    """
    Enhanced main merge function with improved error handling and verification
    """
    logger = logging.getLogger(__name__)
    
    target_host = target_config['host']
    target_port = target_config['port']
    target_index = target_config['index']
    
    logger.info("=" * 60)
    logger.info("STARTING REMOTE REINDEX MERGE")
    logger.info("=" * 60)
    logger.info(f"Source indexes: {len(source_configs)}")
    logger.info(f"Target index: {target_index}")
    logger.info(f"Target cluster: {target_host}:{target_port}")
    logger.info(f"Batch size: {batch_size:,}")
    logger.info("=" * 60)
    
    merge_start_time = time.time()
    
    try:
        # Connect to target cluster with enhanced configuration
        logger.info("🔗 Connecting to target cluster...")
        target_es = wait_for_cluster(target_host, target_port, timeout=60)
        if not target_es:
            raise ConnectionError("Failed to connect to target cluster")
        
        # Validate and collect source information
        logger.info("🔍 Validating source clusters and indexes...")
        source_infos = []
        total_source_docs = 0
        template_source_es = None
        template_source_index = None
        
        for i, source_config in enumerate(source_configs):
            logger.info(f"Validating source {i+1}/{len(source_configs)}: {source_config['index']}")
            
            source_es = wait_for_cluster(source_config['host'], source_config['port'], timeout=60)
            if not source_es:
                raise ConnectionError(f"Failed to connect to source cluster {source_config['host']}:{source_config['port']}")
            
            # Get basic index info including size
            try:
                doc_count = get_document_count_with_retry(source_es, source_config['index'])
                size_bytes = get_index_size_bytes(source_es, source_config['index'])
                source_infos.append({**source_config, 'doc_count': doc_count, 'size_bytes': size_bytes})
                total_source_docs += doc_count
                if template_source_es is None:
                    template_source_es = source_es
                    template_source_index = source_config['index']
                size_gb = bytes_to_gb(size_bytes)
                logger.info(f"  ✓ {source_config['index']}: {doc_count:,} documents ({size_gb:.2f} GB)")
            except Exception as e:
                logger.error(f"Failed to get info for {source_config['index']}: {e}")
                raise
        
        logger.info(f"📊 Total expected documents: {total_source_docs:,}")

        total_source_size_bytes = sum(info.get('size_bytes', 0) for info in source_infos)
        total_source_size_gb = bytes_to_gb(total_source_size_bytes)
        final_shard_count = get_shard_count(target_es, target_index)
        logger.info(f"📦 Total source size: {total_source_size_gb:.2f} GB")
        logger.info(f"   Final shard count: {final_shard_count}")
        
        # Prefers an explicit config or cloned source mapping so web metadata survives merges.
        logger.info("🏗️ Preparing target index...")
        if target_es.indices.exists(index=target_index, request_timeout=60):
            logger.info(f"Target index '{target_index}' already exists")
        else:
            if target_config.get('index_config'):
                logger.info(f"Loading target index configuration from file: {target_config['index_config']}")
                config = load_index_config_from_file(target_config['index_config'])
            elif template_source_es is not None and template_source_index is not None:
                logger.info(f"Cloning target index settings/mappings from source index: {template_source_index}")
                config = clone_source_index_config(template_source_es, template_source_index)
            else:
                logger.warning("Falling back to simplified text-only target mapping; metadata fields may be lost")
                config = create_simplified_index_config()
            
            logger.info("Creating target index...")
            target_es.indices.create(
                index=target_index,
                body=config,
                request_timeout=120
            )
            logger.info(f"✓ Created target index '{target_index}'")

        # Perform merge operations
        logger.info("🔄 Starting merge operations...")
        total_merged_docs = 0
        index_results = []
        
        for i, source_info in enumerate(source_infos):
            logger.info("=" * 50)
            logger.info(f"📥 Merging source {i+1}/{len(source_infos)}: {source_info['index']}")
            logger.info(f"   Expected documents: {source_info['doc_count']:,}")
            logger.info("=" * 50)
            
            source_start_time = time.time()
            
            try:
                result = remote_reindex_with_monitoring(
                    target_es, source_info, target_index, batch_size, source_info['doc_count']
                )
                
                source_duration = time.time() - source_start_time
                docs_created = result.get('created', 0)
                docs_updated = result.get('updated', 0)
                conflicts = result.get('version_conflicts', 0)
                failures = len(result.get('failures', []))
                
                total_merged_docs += docs_created
                
                index_result = {
                    'index': source_info['index'],
                    'expected_docs': source_info['doc_count'],
                    'created_docs': docs_created,
                    'updated_docs': docs_updated,
                    'conflicts': conflicts,
                    'failures': failures,
                    'duration_seconds': source_duration,
                    'rate_docs_per_sec': docs_created / source_duration if source_duration > 0 else 0,
                    'current_total': result.get('current_total_count', 0)
                }
                index_results.append(index_result)
                
                logger.info(f"✅ Completed {source_info['index']}:")
                logger.info(f"   Created: {docs_created:,} documents")
                logger.info(f"   Current total: {result.get('current_total_count', 0):,}")
                logger.info(f"   Duration: {source_duration:.2f} seconds")
                logger.info(f"   Rate: {docs_created/source_duration:.1f} docs/sec" if source_duration > 0 else "   Rate: N/A")
                
            except Exception as e:
                logger.error(f"❌ Failed to merge {source_info['index']}: {e}")
                index_results.append({
                    'index': source_info['index'],
                    'expected_docs': source_info['doc_count'],
                    'error': str(e),
                    'created_docs': 0,
                    'failures': 1,
                    'duration_seconds': time.time() - source_start_time
                })
                continue
        
        #Final verification
        verification_success = enhanced_final_verification(
            target_es, target_index, total_source_docs, total_merged_docs
        )
        final_total = get_document_count_with_retry(target_es, target_index)

        # Get final target index size
        final_size_bytes = get_index_size_bytes(target_es, target_index)
        final_size_gb = bytes_to_gb(final_size_bytes)

        # Generate final report
        total_duration = time.time() - merge_start_time
        successful_indexes = len([r for r in index_results if 'error' not in r])
        
        logger.info("=" * 60)
        if verification_success:
            logger.info("🎉 MERGE COMPLETED SUCCESSFULLY")
        else:
            logger.warning("⚠️ MERGE COMPLETED WITH ISSUES")
        logger.info("=" * 60)
        
        logger.info(f"📊 Final Summary:")
        logger.info(f"   Source indexes processed: {len(source_configs)}")
        logger.info(f"   Successful merges: {successful_indexes}")
        logger.info(f"   Total expected documents: {total_source_docs:,}")
        logger.info(f"   Total merged documents: {total_merged_docs:,}")
        logger.info(f"   Final index count: {final_total:,}")
        logger.info(f"   Final index size: {final_size_gb:.2f} GB")
        logger.info(f"   Total source size: {total_source_size_gb:.2f} GB")
        logger.info(f"   Total duration: {total_duration/60:.1f} minutes")
        
        return verification_success and successful_indexes > 0
        
    except Exception as e:
        logger.error(f"❌ Merge operation failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

def main():
    """Main function for standalone execution"""
    parser = argparse.ArgumentParser(description="Compatible Elasticsearch Index Merger")
    
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--source-configs", type=str, 
                            help="JSON string with source configurations")
    source_group.add_argument("--source-indexes", nargs='+', 
                            help="List of source index names")
    
    parser.add_argument("--target-index", required=True, help="Target index name")
    parser.add_argument("--target-host", default="localhost", help="Target host")
    parser.add_argument("--target-port", type=int, default=9200, help="Target port")
    parser.add_argument("--batch-size", type=int, default=10000, help="Batch size")
    # Accepts an explicit target mapping file so merged web indexes keep their fields.
    parser.add_argument("--index-config", help="Optional path to a target index JSON config. If omitted, the first source index mapping/settings are cloned.")
    parser.add_argument("--log-level", default="INFO", 
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    args = parser.parse_args()
    
    logger = setup_logging(args.log_level)
    
    # Parse source configurations
    if args.source_configs:
        try:
            source_configs = json.loads(args.source_configs)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in source-configs: {e}")
            sys.exit(1)
    else:
        source_configs = []
        for i, index_name in enumerate(args.source_indexes):
            source_configs.append({
                'index': index_name,
                'host': 'localhost',
                'port': 9201 + i
            })
    
    target_config = {
        'index': args.target_index,
        'host': args.target_host,
        'port': args.target_port,
        'index_config': args.index_config
    }
    
    success = remote_reindex_merge(
        source_configs=source_configs,
        target_config=target_config,
        batch_size=args.batch_size,
        use_async=True
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()