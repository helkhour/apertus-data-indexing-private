#!/usr/bin/env python3
"""
Elasticsearch Search Queries Pipeline - Multi-CSV Support
Performs various search query types across multiple CSV files
Enhanced with hit snippets for better analysis
Configurable query execution parameters
"""

import csv
import json
import time
import requests
import sys
import re 
import statistics
from datetime import datetime
from typing import Dict, List, Tuple, Any, Union
import ast
import argparse
import os

class ElasticsearchQueryBenchmark:
    def __init__(
        self,
        es_url: str,
        index_name: str,
        dataset: str,
        config: Dict[str, Any] = None,
        search_field: str = "search_text",
        fallback_search_field: str = "text",
        exclude_record_types: List[str] = None,
    ):
        self.es_url = es_url.rstrip('/')
        self.index_name = index_name
        self.dataset = dataset.lower()  
        self.results = []
        self.search_field = search_field
        self.fallback_search_field = fallback_search_field
        self.exclude_record_types = exclude_record_types or ["status"]

        # Optimizations for large index
        self.request_timeout = 60  
        self.max_retries = 3
        
        # Default configuration
        default_config = {
            'execute_match_query': True,
            'execute_match_phrase_query': True,
            'execute_wildcard_query': False,
            'execute_fuzzy_query': True,
            'execute_bool_must_query': False,
            'match_query_operator': ['or'],
            'match_phrase_slop': [0],
            'bool_must_operator': 'and',
            'bool_must_max_words': 3,
            'bool_must_minimum_should_match': None,
            'search_field': self.search_field,
            'fallback_search_field': self.fallback_search_field,
            'exclude_record_types': self.exclude_record_types,
        }
        
        # Update with provided config
        if config:
            default_config.update(config)
        
        # Set configuration as instance variables
        for key, value in default_config.items():
            setattr(self, key, value)

        self.search_field = str(self.search_field or "search_text")
        self.fallback_search_field = str(self.fallback_search_field or "").strip() or None

        if not isinstance(self.exclude_record_types, list):
            if self.exclude_record_types is None:
                self.exclude_record_types = []
            else:
                self.exclude_record_types = [str(self.exclude_record_types)]
        self.exclude_record_types = [str(value).strip() for value in self.exclude_record_types if str(value).strip()]
        
        # Ensure match_phrase_slop is a list
        if not isinstance(self.match_phrase_slop, list):
            self.match_phrase_slop = [self.match_phrase_slop]
            
        # Ensure match_query_operator is a list
        if not isinstance(self.match_query_operator, list):
            self.match_query_operator = [self.match_query_operator]
        
        # Update query types to include variations
        self.query_types = [
            'match_query',
            'match_phrase_query', 
            'wildcard_query',
            'fuzzy_query',
            'bool_must_query'
        ]
        
        # Add match query operator variations if multiple operators
        if len(self.match_query_operator) > 1:
            self.query_types.remove('match_query')
            for operator in self.match_query_operator:
                self.query_types.append(f'match_query_{operator}')
        elif len(self.match_query_operator) == 1 and self.match_query_operator[0] != 'or':
            self.query_types.remove('match_query')
            self.query_types.append(f'match_query_{self.match_query_operator[0]}')
        
        # Add slop variations to query types if multiple slop values
        if len(self.match_phrase_slop) > 1:
            self.query_types.remove('match_phrase_query')
            for slop in self.match_phrase_slop:
                self.query_types.append(f'match_phrase_query_slop_{slop}')
        elif len(self.match_phrase_slop) == 1 and self.match_phrase_slop[0] != 0:
            self.query_types.remove('match_phrase_query')
            self.query_types.append(f'match_phrase_query_slop_{self.match_phrase_slop[0]}')

    def _is_single_word(self, text: str) -> bool:
        """Check if the text contains only a single word"""
        words = re.findall(r'\b\w+\b', text.strip())
        return len(words) == 1

    def _query_fields(self) -> List[str]:
        fields = [self.search_field]
        if self.fallback_search_field and self.fallback_search_field != self.search_field:
            fields.append(self.fallback_search_field)
        return fields

    def _highlight_fields(self, fragment_size: int = 150, number_of_fragments: int = 3) -> Dict[str, Any]:
        return {
            field: {
                "fragment_size": fragment_size,
                "number_of_fragments": number_of_fragments,
                "pre_tags": ["<MATCH>"],
                "post_tags": ["</MATCH>"],
                "require_field_match": True,
            }
            for field in self._query_fields()
        }

    def _build_query_with_filters(self, query_clause: Dict[str, Any]) -> Dict[str, Any]:
        bool_query: Dict[str, Any] = {"must": [query_clause]}
        if self.exclude_record_types:
            bool_query["must_not"] = [{"terms": {"record_type": self.exclude_record_types}}]
        return {"bool": bool_query}

    def _multi_field_clause(self, per_field_builder):
        fields = self._query_fields()
        clauses = [per_field_builder(field) for field in fields]
        if len(clauses) == 1:
            return clauses[0]
        return {
            "bool": {
                "should": clauses,
                "minimum_should_match": 1,
            }
        }
         
    def _check_elasticsearch_health(self) -> bool:
        """Check if Elasticsearch is responsive and healthy"""
        try:
            response = requests.get(
                f"{self.es_url}/_cluster/health",
                timeout=5
            )
            if response.status_code == 200:
                health = response.json()
                if health.get('status') in ['green', 'yellow']:
                    return True
                else:
                    print(f"    WARNING: Cluster status is {health.get('status')}")
                    return False
            else:
                print(f"    WARNING: Health check returned status {response.status_code}")
                return False
        except Exception as e:
            print(f"    ERROR: Health check failed: {e}")
            return False

    def _make_request(self, method: str, endpoint: str, data: dict = None) -> Tuple[dict, float]:
        """Make HTTP request to Elasticsearch and measure response time"""
        url = f"{self.es_url}/{endpoint}"
        headers = {
            'Content-Type': 'application/json',
            'Connection': 'keep-alive'
        }
        
        for attempt in range(self.max_retries):
            start_time = time.time()
            try:
                if method.upper() == 'GET':
                    response = requests.get(url, headers=headers, timeout=self.request_timeout)
                elif method.upper() == 'POST':
                    response = requests.post(url, headers=headers, json=data, timeout=self.request_timeout)
                
                end_time = time.time()
                query_time = (end_time - start_time) * 1000
                
                response.raise_for_status()
                return response.json(), query_time
                
            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    print(f"    Timeout on attempt {attempt + 1}, retrying...")
                    time.sleep(2 ** attempt)
                    continue
                else:
                    end_time = time.time()
                    query_time = (end_time - start_time) * 1000
                    return {"error": "Timeout after retries"}, query_time
                    
            except requests.exceptions.RequestException as e:
                end_time = time.time()
                query_time = (end_time - start_time) * 1000
                print(f"Request failed: {e}")
                return {"error": str(e)}, query_time

    def match_query(self, text: str, operator: str = 'or') -> Tuple[dict, float]:
        """Standard match query using the main text field with configurable operator"""
        match_config = {
            "query": text
        }
        
        if operator.lower() != 'or':
            match_config["operator"] = operator.lower()
        else:
            # When using OR, add minimum_should_match for better precision
            word_count = len(text.split())
            if word_count <= 2:
                # Short queries: require all words
                match_config["minimum_should_match"] = "100%"
            elif word_count <= 4:
                # Medium queries: require most words  
                match_config["minimum_should_match"] = "85%"
            else:
                # Long queries: require majority
                match_config["minimum_should_match"] = "70%"

        query_clause = self._multi_field_clause(
            lambda field: {"match": {field: match_config}}
        )

        query = {
            "query": self._build_query_with_filters(query_clause),
            "track_total_hits": True,
            "size": 10,
            "_source": True,
            "highlight": {
                "fields": self._highlight_fields(fragment_size=150, number_of_fragments=3)
            },
            "timeout": "30s"
        }
        return self._make_request('POST', f"{self.index_name}/_search", query)
    
    def match_phrase_query(self, text: str, slop: int = 0) -> Tuple[dict, float]:
        """Match phrase query for exact phrase matching with configurable slop"""
        match_phrase_config = {
            "query": text
        }
        
        if slop > 0:
            match_phrase_config["slop"] = slop

        query_clause = self._multi_field_clause(
            lambda field: {"match_phrase": {field: match_phrase_config}}
        )
        
        query = {
            "query": self._build_query_with_filters(query_clause),
            "track_total_hits": True,
            "size": 10,
            "_source": True,
            "highlight": {
                "fields": self._highlight_fields(fragment_size=150, number_of_fragments=3)
            },
            "timeout": "60s"
        }
        return self._make_request('POST', f"{self.index_name}/_search", query)
        
      
    def wildcard_query(self, text: str) -> Tuple[dict, float]:
        """Wildcard query - ONLY executes on single words"""
        if not self._is_single_word(text):
            print(f"    SKIPPING wildcard_query for multi-word text: '{text[:50]}...'")
            return {
                "hits": {"total": {"value": 0}, "max_score": None, "hits": []},
                "took": 0,
                "timed_out": False
            }, 0.0
        else:
            print(f"    Executing wildcard_query on single word: '{text}'")
            wildcard_text = f"*{text.lower()}*"
            query_clause = self._multi_field_clause(
                lambda field: {"wildcard": {f"{field}.exact": wildcard_text}}
            )
            query = {
                "query": self._build_query_with_filters(query_clause),
                "size": 100,
                "highlight": {
                    "fields": self._highlight_fields(fragment_size=200, number_of_fragments=5)
                }
            }
            return self._make_request('POST', f"{self.index_name}/_search", query)

    def fuzzy_query(self, text: str) -> Tuple[dict, float]:
        """Fuzzy query - ONLY executes on single words, uses multi_match fallback for multi-word"""
        if not self._is_single_word(text):
            words = text.split()
            word_count = len(words)
            query_text = " ".join(words)
            print(f"    SKIPPING fuzzy_query for multi-word text: '{text[:50]}...'")
            print(f"    Using multi_match with fuzziness fallback instead")
            # Determine minimum_should_match based on query length
            if word_count <= 2:
                min_should_match = "100%"
            elif word_count <= 4:
                min_should_match = "85%"
            else:
                min_should_match = "70%"
            query = {
                "query": self._build_query_with_filters(
                    {
                        "multi_match": {
                            "query": query_text,
                            "fields": self._query_fields(),
                            "fuzziness": "AUTO",
                            "operator": "or",
                            "max_expansions": 20,
                            "minimum_should_match": min_should_match,
                        }
                    }
                ),
                "track_total_hits": True,
                "size": 10,
                "_source": True,
                "highlight": {
                    "fields": self._highlight_fields(fragment_size=150, number_of_fragments=3)
                },
                "timeout": "30s"
            }
        else:
            print(f"    Executing fuzzy_query on single word: '{text}'")
            query_clause = self._multi_field_clause(
                lambda field: {
                    "fuzzy": {
                        field: {
                            "value": text,
                            "fuzziness": "AUTO"
                        }
                    }
                }
            )
            query = {
                "query": self._build_query_with_filters(query_clause),
                "track_total_hits": True,
                "size": 10,
                "_source": True,
                "highlight": {
                    "fields": self._highlight_fields(fragment_size=150, number_of_fragments=3)
                }
            }
        return self._make_request('POST', f"{self.index_name}/_search", query)
    
    def bool_must_query(self, text: str) -> Tuple[dict, float]:
        """Boolean query with configurable parameters"""
        if self.bool_must_operator.lower() == 'and':
            words = text.split()[:self.bool_must_max_words]
            if len(words) < 2:
                words = [text, text]
            
            query_clause = self._multi_field_clause(
                lambda field: {
                    "bool": {
                        "must": [{"match": {field: word}} for word in words]
                    }
                }
            )
            query = {
                "query": self._build_query_with_filters(query_clause),
                "size": 50,
                "_source": True,
                "highlight": {
                    "fields": self._highlight_fields(fragment_size=150, number_of_fragments=3)
                }
            }
        else:
            words = text.split()
            if len(words) < 2:
                words = [text, text]
            
            def build_field_bool(field: str) -> Dict[str, Any]:
                bool_query = {
                    "should": [{"match": {field: word}} for word in words]
                }

                if self.bool_must_minimum_should_match is not None:
                    bool_query["minimum_should_match"] = self.bool_must_minimum_should_match
                return {"bool": bool_query}

            query_clause = self._multi_field_clause(build_field_bool)
            
            query = {
                "query": self._build_query_with_filters(query_clause),
                "size": 50,
                "_source": True,
                "highlight": {
                    "fields": self._highlight_fields(fragment_size=150, number_of_fragments=3)
                }
            }
            
        return self._make_request('POST', f"{self.index_name}/_search", query)

    def _extract_display_snippet(self, hit: Dict[str, Any], limit: int = 300) -> Tuple[str, str]:
        highlight = hit.get('highlight', {})
        for field in self._query_fields():
            if field in highlight:
                highlighted_fragments = highlight[field]
                return ' | '.join(highlighted_fragments), "HIGHLIGHTED"

        source = hit.get('_source', {})
        for field in self._query_fields() + ['text']:
            source_text = source.get(field, '')
            if source_text:
                snippet = source_text[:limit] + ('...' if len(source_text) > limit else '')
                return snippet, "SOURCE_TEXT"

        return "", "SOURCE_TEXT"

    @staticmethod
    def _extract_agent_fields(hit: Dict[str, Any]) -> Dict[str, Any]:
        agent = hit.get('_source', {}).get('agent', {}) or {}
        return {
            "agent_metadata": agent.get("metadata", {}),
            "agent_trust": agent.get("trust", {}),
            "agent_affordances": agent.get("affordances", {}),
        }
    
    def extract_hit_snippets_fineweb(self, hits_data: list, max_hits: int = 5) -> str:
        """Extract top N hit snippets for Fineweb dataset"""
        if not hits_data:
            return ""
        
        snippets = []
        for i, hit in enumerate(hits_data[:max_hits]):
            score = hit.get('_score', 0)
            url = hit.get('_source', {}).get('url', 'No URL available')
            document_id = hit.get('_source', {}).get('document_id', 'No document_id available')
            text_snippet, snippet_source = self._extract_display_snippet(hit, limit=300)
            
            text_snippet = ' '.join(text_snippet.split())
            snippet_info = f"Hit {i+1} (Score: {score:.3f}, URL: {url}, Document_ID: {document_id}, Type: {snippet_source}): {text_snippet}"
            snippets.append(snippet_info)
        
        return '\n'.join(snippets)

    def extract_hit_snippets_sft(self, hits_data: list, max_hits: int = 5) -> str:
        """Extract top N hit snippets for SFT dataset"""
        if not hits_data:
            return ""
        
        snippets = []
        for i, hit in enumerate(hits_data[:max_hits]):
            score = hit.get('_score', 0)
            conversation_id = hit.get('_source', {}).get('conversation_id', 'No conversation_id available')
            original_metadata = hit.get('_source', {}).get('original_metadata', 'No original_metadata available')
            text_snippet, snippet_source = self._extract_display_snippet(hit, limit=300)
            
            text_snippet = ' '.join(text_snippet.split())
            snippet_info = f"Hit {i+1} (Score: {score:.3f}, Conversation_ID: {conversation_id}, Original_Metadata: {original_metadata}, Type: {snippet_source}): {text_snippet}"
            snippets.append(snippet_info)
        
        return '\n'.join(snippets)

    def extract_hit_snippets_pure_text(self, hits_data: list, max_hits: int = 5) -> str:
        """Extract top N hit snippets for pure_text dataset"""
        if not hits_data:
            return ""
        
        snippets = []
        for i, hit in enumerate(hits_data[:max_hits]):
            score = hit.get('_score', 0)
            text_snippet, snippet_source = self._extract_display_snippet(hit, limit=300)
            
            text_snippet = ' '.join(text_snippet.split())
            snippet_info = f"Hit {i+1} (Score: {score:.3f}, Type: {snippet_source}): {text_snippet}"
            snippets.append(snippet_info)
        
        return '\n'.join(snippets)
    
    def extract_full_text_hits(self, hits_data: list, max_hits: int = 10) -> List[Dict[str, Any]]:
        """Extract full text of top N hits with their scores and metadata"""
        if not hits_data:
            return []
        
        full_text_hits = []
        for i, hit in enumerate(hits_data[:max_hits]):
            source = hit.get('_source', {})
            full_text = ""
            for field in self._query_fields() + ['text']:
                value = source.get(field, "")
                if value:
                    full_text = value
                    break

            hit_info = {
                'rank': i + 1,
                'score': hit.get('_score', 0),
                'full_text': full_text,
            }
            
            if self.dataset.lower() == 'sft':
                hit_info['conversation_id'] = hit.get('_source', {}).get('conversation_id', '')
                hit_info['original_metadata'] = hit.get('_source', {}).get('original_metadata', '')
            elif self.dataset.lower() == 'fineweb':
                hit_info['url'] = source.get('url', '')
                hit_info['document_id'] = source.get('document_id', '')
            elif self.dataset.lower() == 'pure_text':
                pass

            hit_info.update(self._extract_agent_fields(hit))
            
            full_text_hits.append(hit_info)
        
        return full_text_hits

    def extract_response_stats(self, response: dict) -> dict:
        """Extract relevant statistics from ES response"""
        if "error" in response:
            return {
                "total_hits": 0,
                "max_score": 0,
                "took_ms": 0,
                "timed_out": False,
                "error": response["error"],
                "hit_snippets": "",
                "full_text_hits": []
            }
        
        hits = response.get("hits", {})
        hits_data = hits.get("hits", [])

        if hasattr(self, 'dataset') and self.dataset.lower() == 'sft':
            hit_snippets = self.extract_hit_snippets_sft(hits_data)
        elif hasattr(self, 'dataset') and self.dataset.lower() == 'fineweb':
            hit_snippets = self.extract_hit_snippets_fineweb(hits_data)
        elif hasattr(self, 'dataset') and self.dataset.lower() == 'pure_text':
            hit_snippets = self.extract_hit_snippets_pure_text(hits_data)

        full_text_hits = self.extract_full_text_hits(hits_data, max_hits=10)
        
        return {
            "total_hits": hits.get("total", {}).get("value", 0) if isinstance(hits.get("total"), dict) else hits.get("total", 0),
            "max_score": hits.get("max_score", 0) or 0,
            "took_ms": response.get("took", 0),
            "timed_out": response.get("timed_out", False),
            "error": None,
            "hit_snippets": hit_snippets,
            "full_text_hits": full_text_hits
        }
    
    def run_all_queries(self, segment_text: str) -> List[dict]:
        """Run all enabled query types for a given segment text"""
        query_results = []
        
        query_methods = {}
        
        if self.execute_match_query:
            for operator in self.match_query_operator:
                if operator == 'or' and len(self.match_query_operator) == 1:
                    query_name = 'match_query'
                else:
                    query_name = f'match_query_{operator}'
                query_methods[query_name] = lambda text, op=operator: self.match_query(text, op)
            
        if self.execute_match_phrase_query:
            for slop in self.match_phrase_slop:
                if slop == 0:
                    query_name = 'match_phrase_query'
                else:
                    query_name = f'match_phrase_query_slop_{slop}'
                query_methods[query_name] = lambda text, s=slop: self.match_phrase_query(text, s)
                
            
        if self.execute_wildcard_query:
            query_methods['wildcard_query'] = lambda text: self.wildcard_query(text)
            
        if self.execute_fuzzy_query:
            query_methods['fuzzy_query'] = lambda text: self.fuzzy_query(text)
            
        if self.execute_bool_must_query:
            query_methods['bool_must_query'] = lambda text: self.bool_must_query(text)
        
        for query_type, method in query_methods.items():
            print(f"  Running {query_type}...")
            
            try:
                response, query_time = method(segment_text)
                stats = self.extract_response_stats(response)
                
                result = {
                    'timestamp': datetime.now().isoformat(),
                    'segment_text': segment_text,
                    'query_type': query_type,
                    'query_time_ms': round(query_time, 2),
                    'es_took_ms': stats['took_ms'],
                    'total_hits': stats['total_hits'],
                    'max_score': stats['max_score'],
                    'timed_out': stats['timed_out'],
                    'error': stats['error'],
                    'top_5_hits': stats['hit_snippets'],
                    'full_text_hits': stats['full_text_hits']
                }
                
                query_results.append(result)
                self.results.append(result)
                
            except Exception as e:
                print(f"    Error in {query_type}: {e}")
                error_result = {
                    'timestamp': datetime.now().isoformat(),
                    'segment_text': segment_text,
                    'query_type': query_type,
                    'query_time_ms': 0,
                    'es_took_ms': 0,
                    'total_hits': 0,
                    'max_score': 0,
                    'timed_out': False,
                    'error': str(e),
                    'top_5_hits': '',
                    'full_text_hits': []
                }
                query_results.append(error_result)
                self.results.append(error_result)
        
        return query_results

    def process_csv(self, csv_file: str):
        """Process the CSV file and run queries for each segment"""
        print(f"Processing CSV file: {csv_file}")
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                
                total_rows = 0
                processed_rows = 0
                
                for row in reader:
                    if row and row[0].strip():
                        total_rows += 1
                
                file.seek(0)
                reader = csv.reader(file)
                
                print(f"Found {total_rows} segments to process")
                print("=" * 50)
                
                for row in reader:
                    if not row or not row[0].strip():
                        continue
                        
                    processed_rows += 1
                    segment_text = row[0].strip()
                    
                    print(f"Processing segment {processed_rows}/{total_rows}: '{segment_text[:50]}{'...' if len(segment_text) > 50 else ''}'")
                    
                    self.run_all_queries(segment_text)
                    
                    if processed_rows % 10 == 0:
                        print(f"Progress: {processed_rows}/{total_rows} segments processed")
                        
        except FileNotFoundError:
            print(f"Error: CSV file '{csv_file}' not found")
            sys.exit(1)
        except Exception as e:
            print(f"Error processing CSV file: {e}")
            sys.exit(1)
    def process_string(self, text: str):
        """Process a single raw input string instead of a CSV file."""
        text = text.strip()
        if not text:
            print("Empty text input")
            return
        
        print(f"Processing single text input: '{text[:50]}{'...' if len(text) > 50 else ''}'")
        self.run_all_queries(text)
    
    def save_detailed_results(self, filename: str = 'search_results_detailed.json'):
        """Save detailed results to JSON file"""
        print(f"Saving detailed results to {filename}...")
        
        with open(filename, 'w', encoding='utf-8') as file:
            json.dump(self.results, file, indent=2, ensure_ascii=False)
        
        print(f"Detailed results saved to {filename}")

    def generate_summary_stats(self, filename: str = 'search_results_summary.json'):
        """Generate and save summary statistics"""
        print(f"Generating summary statistics...")
        
        if not self.results:
            print("No results to analyze")
            return
        
        stats_by_type = {}
        total_queries = len(self.results)
        successful_queries = len([r for r in self.results if r['error'] is None])
        
        for query_type in self.query_types:
            type_results = [r for r in self.results if r['query_type'] == query_type and r['error'] is None]
            
            if type_results:
                query_times = [r['query_time_ms'] for r in type_results]
                es_times = [r['es_took_ms'] for r in type_results]
                hit_counts = [r['total_hits'] for r in type_results]
                
                stats_by_type[query_type] = {
                    'total_queries': len(type_results),
                    'avg_query_time_ms': round(statistics.mean(query_times), 2),
                    'median_query_time_ms': round(statistics.median(query_times), 2),
                    'min_query_time_ms': round(min(query_times), 2),
                    'max_query_time_ms': round(max(query_times), 2),
                    'avg_es_time_ms': round(statistics.mean(es_times), 2),
                    'avg_hits': round(statistics.mean(hit_counts), 2),
                    'total_hits': sum(hit_counts),
                    'errors': len([r for r in self.results if r['query_type'] == query_type and r['error'] is not None])
                }
            else:
                stats_by_type[query_type] = {
                    'total_queries': 0,
                    'avg_query_time_ms': 0,
                    'median_query_time_ms': 0,
                    'min_query_time_ms': 0,
                    'max_query_time_ms': 0,
                    'avg_es_time_ms': 0,
                    'avg_hits': 0,
                    'total_hits': 0,
                    'errors': len([r for r in self.results if r['query_type'] == query_type])
                }
        
        summary = {
            'overview': {
                'total_queries': total_queries,
                'successful_queries': successful_queries,
                'failed_queries': total_queries - successful_queries
            },
            'query_type_stats': stats_by_type
        }
        
        with open(filename, 'w', encoding='utf-8') as file:
            json.dump(summary, file, indent=2, ensure_ascii=False)
        
        print("\n" + "=" * 60)
        print("SEARCH PIPELINE SUMMARY")
        print("=" * 60)
        print(f"Total queries executed: {total_queries}")
        print(f"Successful queries: {successful_queries}")
        print(f"Failed queries: {total_queries - successful_queries}")
        print()
        
        print("Performance by Query Type:")
        print("-" * 40)
        for query_type, stats in stats_by_type.items():
            print(f"{query_type}:")
            print(f"  Queries: {stats['total_queries']}")
            print(f"  Avg Time: {stats['avg_query_time_ms']}ms")
            print(f"  Avg Hits: {stats['avg_hits']}")
            print(f"  Errors: {stats['errors']}")
            print()
        
        print(f"Summary statistics saved to {filename}")
    
    def save_full_text_results(self, filename: str = 'search_results_fulltext.json'):
        """Save results with full text of top 10 hits for specified query types"""
        print(f"Saving full text results to {filename}...")
        
        target_queries = ['match_query', 'match_phrase_query', 'fuzzy_query']
        
        full_text_results = []
        for result in self.results:
            if any(result['query_type'].startswith(q) for q in target_queries):
                full_text_results.append({
                    'timestamp': result['timestamp'],
                    'segment_text': result['segment_text'],
                    'query_type': result['query_type'],
                    'query_time_ms': result['query_time_ms'],
                    'es_took_ms': result['es_took_ms'],
                    'total_hits': result['total_hits'],
                    'max_score': result['max_score'],
                    'error': result['error'],
                    'top_10_full_text_hits': result['full_text_hits']
                })
        
        with open(filename, 'w', encoding='utf-8') as file:
            json.dump(full_text_results, file, indent=2, ensure_ascii=False)
        
        print(f"Full text results saved to {filename}")
        print(f"Includes top 10 full-text documents for match_query, match_phrase_query, and fuzzy_query")


def parse_config_value(value_str: str) -> Union[bool, int, str, List]:
    """Parse configuration values from string"""
    if not value_str:
        return None
        
    if value_str.lower() in ('true', 'false'):
        return value_str.lower() == 'true'
    
    if value_str.startswith('[') and value_str.endswith(']'):
        try:
            return ast.literal_eval(value_str)
        except:
            return [value_str]
    
    try:
        return int(value_str)
    except ValueError:
        pass
    
    return value_str


def main():
    parser = argparse.ArgumentParser(description="Elasticsearch Search Queries Pipeline - Multi-CSV Support")
        
    # Required arguments
    parser.add_argument("--csv-files", required=False, nargs='+',
                       help="One or more CSV files containing search queries (one query per line)")
    parser.add_argument("--index-name", required=True,
                       help="Elasticsearch index name to search")
    parser.add_argument("--es-url", required=True,
                       help="Elasticsearch URL (e.g., http://localhost:9200)")
    parser.add_argument("--output-dir-base", required=True,
                       help="Base directory to save result files (subdirs will be created per CSV)")
    parser.add_argument("--input-string", type=str,
                    help="Single text string to query instead of CSV")

    
    # Optional configuration
    parser.add_argument("--config", type=str,
                       help="JSON configuration string for query execution parameters")
    
    parser.add_argument("--dataset", choices=['fineweb', 'sft', 'pure_text'], default='fineweb',
                       help="Dataset type: 'fineweb' or 'sft' or 'pure_text' (default: fineweb)")
    parser.add_argument("--search-field", default="search_text",
                       help="Primary searchable field (default: search_text)")
    parser.add_argument("--fallback-search-field", default="text",
                       help="Fallback searchable field for legacy indexes (default: text)")
    parser.add_argument("--exclude-record-types", default="status",
                       help="Comma-separated record_type values to exclude from search results (default: status)")

    args = parser.parse_args()

    if not args.csv_files and not args.input_string:
        print("ERROR: You must provide either --csv-files or --input-string")
        sys.exit(1)

    
    # Parse configuration
    config = {}
    if args.config:
        try:
            cli_config = json.loads(args.config)
            config.update(cli_config)
            print(f"Applied command line configuration")
        except json.JSONDecodeError as e:
            print(f"Error parsing configuration JSON: {e}")
            sys.exit(1)

    excluded_record_types = [
        value.strip()
        for value in (args.exclude_record_types or "").split(",")
        if value.strip()
    ]

    print("=" * 80)
    print("Elasticsearch Search Pipeline Starting...")
    print("=" * 80)
    # Only print CSV file info if CSV_FILES were provided
    if args.csv_files:
        print(f"CSV Files ({len(args.csv_files)}):")
        for csv_file in args.csv_files:
            print(f"  - {csv_file}")
    else:
        print("CSV Files: None (using input string mode)")
    print(f"Index: {args.index_name}")
    print(f"ES URL: {args.es_url}")
    print(f"Output Base Directory: {args.output_dir_base}")
    print(f"Dataset: {args.dataset}")
    print(f"Search Field: {args.search_field}")
    print(f"Fallback Search Field: {args.fallback_search_field}")
    print(f"Excluded Record Types: {args.exclude_record_types}")
    print(f"Configuration: {config}")
    print("=" * 80)
    
    # Create base output directory if it doesn't exist
    os.makedirs(args.output_dir_base, exist_ok=True)
    
    # Process each CSV file separately
    overall_start_time = time.time()

    # If input-string is provided, run single-string mode and exit
    if args.input_string:
        print("\n" + "=" * 80)
        print("PROCESSING DIRECT INPUT STRING")
        print("=" * 80)

        benchmark = ElasticsearchQueryBenchmark(
            args.es_url,
            args.index_name,
            args.dataset,
            config,
            search_field=args.search_field,
            fallback_search_field=args.fallback_search_field,
            exclude_record_types=excluded_record_types,
        )

        # Test ES connection
        try:
            response, _ = benchmark._make_request('GET', '')
            print(f"Connected to Elasticsearch: {response.get('tagline', 'Unknown version')}")
        except Exception as e:
            print(f"Failed to connect to Elasticsearch: {e}")
            sys.exit(1)

        # Run search
        benchmark.process_string(args.input_string)

        # Save results normally
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        job_id = os.environ.get('SLURM_JOB_ID', 'local')

        detailed_filename = os.path.join(args.output_dir_base, f"search_results_detailed_{job_id}_{timestamp}.json")
        summary_filename   = os.path.join(args.output_dir_base, f"search_results_summary_{job_id}_{timestamp}.json")
        fulltext_filename  = os.path.join(args.output_dir_base, f"search_results_fulltext_{job_id}_{timestamp}.json")

        benchmark.save_detailed_results(detailed_filename)
        benchmark.generate_summary_stats(summary_filename)
        benchmark.save_full_text_results(fulltext_filename)

        print("\nCompleted processing of direct input string.")
        print(f"Results saved to {args.output_dir_base}")
        overall_end_time = time.time()
        total_time = overall_end_time - overall_start_time
        print(f"Total execution time: {total_time:.2f} seconds")
        sys.exit(0)

    
    for csv_idx, csv_file in enumerate(args.csv_files, 1):
        print(f"\n{'=' * 80}")
        print(f"PROCESSING CSV FILE {csv_idx}/{len(args.csv_files)}: {csv_file}")
        print(f"{'=' * 80}\n")
        
        if not os.path.exists(csv_file):
            print(f"ERROR: CSV file not found: {csv_file}")
            print("Skipping to next CSV file...")
            continue
        
        # Create a subdirectory for this CSV file NOP

        # Create directory structure: base_dir/csv_basename/index_name/
        csv_basename = os.path.splitext(os.path.basename(csv_file))[0]
        # csv_output_dir = os.path.join(args.output_dir_base, f"{args.index_name}_{csv_basename}")
        csv_output_dir = os.path.join(args.output_dir_base, csv_basename, args.index_name)
        os.makedirs(csv_output_dir, exist_ok=True)
        
        print(f"Results for this CSV will be saved to: {csv_output_dir}")
        
        # Initialize benchmark with configuration for this CSV
        benchmark = ElasticsearchQueryBenchmark(
            args.es_url,
            args.index_name,
            args.dataset,
            config,
            search_field=args.search_field,
            fallback_search_field=args.fallback_search_field,
            exclude_record_types=excluded_record_types,
        )

        # Test ES connection (only on first CSV)
        if csv_idx == 1:
            try:
                response, _ = benchmark._make_request('GET', '')
                print(f"Connected to Elasticsearch: {response.get('tagline', 'Unknown version')}")
            except Exception as e:
                print(f"Failed to connect to Elasticsearch: {e}")
                sys.exit(1)
        
        # Process this CSV file
        csv_start_time = time.time()
        benchmark.process_csv(csv_file)
        csv_end_time = time.time()
        
        csv_time = csv_end_time - csv_start_time
        print(f"\nExecution time for {csv_basename}: {csv_time:.2f} seconds")
        
        # Save results with timestamps for uniqueness
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        job_id = os.environ.get('SLURM_JOB_ID', 'local')
        
        detailed_filename = os.path.join(csv_output_dir, f"search_results_detailed_{job_id}_{timestamp}.json")
        summary_filename = os.path.join(csv_output_dir, f"search_results_summary_{job_id}_{timestamp}.json")
        fulltext_filename = os.path.join(csv_output_dir, f"search_results_fulltext_{job_id}_{timestamp}.json")
        
        benchmark.save_detailed_results(detailed_filename)
        benchmark.generate_summary_stats(summary_filename)
        benchmark.save_full_text_results(fulltext_filename)
        
        print(f"\nResults for {csv_basename} saved to: {csv_output_dir}")
        print(f"  - Detailed: {os.path.basename(detailed_filename)}")
        print(f"  - Summary: {os.path.basename(summary_filename)}")
        print(f"  - Fulltext: {os.path.basename(fulltext_filename)}")
    
    overall_end_time = time.time()
    total_time = overall_end_time - overall_start_time
    
    print(f"\n{'=' * 80}")
    print("ALL CSV FILES PROCESSED")
    print(f"{'=' * 80}")
    print(f"Total CSV files processed: {len(args.csv_files)}")
    print(f"Total execution time: {total_time:.2f} seconds")
    print(f"Average time per CSV: {total_time/len(args.csv_files):.2f} seconds")
    print(f"\nAll results saved under: {args.output_dir_base}")
    print("Each CSV has its own subdirectory with separate result files.")
    print(f"{'=' * 80}\n")
    
    print("Pipeline completed successfully!")


if __name__ == "__main__":
    main()
