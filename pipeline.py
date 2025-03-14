#!/usr/bin/env python3
"""
UN Resolution Classification Pipeline

This module provides tools for classifying UN resolutions using the OpenAI API.
It implements a three-stage hierarchical classification approach with parallel processing
for improved performance.
"""

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict, Any, Tuple
import argparse

import pandas as pd
from tqdm import tqdm
from pydantic import BaseModel, Field
from openai import OpenAI
from dotenv import load_dotenv

# Import classification schema
from un_classification import un_classification

# ============= CONFIGURATION ============= #

# Load environment variables
load_dotenv()
API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise ValueError("API_KEY not found in environment variables. Please set it in your .env file.")

# Default settings
DEFAULT_MODEL = "gpt-4o-mini"
DEFAULT_TEMPERATURE = 0.2
DEFAULT_MAX_TOKENS = 1000
DEFAULT_MAX_WORKERS = 5
DEFAULT_BATCH_SIZE = 20
DEFAULT_INPUT_FILE = "data/UN_VOTING_DATA_RAW.csv"
DEFAULT_OUTPUT_FILE = "data/UN_VOTING_DATA_RAW_with_tags.csv"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Console handler
        logging.FileHandler("un_classification_pipeline.log")  # File handler
    ]
)
logger = logging.getLogger(__name__)

# ============= DATA MODELS ============= #

class MainTagClassification(BaseModel):
    """Pydantic model for stage 1 classification (main tags)"""
    main_tags: List[str] = Field(..., description="List of relevant main category tags")

class SubTag1Classification(BaseModel):
    """Pydantic model for stage 2 classification (subtags)"""
    subtag1s: List[str] = Field(..., description="List of relevant subcategories for the main tag")

class SubTag2Classification(BaseModel):
    """Pydantic model for stage 3 classification (specific items)"""
    subtag2s: List[str] = Field(..., description="List of relevant specific items for the subcategory")

# ============= API FUNCTIONS ============= #

def create_openai_client() -> OpenAI:
    """Create and return an OpenAI client instance"""
    return OpenAI(api_key=API_KEY)

def call_api_staged(title: str, stage: int, previous_tags: Optional[Dict] = None, 
                    model: str = DEFAULT_MODEL) -> Any:
    """
    Analyzes a UN resolution text in stages.
    
    Args:
        title: Title of the resolution to analyze
        stage: 1 for main tag, 2 for subtag1, 3 for subtag2
        previous_tags: Results from previous stages
        model: The OpenAI model to use
        
    Returns:
        Structured classification results
    """
    # Initialize OpenAI client
    client = create_openai_client()
    
    if stage == 1:
        # Stage 1: identify main tag categories
        main_tag_options = list(un_classification.keys())
        system_prompt = f"""You are a UN document classification assistant. Your task is to analyze UN resolutions given their Title.
Classify the resolution according to the following valid main categories (select only values from the list):
        
{main_tag_options}

Rules:
1. Identify ALL relevant main categories from the list.
2. Return only valid category names as a list.
3. If none of the categories apply, return an empty list.
"""
        try:
            response = client.beta.chat.completions.parse(
                model=model,
                temperature=DEFAULT_TEMPERATURE,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Resolution text: {title}"}
                ],
                max_tokens=DEFAULT_MAX_TOKENS,
                response_format=MainTagClassification,
            )
            
            return response.choices[0].message.parsed
            
        except Exception as e:
            logger.error(f"Error during main tag API call: {e}")
            return MainTagClassification(main_tags=[])
        
    elif stage == 2:
        # Stage 2: identify subtag1 based on main tags
        if not previous_tags or "main_tag" not in previous_tags:
            logger.error("Missing main_tag in previous_tags for stage 2 classification")
            return SubTag1Classification(subtag1s=[])
            
        main_tag = previous_tags["main_tag"]
        subcategories = list(un_classification[main_tag].keys())
        
        system_prompt = f"""You are a UN document classification assistant. Your task is to analyze UN resolutions given their Title.
For a resolution categorized in the main category '{main_tag}', select the relevant subcategories from the following valid list:
        
{subcategories}

Rules:
1. Select only unique, valid subcategories from the list above.
2. If none of the listed subcategories apply, return an empty string.
3. Return only the valid subcategory names as a list.
"""
        try:
            response = client.beta.chat.completions.parse(
                model=model,
                temperature=DEFAULT_TEMPERATURE,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Resolution text: {title}"}
                ],
                max_tokens=DEFAULT_MAX_TOKENS,
                response_format=SubTag1Classification,
            )
            
            return response.choices[0].message.parsed
            
        except Exception as e:
            logger.error(f"Error during subtag1 API call for {main_tag}: {e}")
            return SubTag1Classification(subtag1s=[])
        
    elif stage == 3:
        # Stage 3: identify subtag2 based on main tag and subtag1
        if not previous_tags or "main_tag" not in previous_tags or "subtag1" not in previous_tags:
            logger.error("Missing required tags in previous_tags for stage 3 classification")
            return SubTag2Classification(subtag2s=[])
            
        main_tag = previous_tags["main_tag"]
        subtag1 = previous_tags["subtag1"]
        
        if main_tag not in un_classification or subtag1 not in un_classification[main_tag]:
            logger.error(f"Invalid tag combination: {main_tag} > {subtag1}")
            return SubTag2Classification(subtag2s=[])
            
        specific_items = un_classification[main_tag][subtag1]
        
        system_prompt = f"""You are a UN document classification assistant. Your task is to analyze UN resolutions given their Title.
For a resolution categorized as '{main_tag}' > '{subtag1}', choose the most relevant specific items from the following valid options:
        
{specific_items}

Rules:
1. Select only valid items from the above list.
2. If none of the specific items are applicable, return an empty list.
3. Return only valid items as a list.
"""
        try:
            response = client.beta.chat.completions.parse(
                model=model,
                temperature=DEFAULT_TEMPERATURE,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Resolution text: {title}"}
                ],
                max_tokens=DEFAULT_MAX_TOKENS,
                response_format=SubTag2Classification,
            )
            
            return response.choices[0].message.parsed
            
        except Exception as e:
            logger.error(f"Error during subtag2 API call for {main_tag} > {subtag1}: {e}")
            return SubTag2Classification(subtag2s=[])
    
    else:
        logger.error(f"Invalid stage: {stage}")
        return None

# ============= SEQUENTIAL PROCESSING FOR EACH ROW ============= #

def get_tags_sequential(title: str, model: str = DEFAULT_MODEL) -> List[List]:
    """
    Gets classification tags for a UN resolution using sequential processing within a row.
    This ensures that dependencies between API calls are correctly handled.
    
    Args:
        title: The resolution title to classify
        model: OpenAI model to use
        
    Returns:
        List of lists containing [tag, subtag1, subtag2] classifications
    """
    start_time = time.time()
    final_results = []
    
    # Stage 1: Get main tags
    main_tags_result = call_api_staged(title, stage=1, model=model)
    if not main_tags_result.main_tags:
        logger.warning(f"No main tags found for: {title[:50]}...")
        return []
    
    # Process each main tag sequentially
    for main_tag in main_tags_result.main_tags:
        # Get subtag1 results
        subtag1_result = call_api_staged(
            title, 
            stage=2, 
            previous_tags={"main_tag": main_tag},
            model=model
        )
        
        if not subtag1_result.subtag1s:
            logger.debug(f"No subtag1s found for main tag: {main_tag}")
            continue
            
        # Process each subtag1 sequentially
        for subtag1 in subtag1_result.subtag1s:
            # Get subtag2 results
            subtag2_result = call_api_staged(
                title, 
                stage=3, 
                previous_tags={"main_tag": main_tag, "subtag1": subtag1},
                model=model
            )
            
            if subtag2_result.subtag2s:
                for subtag2 in subtag2_result.subtag2s:
                    final_results.append([main_tag, subtag1, subtag2])
            else:
                logger.debug(f"No subtag2s found for {main_tag} > {subtag1}")
    
    elapsed_time = time.time() - start_time
    logger.debug(f"Classification completed in {elapsed_time:.2f}s")
    
    return final_results

# ============= BATCH PROCESSING ============= #

def process_dataframe(df: pd.DataFrame, 
                      model: str = DEFAULT_MODEL,
                      max_workers: int = DEFAULT_MAX_WORKERS,
                      show_progress: bool = True) -> pd.DataFrame:
    """
    Process a dataframe of UN resolutions to add classification tags.
    Uses parallel processing ONLY at the row level, ensuring that
    the processing within each row is sequential.
    
    Args:
        df: DataFrame with a 'Title' column containing resolution titles
        model: OpenAI model to use
        max_workers: Maximum number of parallel worker threads (for processing rows)
        show_progress: Whether to display a progress bar
        
    Returns:
        DataFrame with an additional 'tags' column containing classifications
    """
    if 'Title' not in df.columns:
        raise ValueError("DataFrame must contain a 'Title' column")
    
    # Create a copy to avoid modifying the original
    result_df = df.copy()
    
    # Process each row in parallel
    if max_workers > 1:
        logger.info(f"Processing {len(df)} rows with {max_workers} parallel workers")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Create a list to store futures and corresponding indices
            futures = []
            indices = []
            
            # Submit all tasks
            for idx, row in df.iterrows():
                future = executor.submit(get_tags_sequential, row['Title'], model)
                futures.append(future)
                indices.append(idx)
            
            # Create a list to store results
            results = [None] * len(futures)
            
            # Process results as they complete
            if show_progress:
                for i, future in enumerate(tqdm(as_completed(futures), total=len(futures), desc="Classifying resolutions")):
                    results[indices[i]] = future.result()
            else:
                for i, future in enumerate(as_completed(futures)):
                    results[indices[i]] = future.result()
            
            # Add results to the dataframe
            result_df['tags'] = results
    else:
        # Process sequentially if max_workers is 1
        logger.info("Processing rows sequentially (max_workers=1)")
        
        # Define the processing function
        def process_row(row):
            return get_tags_sequential(row['Title'], model=model)
        
        # Process with or without progress bar
        if show_progress:
            tqdm.pandas(desc="Classifying resolutions")
            result_df['tags'] = result_df.progress_apply(process_row, axis=1)
        else:
            result_df['tags'] = result_df.apply(process_row, axis=1)
    
    return result_df

# ============= MAIN EXECUTION ============= #

def main():
    """Main execution function with command-line argument parsing"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='UN Resolution Classification Pipeline')
    
    parser.add_argument('--input', type=str, default=DEFAULT_INPUT_FILE,
                        help=f'Input CSV file path (default: {DEFAULT_INPUT_FILE})')
    parser.add_argument('--output', type=str, default=DEFAULT_OUTPUT_FILE,
                        help=f'Output CSV file path (default: {DEFAULT_OUTPUT_FILE})')
    parser.add_argument('--model', type=str, default=DEFAULT_MODEL,
                        help=f'OpenAI model to use (default: {DEFAULT_MODEL})')
    parser.add_argument('--workers', type=int, default=DEFAULT_MAX_WORKERS,
                        help=f'Maximum number of parallel workers (default: {DEFAULT_MAX_WORKERS})')
    parser.add_argument('--batch-size', type=int, default=DEFAULT_BATCH_SIZE,
                        help=f'Number of samples to process (default: {DEFAULT_BATCH_SIZE}, use 0 for all)')
    parser.add_argument('--random-seed', type=int, default=None,
                        help='Random seed for reproducible sampling (default: None)')
    parser.add_argument('--no-progress', action='store_true',
                        help='Disable progress bar')
                        
    args = parser.parse_args()
    
    # Load data
    logger.info(f"Loading data from {args.input}")
    try:
        df = pd.read_csv(args.input)
        logger.info(f"Loaded {len(df)} records")
    except Exception as e:
        logger.error(f"Error loading input file: {e}")
        return
    
    # Sample data if requested
    if args.batch_size > 0 and args.batch_size < len(df):
        logger.info(f"Sampling {args.batch_size} records with random seed {args.random_seed}")
        df_sample = df.sample(args.batch_size, random_state=args.random_seed)
    else:
        df_sample = df
        logger.info(f"Processing all {len(df)} records")
    
    # Process the data
    start_time = time.time()
    logger.info(f"Starting classification with model {args.model} using {args.workers} workers")
    
    try:
        result_df = process_dataframe(
            df_sample,
            model=args.model,
            max_workers=args.workers,
            show_progress=not args.no_progress
        )
        
        # Save results
        result_df.to_csv(args.output, index=False)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Classification completed in {elapsed_time:.2f}s")
        logger.info(f"Results saved to {args.output}")
        
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        
if __name__ == "__main__":
    main()