import pandas as pd
import argparse
import sys
import os
from tqdm import tqdm


def load_llm_model(model_name="gpt2"):
    """
    Loads the LLM model using the transformers library.
    """
    print(f"Loading model: {model_name}...")
    try:
        from transformers import pipeline, AutoModelForCausalLM, AutoTokenizer
        import torch

        # Initialize the pipeline
        # You can switch to a specific task like 'text2text-generation' if using T5/BART
        # For generic LLMs, 'text-generation' is common.
        # device=-1 means CPU, set to 0 for GPU if available
        # device = 0 if torch.cuda.is_available() else -1
        # print(f"Using device: {'CUDA' if device == 0 else 'CPU'}")
        generator = pipeline(
            'text-generation', model=model_name, device_map="auto")
        return generator
    except ImportError:
        print("Error: 'transformers' or 'torch' library not found.")
        print("Please install them using: pip install transformers torch")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading model: {e}")
        sys.exit(1)


def generate_query_plan(query, model):
    """
    Generates a query plan for a given SQL query using the loaded model.
    """
    # Construct a prompt that guides the LLM to output a query plan.
    # You might need to provide schema information or specific instructions
    # depending on the model and the database system (e.g., PostgreSQL EXPLAIN format).
    prompt = (
        f"You are a database expert. Generate a valid postgres SQL query execution plan for the following query.\n"
        f"Query: {query}\n"
        f"Plan:"
    )

    try:
        # Generate response
        # max_new_tokens controls the length osql_f the generated output
        response = model(prompt, max_new_tokens=150,
                         num_return_sequences=1, do_sample=True, temperature=0.7)
        generated_text = response[0]['generated_text']

        # Extract the plan part. This logic depends heavily on the model's output format.
        # Here we assume the model appends the plan after the prompt.
        plan = generated_text[len(prompt):].strip()
        return plan
    except Exception as e:
        return f"Error generating plan: {e}"


def process_csv(input_file, output_file, query_column, model_name, limit=None):
    """
    Reads a CSV, generates query plans, and saves the result.
    """
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found.")
        return

    print(f"Reading CSV from {input_file}...")
    df = pd.read_csv(input_file)

    if query_column not in df.columns:
        print(
            f"Error: Column '{query_column}' not found in CSV. Available columns: {list(df.columns)}")
        return

    if limit:
        print(f"Limiting to first {limit} rows.")
        df = df.head(limit)

    # Load the model once
    model = load_llm_model(model_name)

    print(f"Generating query plans for {len(df)} queries...")

    # Enable progress_apply
    tqdm.pandas()

    # Apply the generation function to each row
    # We use a lambda to pass the model instance
    df['generated_query_plan'] = df[query_column].progress_apply(
        lambda q: generate_query_plan(q, model))

    print(f"Saving results to {output_file}...")
    df.to_csv(output_file, index=False)
    print("Processing complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate SQL query plans using an LLM.")

    parser.add_argument(
        "input_csv", help="Path to the input CSV file containing SQL queries.")
    parser.add_argument(
        "--output_csv", help="Path to the output CSV file.", default="output_with_plans.csv")
    parser.add_argument(
        "--column", help="Name of the column containing SQL queries.", default="sql_text")
    parser.add_argument(
        "--model", help="Name of the HuggingFace model to use.", default="Qwen/Qwen3-4B-Instruct-2507")
    parser.add_argument(
        "--limit", type=int, help="Limit the number of rows to process.", default=None)

    args = parser.parse_args()

    process_csv(args.input_csv, args.output_csv,
                args.column, args.model, args.limit)
