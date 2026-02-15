# LLM Query Plan Generator

This script (`llm_initial_pipeline.py`) reads a CSV file containing SQL queries, uses a Large Language Model (LLM) from Hugging Face to generate execution plans for those queries, and saves the results to a new CSV file.

## Prerequisites

Ensure you have Python installed. You will need to install the required libraries:

```bash
pip install pandas transformers torch
```

## Usage

Run the script from the command line. The basic usage is:

```bash
python llm_initial_pipeline.py <input_csv_path> [options]
```

### Arguments

- `input_csv`: **(Required)** Path to the input CSV file containing SQL queries.
- `--output_csv`: Path to the output CSV file. Defaults to `output_with_plans.csv`.
- `--column`: Name of the column in the input CSV that contains the SQL queries. Defaults to `sql_text`.
- `--model`: Name of the Hugging Face model to use. Defaults to `distilgpt2`.
- `--limit`: Limit the number of rows to process (useful for testing). Defaults to processing all rows.

### Examples

**1. Basic usage with default settings:**

```bash
python llm_initial_pipeline.py stackoverflow_n18147.csv
```

**2. Testing with a small number of queries (e.g., first 10):**

```bash
python llm_initial_pipeline.py stackoverflow_n18147.csv --limit 10
```

**3. Using a specific model and output file:**

```bash
python llm_initial_pipeline.py stackoverflow_n18147.csv --output_csv my_results.csv --model "google/gemma-2b"
```

**4. Specifying a different column name for queries:**

```bash
python llm_initial_pipeline.py my_data.csv --column "query_string"
```

## Model Selection

The script defaults to `distilgpt2`, which is small and fast but may not generate accurate SQL execution plans. For better results, consider using models specialized for code or SQL, such as:

- `defog/sqlcoder-7b-2`
- `codellama/CodeLlama-7b-hf`
- `google/gemma-2b` (Good balance of size and performance for local testing)

**Note:** Larger models require more RAM/VRAM.
