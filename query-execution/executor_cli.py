#!/usr/bin/env python3
"""
Interactive command-line interface for PostgreSQL query execution and benchmarking
"""

import sys
import argparse
from query_executor import SimpleQueryExecutor
from config import DB_CONFIG
import json


def print_results(result_dict, title="Results"):
    """Pretty print results dictionary"""
    print(f"\n=== {title} ===")
    for key, value in result_dict.items():
        if key == "execution_plan":
            print(
                f"{key}: [JSON execution plan - use --verbose to see full plan]"
            )
        elif key == "results":
            print(f"{key}: {value} (showing first 5 rows)")
        elif isinstance(value, float):
            print(f"{key}: {value:.2f}")
        else:
            print(f"{key}: {value}")
    print()


def print_execution_plan(plan, indent=0):
    """Recursively print execution plan in a readable format"""
    spacing = "  " * indent
    node_type = plan.get("Node Type", "Unknown")

    # Print current node
    if "Actual Total Time" in plan:
        time_info = f" (Time: {plan['Actual Total Time']:.2f}ms, Rows: {plan.get('Actual Rows', 0)})"
    else:
        time_info = f" (Est Cost: {plan.get('Total Cost', 0):.2f}, Est Rows: {plan.get('Plan Rows', 0)})"

    print(f"{spacing}{node_type}{time_info}")

    # Print relation name if it exists
    if "Relation Name" in plan:
        print(f"{spacing}  Table: {plan['Relation Name']}")

    # Print join condition if it exists
    if "Hash Cond" in plan:
        print(f"{spacing}  Join Condition: {plan['Hash Cond']}")
    elif "Merge Cond" in plan:
        print(f"{spacing}  Join Condition: {plan['Merge Cond']}")

    # Recursively print child plans
    if "Plans" in plan:
        for child_plan in plan["Plans"]:
            print_execution_plan(child_plan, indent + 1)


def interactive_mode():
    """Run interactive query execution loop"""
    executor = SimpleQueryExecutor()

    print("=== Interactive PostgreSQL Query Executor ===")
    print("Enter SQL queries to execute and benchmark.")
    print("Type 'quit' or 'exit' to end the session.")
    print("Type 'help' for available commands.\n")

    while True:
        try:
            # Get query from user
            print("Enter your SQL query (or command):")
            query = input("> ").strip()

            if not query:
                continue

            # Handle special commands
            if query.lower() in ["quit", "exit", "q"]:
                print("Goodbye!")
                break

            if query.lower() in ["help", "h"]:
                print("\nAvailable commands:")
                print("  help, h          - Show this help message")
                print("  quit, exit, q    - Exit the program")
                print("  clear, cls       - Clear the screen")
                print(
                    "\nTo execute a query with hints, you'll be prompted for hints after entering the query."
                )
                print(
                    "Example hints: /*+ HashJoin(table1 table2) SeqScan(table1) */\n"
                )
                continue

            if query.lower() in ["clear", "cls"]:
                import os

                os.system("cls" if os.name == "nt" else "clear")
                continue

            # Get optional hints
            print("\nEnter PostgreSQL hints (optional, press Enter to skip):")
            hints = input("Hints: ").strip()

            # Get benchmark iterations
            iterations_input = input(
                "Number of benchmark iterations (default 3): "
            ).strip()
            try:
                iterations = int(iterations_input) if iterations_input else 3
                iterations = max(1, min(iterations, 20))  # Limit between 1-20
            except ValueError:
                iterations = 3
                print("Invalid iterations input, using default (3)")

            print(
                f"\nExecuting query with {iterations} benchmark iterations..."
            )

            # Execute query
            if hints:
                print(f"Using hints: {hints}")
                plan_result = executor.execute_with_hints(query, hints)
            else:
                plan_result = executor.get_execution_plan(query, analyze=True)

            # Show execution plan results
            if plan_result.get("error"):
                print(f"Error executing query: {plan_result['error']}")
                continue

            print_results(plan_result, "Execution Plan Analysis")

            # Show readable execution plan
            if plan_result.get("execution_plan"):
                print("=== Execution Plan Tree ===")
                plan_data = plan_result["execution_plan"]["Plan"]
                print_execution_plan(plan_data)
                print()

            # Run benchmark
            benchmark_result = executor.benchmark_query(
                query, iterations=iterations
            )
            if benchmark_result.get("error"):
                print(f"Benchmark error: {benchmark_result['error']}")
            else:
                print_results(
                    benchmark_result,
                    f"Benchmark Results ({iterations} iterations)",
                )

            print("-" * 60)

        except KeyboardInterrupt:
            print("\n\nSession interrupted. Goodbye!")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            print("Continuing...")


def single_query_mode(query, hints=None, iterations=3, verbose=False):
    """Execute a single query and return results"""
    executor = SimpleQueryExecutor()

    print(f"Executing query: {query}")
    if hints:
        print(f"With hints: {hints}")

    # Execute with or without hints
    if hints:
        result = executor.execute_with_hints(query, hints)
    else:
        result = executor.get_execution_plan(query, analyze=True)

    # Handle errors
    if result.get("error"):
        print(f"Error: {result['error']}")
        return

    # Print execution plan results
    print_results(result, "Execution Plan Analysis")

    # Show detailed execution plan if verbose
    if verbose and result.get("execution_plan"):
        print("=== Detailed Execution Plan ===")
        print(json.dumps(result["execution_plan"], indent=2))
        print()

        print("=== Execution Plan Tree ===")
        plan_data = result["execution_plan"]["Plan"]
        print_execution_plan(plan_data)
        print()

    # Run benchmark
    if iterations > 1:
        benchmark_result = executor.benchmark_query(
            query, iterations=iterations
        )
        if benchmark_result.get("error"):
            print(f"Benchmark error: {benchmark_result['error']}")
        else:
            print_results(
                benchmark_result,
                f"Benchmark Results ({iterations} iterations)",
            )


def main():
    """Main entry point with command line argument parsing"""
    parser = argparse.ArgumentParser(
        description="Interactive PostgreSQL Query Executor and Benchmarker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python executor_cli.py                           # Interactive mode
  python executor_cli.py -q "SELECT * FROM users" # Single query
  python executor_cli.py -q "SELECT * FROM users" --hints "/*+ SeqScan(users) */"
  python executor_cli.py -q "SELECT * FROM users" -i 5 --verbose
        """,
    )

    parser.add_argument(
        "-q",
        "--query",
        help="SQL query to execute (if not provided, runs in interactive mode)",
    )
    parser.add_argument(
        "--hints",
        help='PostgreSQL hints to apply (e.g., "/*+ HashJoin(a b) */")',
    )
    parser.add_argument(
        "-i",
        "--iterations",
        type=int,
        default=3,
        help="Number of benchmark iterations (default: 3, max: 20)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show detailed execution plan JSON",
    )

    args = parser.parse_args()

    # Validate iterations
    if args.iterations < 1 or args.iterations > 20:
        print("Iterations must be between 1 and 20")
        return 1

    # Run in single query mode if query provided
    if args.query:
        single_query_mode(
            args.query, args.hints, args.iterations, args.verbose
        )
    else:
        # Run in interactive mode
        interactive_mode()

    return 0


if __name__ == "__main__":
    sys.exit(main())
