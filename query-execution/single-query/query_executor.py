#!/usr/bin/env python3
"""
Simplified query executor for Cardinal project initial setup
Just focuses on running PostgreSQL queries and collecting execution plans
"""

import psycopg2
import json
import time
from typing import Dict, List, Tuple, Any
from config import DB_CONFIG


class SimpleQueryExecutor:
    def __init__(self, **kwargs):
        """
        Initialize with database configuration from config.py
        Override specific parameters with kwargs if needed
        """
        self.connection_params = DB_CONFIG.copy()
        self.connection_params.update(kwargs)  # Allow overrides

    def get_connection(self):
        """Create a database connection"""
        return psycopg2.connect(**self.connection_params)

    def get_execution_plan(
        self, query: str, analyze: bool = False
    ) -> Dict[str, Any]:
        """
        Get execution plan for a query

        Args:
            query: SQL query string
            analyze: If True, actually execute query and get real stats

        Returns:
            Dictionary containing execution plan and metadata
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Build EXPLAIN command
            if analyze:
                explain_cmd = (
                    "EXPLAIN (ANALYZE true, BUFFERS true, FORMAT JSON) "
                )
            else:
                explain_cmd = "EXPLAIN (FORMAT JSON) "

            explain_query = explain_cmd + query

            # Execute EXPLAIN
            start_time = time.time()
            cursor.execute(explain_query)
            explain_result = cursor.fetchone()[0]
            explain_time = time.time() - start_time

            # Extract plan information
            plan = explain_result[0]["Plan"]
            result = {
                "query": query,
                "execution_plan": explain_result[0],
                "explain_time_ms": round(explain_time * 1000, 2),
                "analyzed": analyze,
            }

            if analyze:
                result.update(
                    {
                        "actual_total_time": plan.get("Actual Total Time", 0),
                        "actual_rows": plan.get("Actual Rows", 0),
                        "planning_time": explain_result[0].get(
                            "Planning Time", 0
                        ),
                        "execution_time": explain_result[0].get(
                            "Execution Time", 0
                        ),
                    }
                )
            else:
                result.update(
                    {
                        "estimated_cost": plan.get("Total Cost", 0),
                        "estimated_rows": plan.get("Plan Rows", 0),
                        "planning_time": explain_result[0].get(
                            "Planning Time", 0
                        ),
                    }
                )

            return result

        except Exception as e:
            return {"query": query, "error": str(e), "execution_plan": None}
        finally:
            cursor.close()
            conn.close()

    def execute_query(self, query: str) -> Dict[str, Any]:
        """
        Execute query and measure performance
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            start_time = time.time()
            cursor.execute(query)
            results = cursor.fetchall()
            execution_time = time.time() - start_time

            return {
                "query": query,
                "execution_time_ms": round(execution_time * 1000, 2),
                "row_count": len(results),
                "results": results[:5],  # First 5 rows only
                "success": True,
            }

        except Exception as e:
            return {"query": query, "error": str(e), "success": False}
        finally:
            cursor.close()
            conn.close()

    def execute_with_hints(self, query: str, hints: str) -> Dict[str, Any]:
        """
        Execute query with PostgreSQL hints (requires pg_hint_plan extension)

        Args:
            query: SQL query
            hints: Hint string (e.g., "/*+ HashJoin(a b) SeqScan(c) */")
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Try to load pg_hint_plan if available
            cursor.execute("SELECT 1")  # Test connection first

            # Combine hints with query
            hinted_query = f"{hints}\n{query}"

            # Get execution plan with hints
            return self.get_execution_plan(hinted_query, analyze=True)

        except Exception as e:
            return {
                "query": query,
                "hints": hints,
                "error": f"Failed to execute with hints: {str(e)}",
            }
        finally:
            cursor.close()
            conn.close()

    def compare_execution_strategies(
        self, query: str, hint_variations: List[str]
    ) -> Dict[str, Any]:
        """
        Compare different execution strategies for the same query

        Args:
            query: SQL query
            hint_variations: List of hint strings to test
        """
        results = []

        # Test default execution
        default_result = self.get_execution_plan(query, analyze=True)
        if not default_result.get("error"):
            results.append(
                {
                    "strategy": "default",
                    "hints": None,
                    "execution_time": default_result.get(
                        "actual_total_time", 0
                    ),
                    "rows": default_result.get("actual_rows", 0),
                }
            )

        # Test each hint variation
        for i, hints in enumerate(hint_variations):
            hint_result = self.execute_with_hints(query, hints)
            if not hint_result.get("error"):
                results.append(
                    {
                        "strategy": f"hint_{i+1}",
                        "hints": hints,
                        "execution_time": hint_result.get(
                            "actual_total_time", 0
                        ),
                        "rows": hint_result.get("actual_rows", 0),
                    }
                )

        return {
            "query": query,
            "strategies_tested": len(results),
            "results": results,
            "best_strategy": (
                min(results, key=lambda x: x["execution_time"])
                if results
                else None
            ),
        }

    def benchmark_query(
        self, query: str, iterations: int = 5
    ) -> Dict[str, Any]:
        """
        Run query multiple times and collect performance statistics
        """
        results = []

        for i in range(iterations):
            result = self.execute_query(query)
            if result["success"]:
                results.append(result["execution_time_ms"])
            else:
                return {
                    "error": f"Query failed on iteration {i+1}: {result['error']}"
                }

        if results:
            avg_time = sum(results) / len(results)
            min_time = min(results)
            max_time = max(results)

            return {
                "query": query,
                "iterations": iterations,
                "avg_time_ms": round(avg_time, 2),
                "min_time_ms": min_time,
                "max_time_ms": max_time,
                "all_times": results,
            }
        else:
            return {"error": "No successful executions"}


def main():
    """Test the simplified executor"""
    executor = SimpleQueryExecutor()

    test_queries = [
        "SELECT * FROM customers WHERE country = 'USA'",
        """
        SELECT c.name, COUNT(o.order_id) as order_count
        FROM customers c 
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.name
        ORDER BY order_count DESC
        """,
    ]

    print("=== Simple Query Executor Test ===\n")

    for i, query in enumerate(test_queries, 1):
        print(f"Query {i}: {query.strip()}")

        # Get execution plan (estimated)
        plan_result = executor.get_execution_plan(query, analyze=False)
        if plan_result.get("execution_plan"):
            print(f"  Estimated Cost: {plan_result.get('estimated_cost')}")
            print(f"  Estimated Rows: {plan_result.get('estimated_rows')}")

        # Get actual execution stats
        analyze_result = executor.get_execution_plan(query, analyze=True)
        if not analyze_result.get("error"):
            print(
                f"  Actual Time: {analyze_result.get('actual_total_time')} ms"
            )
            print(f"  Actual Rows: {analyze_result.get('actual_rows')}")

        # Benchmark
        benchmark = executor.benchmark_query(query, iterations=3)
        if not benchmark.get("error"):
            print(f"  Avg Execution: {benchmark['avg_time_ms']} ms")

        print()

    # Test hint functionality with join query
    print("=== Hint Testing ===\n")
    join_query = """
    SELECT c.name, oi.product_name, SUM(oi.quantity) as total_quantity
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.status = 'completed'
    GROUP BY c.name, oi.product_name
    ORDER BY total_quantity DESC
    """

    # Test different hint strategies
    hint_variations = [
        "/*+ HashJoin(c o) HashJoin(o oi) */",
        "/*+ NestLoop(c o) HashJoin(o oi) */",
        "/*+ SeqScan(customers) SeqScan(orders) */",
    ]

    print(f"Testing query with different hints: {join_query.strip()}")
    print()

    comparison = executor.compare_execution_strategies(
        join_query, hint_variations
    )
    if comparison.get("results"):
        print(f"Strategies tested: {comparison['strategies_tested']}")
        for result in comparison["results"]:
            strategy_name = result["strategy"]
            exec_time = result["execution_time"]
            hints = result.get("hints", "None")
            print(f"  {strategy_name}: {exec_time:.2f}ms - Hints: {hints}")

        if comparison.get("best_strategy"):
            best = comparison["best_strategy"]
            print(
                f"\nBest strategy: {best['strategy']} ({best['execution_time']:.2f}ms)"
            )
    else:
        print(
            "No successful hint comparisons (pg_hint_plan may not be available)"
        )
        print("\n" + "=" * 50)
        print(
            "Note: For hint functionality to work, install pg_hint_plan extension:"
        )
        print("  brew install pg_hint_plan  # macOS")
        print("  sudo apt-get install postgresql-14-pg-hint-plan  # Ubuntu")
        print("  Then: CREATE EXTENSION pg_hint_plan;")


if __name__ == "__main__":
    main()
