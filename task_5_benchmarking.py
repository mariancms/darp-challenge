from pathlib import Path
import sqlite3
import statistics
import time

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "assessment.db"

OUTPUT_DIR = BASE_DIR / "output"
REPORT_PATH = OUTPUT_DIR / "task_5_performance.txt"

REPETITIONS = 20
WARMUP_RUNS = 3

BASELINE_QUERY = """
                 SELECT d.account_id,
                        a.name,
                        a.address,
                        d.queue,
                        d.status,
                        d.changed_datetime

                 FROM daily_status AS d

                          INNER JOIN accounts AS a
                                     ON a.account_id = d.account_id

                 WHERE EXISTS (SELECT 1
                               FROM daily_status AS history
                               WHERE history.account_id = d.account_id
                                 AND history.queue IN ('COLLECTIONS', 'LEGAL'))

                    OR EXISTS (SELECT 1
                               FROM monthly_status AS monthly
                               WHERE monthly.account_id = d.account_id
                                 AND monthly.queue IN ('COLLECTIONS', 'LEGAL'))

                 ORDER BY d.account_id,
                          d.changed_datetime; \
                 """

OPTIMIZED_QUERY = """
                  WITH qualifying_accounts AS (SELECT account_id
                                               FROM daily_status
                                               WHERE queue IN ('COLLECTIONS', 'LEGAL')

                                               UNION

                                               SELECT account_id
                                               FROM monthly_status
                                               WHERE queue IN ('COLLECTIONS', 'LEGAL'))

                  SELECT d.account_id,
                         a.name,
                         a.address,
                         d.queue,
                         d.status,
                         d.changed_datetime

                  FROM qualifying_accounts AS q

                           INNER JOIN daily_status AS d
                                      ON d.account_id = q.account_id

                           INNER JOIN accounts AS a
                                      ON a.account_id = d.account_id

                  ORDER BY d.account_id,
                           d.changed_datetime; \
                  """


# Index handling

def remove_indexes(
        conn: sqlite3.Connection,
) -> None:
    # Removing indexes allows us to obtain a clean baseline again if the script is rerun.
    conn.execute(
        "DROP INDEX IF EXISTS idx_daily_queue_account"
    )

    conn.execute(
        "DROP INDEX IF EXISTS idx_monthly_queue_account"
    )

    conn.execute(
        "DROP INDEX IF EXISTS idx_daily_account_datetime"
    )

    conn.commit()


def create_indexes(
        conn: sqlite3.Connection,
) -> None:
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_daily_queue_account
            ON daily_status (queue, account_id)
        """
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_monthly_queue_account
            ON monthly_status (queue, account_id)
        """
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_daily_account_datetime
            ON daily_status (account_id, changed_datetime)
        """
    )

    conn.commit()


# Benchmark

def benchmark_query(
        conn: sqlite3.Connection,
        query: str,
) -> dict:

    # Warm up SQLite and filesystem cache.
    for _ in range(WARMUP_RUNS):
        conn.execute(query).fetchall()

    times_ms = []

    result = None

    for _ in range(REPETITIONS):
        start = time.perf_counter_ns()

        result = conn.execute(query).fetchall()

        end = time.perf_counter_ns()

        elapsed_ms = (end - start) / 1_000_000

        times_ms.append(elapsed_ms)

    return {
        "rows": result,
        "mean_ms": statistics.mean(times_ms),
        "median_ms": statistics.median(times_ms),
        "min_ms": min(times_ms),
        "max_ms": max(times_ms),
    }


# Query plan

def get_query_plan(
        conn: sqlite3.Connection,
        query: str,
) -> list[str]:
    rows = conn.execute(
        "EXPLAIN QUERY PLAN " + query
    ).fetchall()

    return [
        f"{row[0]} | {row[1]} | {row[2]} | {row[3]}"
        for row in rows
    ]


# Validation

def get_qualifying_account_count(
        conn: sqlite3.Connection,
) -> int:
    query = """
            SELECT COUNT(*)
            FROM (SELECT account_id
                  FROM daily_status
                  WHERE queue IN ('COLLECTIONS', 'LEGAL')

                  UNION

                  SELECT account_id
                  FROM monthly_status
                  WHERE queue IN ('COLLECTIONS', 'LEGAL')) \
            """

    return conn.execute(query).fetchone()[0]


def validate_results(
        baseline_rows: list,
        optimized_rows: list,
) -> None:

    if baseline_rows != optimized_rows:
        raise ValueError(
            "Baseline and optimized queries returned different results"
        )


# Report

def write_report(
    qualifying_accounts: int,
    baseline: dict,
    optimized: dict,
    baseline_plan: list[str],
    optimized_plan: list[str],
) -> None:

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    speedup = (
        baseline["median_ms"]
        / optimized["median_ms"]
    )

    improvement_percent = (
        (
            baseline["median_ms"]
            - optimized["median_ms"]
        )
        / baseline["median_ms"]
        * 100
    )

    report = f"""
PERFORMANCE COMPARISON

Dataset
-------
Qualifying accounts: {qualifying_accounts}
Rows returned: {len(optimized["rows"])}
Benchmark runs: {REPETITIONS}


Baseline
--------
The baseline query used correlated EXISTS checks on daily_status
and monthly_status for each daily status row.

Median time: {baseline["median_ms"]:.3f} ms
Mean time: {baseline["mean_ms"]:.3f} ms


Improvements
------------
Logical:
- Qualifying accounts are identified once using a CTE and UNION.
- The result is then joined to daily_status and accounts.

Database:
- Added index on daily_status(queue, account_id)
- Added index on monthly_status(queue, account_id)
- Added index on daily_status(account_id, changed_datetime)


Optimized
---------
Median time: {optimized["median_ms"]:.3f} ms
Mean time: {optimized["mean_ms"]:.3f} ms


Comparison
----------
Speedup: {speedup:.2f}x
Median time reduction: {improvement_percent:.2f}%


Validation
----------
Baseline and optimized queries returned the same results.


Baseline Query Plan
-------------------
{chr(10).join(baseline_plan)}


Optimized Query Plan
--------------------
{chr(10).join(optimized_plan)}

""".strip()

    REPORT_PATH.write_text(
        report,
        encoding="utf-8",
    )

    print(f"\nPerformance report written to:\n{REPORT_PATH}")


# Main

def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found: {DB_PATH}"
        )

    with sqlite3.connect(DB_PATH) as conn:
        # Baseline
        remove_indexes(conn)

        print("Running baseline benchmark...")

        baseline_plan = get_query_plan(
            conn,
            BASELINE_QUERY,
        )

        baseline = benchmark_query(
            conn,
            BASELINE_QUERY,
        )

        print(
            f"Baseline median: "
            f"{baseline['median_ms']:.3f} ms"
        )

        # Optimization
        create_indexes(conn)

        print("Running optimized benchmark...")

        optimized_plan = get_query_plan(
            conn,
            OPTIMIZED_QUERY,
        )

        optimized = benchmark_query(
            conn,
            OPTIMIZED_QUERY,
        )

        print(
            f"Optimized median: "
            f"{optimized['median_ms']:.3f} ms"
        )

        # Validation
        validate_results(
            baseline["rows"],
            optimized["rows"],
        )

        qualifying_accounts = (
            get_qualifying_account_count(conn)
        )

        write_report(
            qualifying_accounts,
            baseline,
            optimized,
            baseline_plan,
            optimized_plan,
        )


if __name__ == "__main__":
    main()
