from pathlib import Path
import sqlite3

import polars as pl


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "assessment.db"

OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_FILE = OUTPUT_DIR / "collections_legal_latest_updates.csv"


def get_latest_updates(conn: sqlite3.Connection) -> pl.DataFrame:
    query = """
        WITH latest_november AS (
            SELECT *
            FROM (
                SELECT
                    account_id,
                    queue,
                    status,
                    changed_datetime,
                    ROW_NUMBER() OVER (
                        PARTITION BY account_id
                        ORDER BY changed_datetime DESC, id DESC
                    ) AS rn
                FROM daily_status
                WHERE changed_datetime < '2025-11-27 00:00:00'
                  AND changed_datetime >= '2025-11-01 00:00:00'
            )
            WHERE rn = 1
        ),

        accounts_at_cutoff AS (
            SELECT
                m.account_id,
                COALESCE(n.queue, m.queue) AS queue,
                COALESCE(n.status, m.status) AS status
            FROM monthly_status AS m

            LEFT JOIN latest_november AS n
                ON m.account_id = n.account_id

            WHERE m.snapshot_month = '2025-10-01'
        ),

        qualifying_accounts AS (
            SELECT account_id
            FROM accounts_at_cutoff
            WHERE queue IN ('COLLECTIONS', 'LEGAL')
        ),

        latest_change AS (
            SELECT *
            FROM (
                SELECT
                    d.account_id,
                    d.queue,
                    d.status,
                    d.changed_datetime,
                    ROW_NUMBER() OVER (
                        PARTITION BY d.account_id
                        ORDER BY d.changed_datetime DESC, d.id DESC
                    ) AS rn
                FROM daily_status AS d

                INNER JOIN qualifying_accounts AS q
                    ON d.account_id = q.account_id

                WHERE d.changed_datetime < '2025-11-27 00:00:00'
            )
            WHERE rn = 1
        )

        SELECT
            a.account_id,
            a.name,
            a.address,
            l.changed_datetime,
            l.queue,
            l.status

        FROM qualifying_accounts AS q

        INNER JOIN accounts AS a
            ON a.account_id = q.account_id

        LEFT JOIN latest_change AS l
            ON l.account_id = q.account_id

        ORDER BY a.account_id;
    """

    return pl.read_database(
        query=query,
        connection=conn,
    )


def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        result = get_latest_updates(conn)

    print(result)
    print(f"\nQualifying accounts: {result.height}")

    result.write_csv(OUTPUT_FILE)

    print(f"CSV exported to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()