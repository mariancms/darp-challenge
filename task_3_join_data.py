from pathlib import Path
import sqlite3

import polars as pl

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "assessment.db"

OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_FILE = OUTPUT_DIR / "account_activity.csv"


def create_account_activity_table(
        conn: sqlite3.Connection,
) -> None:

    conn.execute("DROP TABLE IF EXISTS account_activity")

    conn.execute(
        """
        CREATE TABLE account_activity AS

        WITH ranked_activity AS (SELECT account_id,
                                        queue,
                                        status,
                                        changed_datetime,

                                        ROW_NUMBER() OVER (
                    PARTITION BY account_id
                    ORDER BY changed_datetime DESC, id DESC
                ) AS row_number

                                 FROM daily_status

                                 WHERE changed_datetime >= '2025-01-01 00:00:00')

        SELECT a.account_id,
               a.name,
               a.address,
               r.changed_datetime AS latest_update_datetime,
               r.queue,
               r.status

        FROM ranked_activity AS r

                 INNER JOIN accounts AS a
                            ON a.account_id = r.account_id

        WHERE r.row_number = 1;
        """
    )

    conn.commit()


def validate_account_activity(
        conn: sqlite3.Connection,
) -> None:

    total_rows = conn.execute(
        """
        SELECT COUNT(*)
        FROM account_activity
        """
    ).fetchone()[0]

    unique_accounts = conn.execute(
        """
        SELECT COUNT(DISTINCT account_id)
        FROM account_activity
        """
    ).fetchone()[0]

    null_rows = conn.execute(
        """
        SELECT COUNT(*)
        FROM account_activity
        WHERE account_id IS NULL
           OR name IS NULL
           OR address IS NULL
           OR latest_update_datetime IS NULL
           OR queue IS NULL
           OR status IS NULL
        """
    ).fetchone()[0]

    print(f"Rows in final dataset: {total_rows}")
    print(f"Unique accounts: {unique_accounts}")
    print(f"Rows containing null values: {null_rows}")

    if total_rows != unique_accounts:
        raise ValueError(
            "The result contains more than one row "
            "per account."
        )

    if null_rows > 0:
        raise ValueError(
            "The final dataset contains null values."
        )


def export_to_csv(
        conn: sqlite3.Connection,
) -> None:

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    query = """
            SELECT account_id,
                   name,
                   address,
                   latest_update_datetime,
                   queue,
                   status
            FROM account_activity
            ORDER BY account_id; \
            """

    df = pl.read_database(
        query=query,
        connection=conn,
    )

    df.write_csv(OUTPUT_FILE)

    print(f"CSV exported to: {OUTPUT_FILE}")


def show_sample(
        conn: sqlite3.Connection,
) -> None:

    query = """
            SELECT *
            FROM account_activity
            ORDER BY account_id LIMIT 10; \
            """

    df = pl.read_database(
        query=query,
        connection=conn,
    )

    print("\nSample:")
    print(df)


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found: {DB_PATH}"
        )

    with sqlite3.connect(DB_PATH) as conn:
        create_account_activity_table(conn)

        validate_account_activity(conn)

        show_sample(conn)

        export_to_csv(conn)


if __name__ == "__main__":
    main()
