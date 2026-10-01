from pathlib import Path
import sqlite3

import polars as pl


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "assessment.db"

START_DATE = "2025-01-01 00:00:00"


def get_active_accounts(conn: sqlite3.Connection) -> pl.DataFrame:
    query = """
        SELECT DISTINCT account_id
        FROM daily_status
        WHERE changed_datetime >= ?
        ORDER BY account_id;
    """

    return pl.read_database(
        query=query,
        connection=conn,
        execute_options={
            "parameters": (START_DATE,)
        },
    )


def main() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        active_accounts = get_active_accounts(conn)

    print(active_accounts)

    print(
        f"\nNumber of accounts with activity "
        f"on or after 2025-01-01: "
        f"{active_accounts.height}"
    )

    # Check for the known edge case
    if 99999 in active_accounts["account_id"].to_list():
        raise ValueError(
            "Account 99999 should not be included because its last activity was before 2025-01-01."
        )

    print("Validation OK: account 99999 is correctly excluded.")


if __name__ == "__main__":
    main()