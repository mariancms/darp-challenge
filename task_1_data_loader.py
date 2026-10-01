from pathlib import Path
from datetime import datetime
import re
import sqlite3

import polars as pl

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = BASE_DIR / "assessment.db"


# Database schema

def create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE accounts (
            account_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            address TEXT NOT NULL
        );

        CREATE TABLE daily_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            queue TEXT NOT NULL,
            status TEXT NOT NULL,
            changed_datetime TEXT NOT NULL,

            FOREIGN KEY (account_id)
                REFERENCES accounts(account_id)
        );

        CREATE TABLE monthly_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            queue TEXT NOT NULL,
            status TEXT NOT NULL,
            month INTEGER NOT NULL,
            snapshot_month TEXT NOT NULL,

            FOREIGN KEY (account_id)
                REFERENCES accounts(account_id)
        );
        """
    )


def parse_changed_datetime(df: pl.DataFrame) -> pl.DataFrame:
    # Found multiple datetime formats for changed_datetime in the data, use this to normalize the datetime

    # Preserve the raw source value temporarily
    df = df.with_columns(
        pl.col("changed_datetime")
        .cast(pl.String)
        .alias("_raw_changed_datetime")
    )

    raw = pl.col("_raw_changed_datetime")

    excel_serial = raw.cast(pl.Float64, strict=False)
    excel_datetime = pl.from_epoch(
        (
                (excel_serial - 25569) * 86400
        )
        .round()
        .cast(pl.Int64),
        time_unit="s",
    )

    df = df.with_columns(
        pl.coalesce(
            [
                # Standard format
                raw.str.strptime(
                    pl.Datetime,
                    format="%Y-%m-%d %H:%M:%S",
                    strict=False,
                ),

                # Alternative format
                raw.str.strptime(
                    pl.Datetime,
                    format="%d-%m-%y %H:%M",
                    strict=False,
                ),

                # Excel datetime.
                excel_datetime,
            ]
        ).alias("changed_datetime")
    )

    # Make sure nothing remained unparsed.
    invalid_rows = df.filter(
        pl.col("changed_datetime").is_null()
    )

    if invalid_rows.height > 0:
        invalid_values = invalid_rows[
            "_raw_changed_datetime"
        ].to_list()

        raise ValueError(
            "Could not parse changed_datetime values:\n"
            f"{invalid_values}"
        )

    return df.drop("_raw_changed_datetime")


# Accounts

def load_accounts(conn: sqlite3.Connection) -> None:
    file_path = DATA_DIR / "accounts.csv"

    if not file_path.exists():
        raise FileNotFoundError(
            f"Accounts file not found: {file_path}"
        )

    df = pl.read_csv(file_path)

    df = df.select(
        pl.col("account_id").cast(pl.Int64),
        pl.col("name"),
        pl.col("address"),
    )

    # Check that account_id's are unique
    duplicate_accounts = (
        df.group_by("account_id")
        .len()
        .filter(pl.col("len") > 1)
    )

    if duplicate_accounts.height > 0:
        raise ValueError(
            "Duplicate account IDs found:\n"
            f"{duplicate_accounts}"
        )

    conn.executemany(
        """
        INSERT INTO accounts (account_id,
                              name,
                              address)
        VALUES (?, ?, ?)
        """,
        df.iter_rows(),
    )

    print(f"Loaded {df.height} accounts")


# Daily status

def load_daily_status(conn: sqlite3.Connection) -> None:
    daily_files = sorted(
        DATA_DIR.glob("daily_*.csv")
    )

    if not daily_files:
        raise FileNotFoundError(
            f"No daily files found in {DATA_DIR}"
        )

    total_rows = 0

    for file_path in daily_files:
        # Validate filename
        match = re.fullmatch(
            r"daily_(\d{4})(\d{2})(\d{2})\.csv",
            file_path.name,
        )

        if not match:
            raise ValueError(
                f"Unexpected daily filename: "
                f"{file_path.name}"
            )

        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))

        file_date = datetime(
            year,
            month,
            day,
        ).date()

        df = pl.read_csv(file_path)

        # Normalize all datetime formats.
        df = parse_changed_datetime(df)

        # Prepare the dataframe for insertion.
        df = df.select(
            pl.col("account")
            .cast(pl.Int64)
            .alias("account_id"),

            pl.col("queue"),

            pl.col("status"),

            # Store SQLite datetime consistently as text.
            pl.col("changed_datetime")
            .dt.strftime("%Y-%m-%d %H:%M:%S"),
        )

        conn.executemany(
            """
            INSERT INTO daily_status (account_id,
                                      queue,
                                      status,
                                      changed_datetime)
            VALUES (?, ?, ?, ?)
            """,
            df.iter_rows(),
        )

        total_rows += df.height

    print(
        f"Loaded {total_rows} daily status rows "
        f"from {len(daily_files)} files"
    )


# Monthly status

def load_monthly_status(
        conn: sqlite3.Connection,
) -> None:
    monthly_files = sorted(
        DATA_DIR.glob("monthly_*.csv")
    )

    if not monthly_files:
        raise FileNotFoundError(
            f"No monthly files found"
        )

    total_rows = 0

    for file_path in monthly_files:
        match = re.fullmatch(
            r"monthly_(\d{4})(\d{2})\.csv",
            file_path.name,
        )

        if not match:
            raise ValueError(
                f"Unexpected monthly filename"
            )

        year = int(match.group(1))
        file_month = int(match.group(2))

        df = pl.read_csv(file_path)

        # Store year/month representation
        snapshot_month = (
            f"{year:04d}-{file_month:02d}-01"
        )

        df = (
            df.select(
                pl.col("account")
                .cast(pl.Int64)
                .alias("account_id"),

                pl.col("queue"),

                pl.col("status"),

                pl.col("month")
                .cast(pl.Int64),
            )
            .with_columns(
                pl.lit(snapshot_month)
                .alias("snapshot_month")
            )
        )

        conn.executemany(
            """
            INSERT INTO monthly_status (account_id,
                                        queue,
                                        status,
                                        month,
                                        snapshot_month)
            VALUES (?, ?, ?, ?, ?)
            """,
            df.iter_rows(),
        )

        total_rows += df.height

    print(
        f"Loaded {total_rows} monthly status rows "
        f"from {len(monthly_files)} files"
    )


# Database validation

def validate_database(
        conn: sqlite3.Connection,
) -> None:
    print("\nDatabase row counts:")

    tables = (
        "accounts",
        "daily_status",
        "monthly_status",
    )

    for table in tables:
        count = conn.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]

        print(f"{table}: {count}")

    # Validate foreign-key relationships
    foreign_key_issues = conn.execute(
        "PRAGMA foreign_key_check"
    ).fetchall()

    if foreign_key_issues:
        raise ValueError(
            "Foreign key errors found:\n"
            f"{foreign_key_issues}"
        )

    print("Foreign key validation: OK")


def main() -> None:
    if not DATA_DIR.exists():
        raise FileNotFoundError(
            f"Data directory not found: "
            f"{DATA_DIR}"
        )

    # Rebuild the database each time
    if DB_PATH.exists():
        DB_PATH.unlink()

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "PRAGMA foreign_keys = ON"
        )

        create_tables(conn)
        load_accounts(conn)
        load_daily_status(conn)
        load_monthly_status(conn)
        conn.commit()
        validate_database(conn)

    print(f"Database created successfully")


if __name__ == "__main__":
    main()
