import time
import polars as pl

VALID = "VALID"
INVALID = "INVALID"
RECHECK = "RECHECK"


def load_data(path: str) -> pl.LazyFrame:
    return (
        pl.scan_csv(path)
        .select(pl.all().name.to_lowercase())
    )

def validate(df: pl.LazyFrame) -> pl.LazyFrame:
    sub_status = pl.col("sub_status").str.to_lowercase().fill_null("")
    title = pl.col("title").str.to_lowercase().fill_null("")
    req = pl.col("req").str.to_lowercase().fill_null("")
    email = pl.col("email").fill_null("")
    company = pl.col("company").str.to_lowercase().fill_null("")
    link = pl.col("employees_proflink").str.to_lowercase().fill_null("")
    status = pl.col("status").str.to_lowercase().fill_null("")

    # --- Title / PL ---
    title_invalid = (
        sub_status.str.contains("title/pl")
        & (
            title.eq("")
            | ~title.str.contains(req)
        )
    )

    # --- Other (auto) ---
    other_invalid = (
        sub_status.str.contains("other")
        & (company.eq("") | email.eq(""))
    )

    # --- Proflink ---
    valid_link = (
        link.str.contains("linkedin.com/in/")
        | link.str.contains("zoominfo.com/p/")
    ).fill_null(False)

    # Fixed: Use regex extraction instead of split/get for streaming safety
    # Extract domain from email using regex: everything after @ and before first dot
    email_domain = email.str.extract(r"@([^.]+)", 1).fill_null("")
    
    # Clean company name
    company_clean = company.str.replace_all(" ", "")
    
    # Check if email domain contains company name
    email_domain_match = (
        (email_domain.ne("") & company_clean.ne(""))
        & email_domain.str.contains(company_clean, literal=True)
    )

    proflink_invalid = (
        sub_status.str.contains("proflink")
        & ~(valid_link | email_domain_match)
    )

    # --- N1 / NWC ---
    n1_invalid = (
        sub_status.str.contains("n1")
        & (status.str.contains("r") | status.str.contains("f"))
    )

    n1_recheck = (
        sub_status.str.contains("n1")
        & (status.str.contains(r"\?") | status.str.contains("no info"))
    )

    return (
        df.with_columns(
            pl.when(title_invalid | other_invalid | proflink_invalid | n1_invalid)
            .then(pl.lit(INVALID))
            .when(n1_recheck)
            .then(pl.lit(RECHECK))
            .otherwise(pl.lit(VALID))
            .alias("Result")
        )
        .with_columns(
            pl.when(title_invalid)
            .then(pl.lit("Title does not meet keyword requirements"))
            .when(other_invalid)
            .then(pl.lit("Missing company or email"))
            .when(proflink_invalid)
            .then(pl.lit("Profile link does not meet requirements"))
            .when(n1_invalid)
            .then(pl.lit("Lead status invalid"))
            .when(n1_recheck)
            .then(pl.lit("Status requires recheck"))
            .otherwise(pl.lit(""))
            .alias("Comment")
        )
    )


def process_csv(input_path: str, output_path: str) -> None:
    start = time.perf_counter()

    df = validate(load_data(input_path)).collect(engine="streaming")

    df.write_csv(output_path)

    elapsed = time.perf_counter() - start

    summary = df.group_by("Result").len().sort("Result")

    print("\nProcessing completed")
    print(f"Rows processed: {df.height}")
    print(f"Processing time: {elapsed:.3f} seconds")
    print("\nResult summary:")
    print(summary)


if __name__ == "__main__":
    process_csv(
        input_path="test_input_50k.csv",
        output_path="processed_output_50k.csv",
    )