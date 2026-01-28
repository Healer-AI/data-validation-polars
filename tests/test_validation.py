import polars as pl
from data_validation_fast import validate

def run_validation(data: dict):
    df = pl.DataFrame(data).lazy()
    return validate(df).collect()


def test_title_pl_valid():
    df = run_validation({
        "title": ["Senior Product Manager"],
        "req": ["product manager"],
        "sub_status": ["Title/PL"],
        "company": ["Acme"],
        "email": ["john@acme.com"],
        "employees_proflink": [None],
        "status": [None],
    })

    assert df["Result"][0] == "VALID"


def test_title_pl_invalid():
    df = run_validation({
        "title": ["Engineer"],
        "req": ["product manager"],
        "sub_status": ["Title/PL"],
        "company": ["Acme"],
        "email": ["john@acme.com"],
        "employees_proflink": [None],
        "status": [None],
    })

    assert df["Result"][0] == "INVALID"


def test_proflink_valid_linkedin():
    df = run_validation({
        "title": ["CTO"],
        "req": [""],
        "sub_status": ["Proflink"],
        "company": ["Globex"],
        "email": ["cto@globex.com"],
        "employees_proflink": ["https://linkedin.com/in/test"],
        "status": [None],
    })

    assert df["Result"][0] == "VALID"


def test_n1_recheck():
    df = run_validation({
        "title": ["Director"],
        "req": [""],
        "sub_status": ["N1"],
        "company": ["Initech"],
        "email": ["anna@initech.com"],
        "employees_proflink": [None],
        "status": ["N?"],
    })

    assert df["Result"][0] == "RECHECK"
