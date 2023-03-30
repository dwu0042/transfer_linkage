import polars as pl
import overlap_fixer as ovfxr
from functools import partial

_nulls = [
    "", "NA", "na", "Na", "N/A", "n/a", "N/a", "NaN", "''", " ", "NULL",
]

class DataHandlingError(Exception):
    pass

def ingest_csv(
    csv_path,
    convert_dates=False,
    ):

    return pl.read_csv(csv_path, has_header=True, try_parse_dates=convert_dates, null_values=_nulls)

def clean_database(
    database:pl.DataFrame, 
    delete_missing=False, 
    delete_errors=False,
    convert_dates=False,
    date_format=r"%Y-%m-%d",
    subject_id="sID",
    facility_id="fID",
    admission_date='Adate',
    discharge_date='Ddate',
    retain_auxiliary_data=True,
    verbose=True,
    **kwargs):

    # Check column existence
    expected_cols = {subject_id, facility_id, admission_date, discharge_date}
    found_cols = set(database.columns)
    missing_cols = expected_cols.difference (found_cols)
    if len(missing_cols):
        raise DataHandlingError(f"Column(s) {', '.join(missing_cols)} provided as argument were not found in the database.")

    # Standardise column names
    report = database.rename({
        subject_id: 'sID',
        facility_id: 'fID',
        admission_date: 'Adate',
        discharge_date: 'Ddate',
    })

    # Check data format, column names, variable format, parse dates
    if convert_dates:
        date_expressions = [
            pl.col('Adate').str.strptime(pl.Date, fmt=date_format),
            pl.col('Ddate').str.strptime(pl.Date, fmt=date_format),
        ]
    else:
        [
            pl.col('Adate').cast(pl.Utf8),
            pl.col('Ddate').cast(pl.Utf8)
        ]
    # Coerce types
    report = report.with_columns([
        pl.col('sID').cast(pl.Utf8),
        pl.col('fID').cast(pl.Utf8),
        *date_expressions,
    ])

    # Trim auxiliary data
    if not retain_auxiliary_data:
        report = report.select(pl.col('sID', 'fID', 'Adate', 'Ddate'))

    # Check and clean missing values
    report = clean_missing_values(report, delete_missing=delete_missing, verbose=verbose)

    # Check erroneous records
    report = clean_erroneous_records(report, delete_errors=delete_errors, verbose=verbose)

    # remove row duplicates
    report = report.unique()

    # Fix overlapping stays
    subject_chunks = report.partition_by('sID')
    overlap_fixing_method = partial(fix_overlapping_stays, verbose=verbose)
    report = pl.concat(map(overlap_fixing_method, subject_chunks))

    return report

def clean_missing_values(database:pl.DataFrame, delete_missing=False, verbose=True):
    """Checks for and potentially deletes recods with missing values"""
    # Check for missing values
    missing_records = database.filter(
        pl.any(pl.col('*').is_null()) | 
        pl.any(pl.col('sID', 'fID').str.strip() == '')
    )
    if len(missing_records):
        if verbose:
            print(f"Found {len(missing_records)} records with missing values.") 
        if not delete_missing:
            raise DataHandlingError(f"Please deal with these missing values or set argument delete_missing to 'record' or 'subject'.")
        elif delete_missing == 'record':
            if verbose:
                print("Deleting missing records...")
            return database.filter(pl.all(pl.col('*').is_not_null()))
        elif delete_missing == 'subject':
            if verbose:
                print("Deleting records of subjects with any missing records...")
            subjects = missing_records.select(pl.col('sID')).to_series()
            return database.filter(~pl.col('subject').is_in(subjects))
        else:
            raise DataHandlingError(f"Unknown delete_missing value: {delete_missing}. Acceptable values: 'record', 'subject'.")

    # no missing return as-is
    return database

def clean_erroneous_records(database:pl.DataFrame, delete_errors=False, verbose=True):
    """Checks for and potnetially deletes records which are erroneous
    
    Erroneous records are when the discharge date is recrded as before the admission date
    """

    erroneous_records = database.filter(
        pl.col('Adate') > pl.col('Ddate')
    )
    if len(erroneous_records):
        if verbose:
            print(f"Found {len(erroneous_records)} records with date errors.")
        if not delete_errors:
            raise DataHandlingError("Please deal with these errors or set argument delete_errors to 'record' or 'subject'.")
        elif delete_errors == 'record':
            if verbose:
                print("Deleting records with date errors...")
            return database.filter((pl.col('Adate') > pl.col('Ddate')).is_not())
        elif delete_errors == 'subject':
            if verbose:
                print("Deleting records of subjects with date errors...")
            subjects = erroneous_records.select(pl.col('sID')).to_series()
            return database.filter(~pl.col('subject').is_in(subjects))
    # no errors, return as-is
    return database

def fix_overlapping_stays(chunk:pl.DataFrame, verbose=True):
    """Fix overlapping stays for a single given subject"""

    # check for overlaps
    has_overlaps = any(chunk.select('Adate')[1:,:].to_series() < chunk.select('Ddate')[:-1,:].to_series())
    if has_overlaps and verbose:
        print(f"Individual {chunk.select('sID')[0].item()} has overlapping records. Fixing...")
    else:
        return chunk

    schema = chunk.columns[1:]

    # extract stay info
    data = zip(*(chunk.select(pl.exclude('sID')).sort('Adate')))

    # clean the overlaps
    raw_cleaned = ovfxr.clean_overlaps(data)

    # coerce back into the correct format17:47
    new_stays = pl.from_records(list(zip(*raw_cleaned)), schema=schema)
    compat_sid_col = pl.concat([chunk.select('sID'), new_stays], how='horizontal').fill_null(strategy='forward')

    return compat_sid_col