import datetime
import polars as pl

def parse_indv_transition(chunk: pl.DataFrame, window: float=1):

    _cols = ['sID', 'fID', 'Date']

    # silly hack to get an iterator from row 1 -> end
    next_rows = chunk.iter_rows()
    first_row = next(next_rows)

    # yield up the first row as the first event
    # yielded is (indv, location, start date)
    yield dict(zip(_cols, first_row[:-1]))

    for row, next_row in zip(chunk.iter_rows(), next_rows): 
        # searching for transfers within the time window
        # call these transitions at the admission date fo the next row
        if next_row[2] - row[-1] < datetime.timedelta(days=window):
            yield dict(zip(_cols, next_row[:-1]))
        else:
            yield dict(zip(_cols, (row[0], None, row[-1])))
            yield dict(zip(_cols, next_row[:-1]))

    yield dict(zip(_cols, [chunk[-1,'sID'], None, chunk[-1,'Ddate']]))



def parse_transitions(df: pl.DataFrame):

    chunks = df.partition_by('sID')
    transitions = pl.concat([pl.from_records(list(parse_indv_transition(chunk))) for chunk in chunks])
    return transitions
