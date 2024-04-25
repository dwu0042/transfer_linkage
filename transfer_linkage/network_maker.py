import polars as pl
import numpy as np
from scipy import sparse


def make_adj(database: pl.DataFrame):
    facilities = database.select("fID").to_series().unique().to_list()
    facilities_map = {fac: i for i, fac in enumerate(facilities)}
    adj_mat = sparse.dok_matrix((len(facilities), len(facilities)))

    for chunk in database.partition_by("sID"):
        from_nd = None
        for sID, fID, Adate, Ddate, *aux in chunk.iter_rows():
            if from_nd is not None:
                adj_mat[facilities_map[from_nd], facilities_map[fID]] += 1
            from_nd = fID

    return adj_mat, facilities_map


def write_adj(A, fac, path):
    A_ = np.array(A.todense().astype(int))
    with open(path, "w") as fp:
        fp.write(";")
        fp.write(";".join(fac.keys()))
        fp.write("\n")
        for row, name in zip(A_, fac):
            fp.write(";".join([name, *map(str, row.tolist())]))
            fp.write("\n")
