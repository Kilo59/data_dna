import pathlib
from pprint import pprint as pp

import Levenshtein as lev
import invoke
import pandas as pd

DATA_DIR = pathlib.Path("data")

TOP_1K_PATH = DATA_DIR / "311_Service_Requests_from_2010_to_Present_top10000.csv"


def lev_pair_wise_comparison(my_list: list[str], score_cutoff=None) -> dict:
    results = {}

    for i in my_list:
        res = {}
        idx = my_list.index(i)
        for j in range(len(my_list)):
            if idx == j:
                continue
            res[my_list[j]] = lev.distance(i, my_list[j], score_cutoff=score_cutoff)
        results[i] = res

    return results


@invoke.task
def get_descriptors(ctx, limit=0):
    df = pd.read_csv(TOP_1K_PATH, dtype=str)
    desc_set = set(df["Descriptor"])
    desc_list = [str(x) for x in desc_set]
    desc_list = sorted(desc_list)
    if limit:
        pp(desc_list[:limit])
    else:
        pp(desc_list)
    return desc_list


@invoke.task(aliases=["leven"])
def levenshtein(ctx, distance=2):
    pp(lev_pair_wise_comparison(["foo", "bar", "fizz", "buzz", "bizz"]))
