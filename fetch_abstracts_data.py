import arxiv
import pandas as pd
import os
import re

def make_arxiv_query(q: str, filename: str, max_results: int) -> None:
    client = arxiv.Client(num_retries=10, delay_seconds=10, page_size=500)
    my_query = arxiv.Search(
        query = q,
        max_results=max_results,
        sort_by = arxiv.SortCriterion.SubmittedDate,
        sort_order = arxiv.SortOrder.Descending)
    results_as_dicts = [vars(r) for r in client.results(my_query)]
    if results_as_dicts:
        df = pd.DataFrame(data = results_as_dicts)
        df["query"] = q
        # export 
        df.to_csv(filename)

if __name__ == "__main__":
    if not os.path.isdir("data"):
        os.mkdir("data")
    if not os.path.isdir("data/arxiv"):
        os.mkdir("data/arxiv")
    # # make the queries
    kws = ['abs:"fake news"', 'abs:misinformation', 'abs:disinformation']
    for kw in kws:
        # prep the filename
        punct_removed = re.sub('"|abs\:|\(||\)||\|"', "", kw)
        filename = "./data/arxiv/" + re.sub("\s+", "_", punct_removed) + ".csv"
        if not os.path.isfile(filename):
            make_arxiv_query(q = kw, filename=filename, max_results=float("Inf"))
    # all in one dataset
    df_list = []
    for p in os.listdir("data/arxiv"):
        df_list.append(pd.read_csv(os.path.join("data", "arxiv", p)))
    final_df = pd.concat(df_list).drop_duplicates("entry_id")
    final_df.to_csv("data/arxiv_data.csv")






