import os
from dotenv import load_dotenv
import datetime
import mediacloud.api
import pandas as pd

# load .env file
load_dotenv()
# create a mediacloud api instance
mc = mediacloud.api.MediaCloud(os.getenv('KEY'))

def make_df(results_dict: dict):
    return pd.DataFrame(results_dict["counts"])
    
def get_counts(
    query:str = '"fake news" OR "misinformation" OR "disinformation"',
    date_from: datetime.date = datetime.date(2019, 1, 1),
    date_to: datetime.date = datetime.date(2021, 10, 17)):
    ### make the main query
    ## prep date clause
    date_range_ = mc.dates_as_query_clause(date_from, date_to) # default is start & end inclusive
    # make the query and split the results by month
    main_query = mc.storyCount(query, date_range_, split=True, split_period='month')
    main_df = make_df(main_query)
    ### Normalize
    ## fetch total stories
    total_stories = mc.storyCount("*", date_range_, split=True, split_period='month')
    total_stories_df = make_df(total_stories)
    # add the ratio var
    main_df["total_stories"] = total_stories_df["count"]
    main_df["ratio"] = main_df["count"] / main_df["total_stories"]
    # add the query
    main_df["query"] = query
    return main_df

if __name__ == "__main__":
    query_dicts = [
        {
            "q":'("fake news" OR "misinformation" OR "disinformation") AND ("social media" OR "social networks" OR Facebook OR Instagram OR Twitter OR Tiktok)',
            "q_name": 'social_media'
        },
        {
            "q":'("fake news" OR "misinformation" OR "disinformation") AND ("search engine" OR Google OR Bing OR Yandex OR Yahoo OR Duckduckgo OR Youtube)',
            "q_name": 'search_engines'
        },
        {
            "q":'("fake news" OR "misinformation" OR "disinformation") AND (ads OR advertising OR "micro-targeting" OR "microtargeting" OR "micro-targeting" OR "sponsored content")',
            "q_name": 'ads'
        },
        {
            "q":'("fake news" OR "misinformation" OR "disinformation") AND (privacy OR cookies OR tracking OR "user data")',
            "q_name": 'privacy'
        }
    ]
    ## fetch the data
    for query_dict in query_dicts:
        ## api call
        df = get_counts(
            query=query_dict["q"],
            date_from=datetime.date(2013,1,1)
        )
        # add the query name
        df["query_name"] = query_dict["q_name"]
        # prep the filename
        filename = os.path.join("data", "mediacloud", f"{query_dict['q_name']}.csv")
        # export
        df.to_csv(filename)
    ## all in one
    df_list = []
    for p in os.listdir("data/mediacloud"):
        df_list.append(pd.read_csv(os.path.join("data", "mediacloud", p)))
    final_df = pd.concat(df_list)
    final_df.to_csv("data/mediacloud_data.csv")
