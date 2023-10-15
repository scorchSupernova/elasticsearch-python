from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

es = Elasticsearch("http://localhost:9200")

def get_data():
    print(es.info().body)

def create_index():
    mappings = {
        "properties": {
            "title": {"type": "text", "analyzer": "english"},
            "ethnicity": {"type": "text", "analyzer": "standard"},
            "director": {"type": "text", "analyzer": "standard"},
            "cast": {"type": "text", "analyzer": "standard"},
            "genre": {"type": "text", "analyzer": "standard"},
            "plot": {"type": "text", "analyzer": "english"},
            "year": {"type": "integer"},
            "wiki_page": {"type": "keyword"}
        }
    }

    es.indices.create(index="movies", mappings=mappings)
    print(es.indices.exists(index="movies"))

def add_data_to_index_iterate_loop():
    import pandas as pd
    df = pd.read_csv("wiki_movie_plots_deduped.csv").dropna().sample(5000, random_state=42).reset_index()

    for i, row in df.iterrows():
        doc = {
            "title": row["Title"],
            "ethnicity": row["Origin/Ethnicity"],
            "director": row["Director"],
            "cast": row["Cast"],
            "genre": row["Genre"],
            "plot": row["Plot"],
            "year": row["Release Year"],
            "wiki_page": row["Wiki Page"]
        }

        es.index(index="movies1", document=doc, id=i)
        print(es.get(index="movies1", id=i))

def add_data_to_index_bulk():
    import pandas as pd
    df = pd.read_csv("wiki_movie_plots_deduped.csv").dropna().sample(5000, random_state=42).reset_index()

    bulk_data = []
    for i, row in df.iterrows():
        bulk_data.append({
            "_index": "movies",
            "_id": i,
            "_source": {
                "title": row["Title"],
                "ethnicity": row["Origin/Ethnicity"],
                "director": row["Director"],
                "cast": row["Cast"],
                "genre": row["Genre"],
                "plot": row["Plot"],
                "year": row["Release Year"],
                "wiki_page": row["Wiki Page"]
            }
        })

    bulk(es, bulk_data)
    print(es.cat.count(index="movies", format="json"))

def refresh_index():
    es.indices.refresh(index="movies1")


def search_using_indexes():
    query = {
          "bool": {
            "filter": [
              {
                "match": {
                  "director": "Roman Polanski"
                }
              }
            ],
            "must_not": {
              "match": {
                "cast": "Jack"
              }
            }
          }
    }

    print(query)
    res = es.search(index="movies", query=query)
    print(res.body)






if __name__ == "__main__":
    get_data()
    create_index()
    add_data_to_index_iterate_loop()
    add_data_to_index_bulk()
    refresh_index()
    search_using_indexes()