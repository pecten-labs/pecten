import sys
import json
from datetime import datetime
import pandas as pd

def main(args):

    with open(args.file1, "r") as f1, open(args.file2, "a") as f2:
        for line in f1.readlines():
            data = json.loads(line)
            data["news_topics"] = str(data["news_topics"])
            f2.write(json.dumps(data, cls=MongoEncoder) + '\n')

def file_to_mongodb(args):
    operations = []
    storage = Storage(mongo_connection_string="mongodb://igenie_readwrite:igenie@35.189.89.82:27017/dax_gcp")
    with open(args.file1, "r") as f1:
        records = 0
        for line in f1:
            data = json.loads(line)
            #change column names
            try:
                data["NEWS_TITLE_NewsDim"] = data.pop("news_title")
                data["NEWS_DATE_NewsDim"] = datetime.strptime(data.pop("news_date"), '%Y-%m-%d %H:%M:%S')
                data["NEWS_ARTICLE_TXT_NewsDim"] = data.pop("news_article_txt")
                data["NEWS_SOURCE_NewsDim"] = data.pop("news_source")
                data["NEWS_PUBLICATION_NewsDim"] = data.pop("news_publication")
                data["categorised_tag"] = data.pop("news_topics")
                if data["constituent"] == "BMW":
                    data["constituent"] = "bmw"
            except Exception as e:
                print(e)
                continue

            operations.append(data)
            records += 1

            if len(operations) == 1000:
                print("Saving {} records".format(records))
                storage.save_to_mongodb(operations, "dax_gcp", "all_news")
                operations = []

        if len(operations) > 0:
            storage.save_to_mongodb(operations, "dax_gcp", "all_news")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('file1', help='The path of the Google key')
    parser.add_argument('file2', help='The connection string')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage, MongoEncoder
    #main(args)
    file_to_mongodb(args)