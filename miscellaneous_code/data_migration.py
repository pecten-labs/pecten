import sys
from pymongo import MongoClient
import os
import json
from pprint import pprint
from copy import deepcopy

def other_tables(args):
    param_table = "PARAM_TWITTER_COLLECTION"
    parameters_list = ["CONNECTION_STRING"]

    parameters = get_parameters(args.param_connection_string, param_table, parameters_list)

    storage = Storage(google_key_path=args.google_key_path, mongo_connection_string=parameters["CONNECTION_STRING"])

    mongo_connection_string = parameters["CONNECTION_STRING"]


    client = MongoClient(mongo_connection_string)
    db = client["dax_gcp"]

    for collection_name in args.mongo_collections.split(","):
        collection = db[collection_name]
        cursor = collection.find({},{"_id":0})
        data = list(cursor)
        file_name = "{}.json".format(collection_name)

        open(file_name, 'w').write("\n".join(json.dumps(e, cls=MongoEncoder) for e in data))

        cloud_file_name = "{}/{}".format(args.bucket,file_name)

        if os.path.isfile(file_name):
            if storage.upload_to_cloud_storage(args.google_key_path, args.bucket, file_name, file_name):
                print("File uploaded to Cloud Storage")
                os.remove(file_name)
            else:
                print("File not uploaded to Cloud storage.")
        else:
            print("File does not exists in the local filesystem.")

def tweet_table(args):
    param_table = "PARAM_TWITTER_COLLECTION"
    parameters_list = ["CONNECTION_STRING"]

    parameters = tap.get_parameters(args.param_connection_string, param_table, parameters_list)

    mongo_connection_string = parameters["CONNECTION_STRING"]
    client = MongoClient(mongo_connection_string)
    db = client["dax_gcp"]
    collection = db["tweets"]
    file_name = "{}.json".format("tweets-raw")
    file_name_unmodified = "{}.json".format("tweets-unmodified")
    fields_to_keep = ["text", "favorite_count", "source", "retweeted", "entities", "id_str",
                      "retweet_count", "favorited", "user", "lang", "created_at", "place", "constituent_name",
                      "constituent_id", "search_term", "id", "sentiment_score", "entity_tags", "relevance"]


    cursor = collection.find({}, no_cursor_timeout=True)


    print("Writing local file")
    with open(file_name, "w") as f, open(file_name_unmodified, "w") as f2:
        count = 0
        for tweet in cursor:
            # Removing bad fields
            clean_tweet = tap.scrub(tweet)
            clean_tweet["constituent"]

            # Separate the tweets that go to one topic or the other

            # unmodified
            t_unmodified = deepcopy(clean_tweet)
            t_unmodified["date"] = tap.convert_timestamp(t_unmodified["created_at"])
            f2.write(json.dumps(t_unmodified, cls=MongoEncoder) + '\n')

            # modified
            tagged_tweet = dict((k, clean_tweet[k]) for k in fields_to_keep if k in clean_tweet)
            tagged_tweet['date'] = tap.convert_timestamp(clean_tweet["created_at"])
            f.write(json.dumps(tagged_tweet, cls=MongoEncoder) + '\n')
            count += 1

            if count == 10000:
                print("Saved {} tweets".format(count))
                count = 0

    return
    bucket_name = "igenie-tweets"
    cloud_file_name = "historical/{}".format(file_name)

    print("Writing to cloud storage")
    if os.path.isfile(file_name):
        if storage.upload_to_cloud_storage(args.google_key_path, bucket_name, file_name, cloud_file_name):
            print("File uploaded to Cloud Storage")
            #os.remove(file_name)
        else:
            print("File not uploaded to Cloud storage.")
    else:
        print("File does not exists in the local filesystem.")

def get_parameters(connection_string, table, column_list):
    storage = Storage()

    data = storage.get_sql_data(connection_string, table, column_list)[0]
    parameters = {}

    for i in range(0, len(column_list)):
        parameters[column_list[i]] = data[i]

    return parameters

def update_tweets(args):
    #"tweets_000000000000.json","tweets_000000000001.json",
    files = ["tweets_000000000002.json",
             "tweets_000000000003.json","tweets_000000000004.json","tweets_000000000005.json",
             "tweets_000000000006.json","tweets_000000000007.json","tweets_000000000008.json",
             "tweets_000000000009.json","tweets_000000000010.json","tweets_000000000011.json",
             "tweets_000000000012.json","tweets_000000000013.json","tweets_000000000014.json",
             "tweets_000000000015.json","tweets_000000000016.json","tweets_000000000017.json",
             "tweets_000000000018.json","tweets_000000000019.json","tweets_000000000020.json"]
    out = "tweets_fixed.json"

    for file in files:
        print("Processing file: {}".format(file))
        with open(file, 'r') as f1, open(out, "a") as f2:
            records = 0
            total = 0
            for line in f1:
                tweet = json.loads(line)
                if 'sentiment_score' in tweet:
                    if tweet['sentiment_score'] is None:
                        tweet['sentiment_score'] = tap.get_nltk_sentiment(str(tweet['text']))
                        if tweet['sentiment_score'] is None:
                            print("{} had null sentiment_score".format(tweet['text']))
                else:
                    tweet['sentiment_score'] = tap.get_nltk_sentiment(str(tweet['text']))

                f2.write(json.dumps(tweet, cls=MongoEncoder) + '\n')
                records += 1
                total += 1
                if records == 2000:
                    print("Wrote {} records".format(total))
                    records = 0

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The connection string')
    parser.add_argument('function')
    parser.add_argument('mongo_collections', help='Comma separated list of collection names')
    parser.add_argument('bucket')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage, MongoEncoder
    from utils import twitter_analytics_helpers as tap
    if args.function == "other_tables":
        other_tables(args)
    elif args.function == "tweet_table":
        tweet_table(args)
    elif args.function == "update_tweets":
        update_tweets(args)


