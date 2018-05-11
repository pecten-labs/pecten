import sys
import requests
import urllib.parse
from datetime import datetime
import json
import time

def main(args):
    from utils.Storage import Storage
    #Get tweets
    query = """
    SELECT *
    FROM pecten_dataset.tweets_backup
    WHERE lang = 'de' and id NOT IN(
        SELECT distinct(id)
        FROM pecten_dataset.tweets
        WHERE text_original_language IS NOT NULL)
    """

    storage_client = Storage(args.google_key_path)
    dataset = 'pecten_dataset'

    if args.environment != 'production':
        dataset += "_" + args.environment

    it = storage_client.get_bigquery_data(query,iterator_flag=True)
    to_insert = []

    total = 0

    for tweet in it:
        data = dict((k,tweet[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(tweet[k],datetime) else
                   (k,tweet[k]) for k in list(tweet._xxx_field_to_index.keys()))

        response = None

        try:
            url = "http://{}:5674/translate".format(args.ip_address)
            payload = {'q': tweet['text']}
            response = requests.get(url, params=payload,timeout=11)
            response = json.loads(response.text)
        except Exception as e:
            print(e)
            continue

        translated_text = None
        if response:
            try:
                translated_text = response['data']['translations'][0]['translatedText']
            except Exception as e:
                print(e)
                continue

        if translated_text:
            data['text_original_language'] = data['text']
            data['text'] = translated_text

        to_insert.append(data)

        if len(to_insert) == 500:
            print("Inserting to BQ production")
            try:
                result = storage_client.insert_bigquery_data("pecten_dataset", "tweets", to_insert)
                if result:
                    print("Data inserted")
                else:
                    print("Data not inserted")
            except Exception as e:
                print(e)

            print("Inserting to BQ test")
            try:
                result = storage_client.insert_bigquery_data("pecten_dataset_test", "tweets", to_insert)
                if result:
                    print("Data inserted")
                else:
                    print("Data not inserted")
            except Exception as e:
                print(e)

            to_insert = []

def write_to_file(args):
    from utils.Storage import Storage,MongoEncoder
    # Get tweets
    query = """
        SELECT *
        FROM pecten_dataset.tweets
        WHERE lang = 'de'
        """

    storage_client = Storage(args.google_key_path)
    dataset = 'pecten_dataset'
    if args.environment != 'production':
        dataset += "_" + args.environment

    it = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []

    total = 0

    ids = set()

    with open("tr_tweets.json", "r") as fo:
        for line in fo:
            ids.add(json.loads(line)['id'])

    with open("tr_tweets.json", "a") as f:
        for tweet in it:
            if tweet.id in ids:
                continue

            start_time = time.time()
            data = dict((k, tweet[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(tweet[k], datetime) else
                        (k, tweet[k]) for k in list(tweet._xxx_field_to_index.keys()))

            response = None

            try:
                url = "http://{}:5674/translate".format(args.ip_address)
                payload = {'q': tweet['text']}
                print("Making request")
                response = requests.get(url, params=payload,timeout=11)
                response = json.loads(response.text)
            except Exception as e:
                print(e)
                continue

            translated_text = None
            if response:
                try:
                    translated_text = response['data']['translations'][0]['translatedText']
                except Exception as e:
                    print(e)
                    continue

            if translated_text:
                data['text_original_language'] = data['text']
                data['text'] = translated_text

            f.write(json.dumps(data, cls=MongoEncoder) + '\n')
            print("--- {} seconds ---".format(time.time() - start_time))
            total += 1

            if total % 1000 == 0:
                print("Saved {} records".format(total))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The connection string')
    parser.add_argument('environment', help='production or test')
    parser.add_argument('ip_address')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    main(args)

