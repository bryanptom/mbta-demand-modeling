'''
These functions process scraped tweets from the MBTA twitter feed. The tweets are stored in a directory with the following structure:

- MBTA_Twitter:
    - MBTA_tweets:
        tweet_id_1.json
        tweet_id_2.json
        ...
    - MBTA_images:
        image_id_1.jpg
        image_id_2.jpg
        ...

Where tweet_id is the unique identifier for each tweet. That is, you can always find the tweet by going to twitter.com/MBTA/status/tweet_id. Image ids are for 
the twing address for attached images. You can always find an image by going to pbs.twimg.com/media/<image_id>?format=png&name=orig

In their original format, it's hard to work with the tweets.
'''

import json
import os
from copy import deepcopy
import pandas as pd
from tqdm import tqdm

def make_image_url_from_id(image_id: str) -> str:
    '''
    This function takes an image id and returns the full url for the image. The url is in the format:

        https://pbs.twimg.com/media/<image_id>?format=jpg&name=orig

    params:
    - image_id: The id of the image
    returns:
    - url: The full url for the image
    '''
    return f'https://pbs.twimg.com/media/{image_id}?format=jpg&name=orig'

def image_or_video_id_from_url(url: str) -> str:
    '''
    This function takes a url and returns the image or video id. The url is in the format:

        https://pbs.twimg.com/media/<image_id>?format=jpg&name=orig

    for images or

        https://video.twimg.com/amplify_video/XXXXXXXX/vid/1280x720/<video_id>.mp4?tag=13

    for videos. This function returns the id from images and the full url from videos, since those are not dowmloaded. 

    params:
    - url: The url to extract the id from
    returns:
    - id/url: The id of the image or url from the video
    '''
    try:
        if url.startswith('https://pbs.twimg.com/media/'):
            return url.split('/')[-1].split('?')[0]
        elif url.startswith('https://video.twimg.com/amplify_video/') or url.startswith('https://video.twimg.com/ext_tw_video/'):
            return url
        else:
            print(f'Invalid url {url}')
            raise ValueError('Invalid url')
    
    except Exception as e:
        print(f'Error processing url {url}')
        raise ValueError('Invalid url')

EMPTY_MEDIA_DICT = {'image_ids': [], 'video_urls': []}

def tweet_jsons_to_csv(tweets_dir: str | os.PathLike, output_file: str | os.PathLike, media_output_json: str | os.PathLike = None):
    '''
    This function takes the tweets from the tweets directory and writes them to a csv file. An example tweet json is:

    {
        "address": "https://twitter.com/MBTA/status/1717602383570620896",
        "dataMode": "json",
        "hotlinkProtected": false,
        "connectionTimeOut": -1,
        "otherPropertiesMap": {
            "CustomFolderName": "MBTA",
            "CustomFileName": "1717602383570620896",
            "FileExtensionHandleMethod": "workoutExtension",
            "created_at": "Thu Oct 26 18:01:34 +0000 2023",
            "status_id": "1717602383570620896",
            "full_url": "https://twitter.com/MBTA/status/1717602383570620896",
            "tweet_text": "The MBTA Board of Directors Subcommittees will hold virtual or in-...",
            "owner_display_name": "MBTA",
            "favorite_count": "4",
            "quote_count": "1",
            "retweet_count": "2",
            "reply_count": "1",
            "is_retweet": "false",
            "is_quoted_tweet": "false",
            "is_reply_tweet": "true",
            "target_tweet_id": "1717602383570620871",
            "target_tweet_url": "https://twitter.com/MBTA/status/1717602383570620871",
            "has_media": "true",
            "media_urls": "https://pbs.twimg.com/media/F9YmRAnWsAA4xpI?format=jpg&name=orig",
            "media_details": [
                {
                    "type": "image",
                    "url": "https://pbs.twimg.com/media/F9YmRAnWsAA4xpI?format=jpg&name=orig"
                }
            ]
        }
    }

    The csv file will have the following columns:
    - tweet_id
    - created_at
    - tweet_text
    - tweet_url
    - is_quoted_tweet
    - is_reply_tweet
    - target_tweet_id
    - target_tweet_url
    - has_media
    - media_1_type
    - media_1_url
    - media_2_type
    - media_2_url
    ... (up to n media items, n is max in the dataset)

    params:
    - tweets_dir: The directory containing the tweet jsons
    - output_file: The file to write the tweets to, must be a csv file

    '''

    # Get all the tweet jsons
    tweet_jsons = os.listdir(tweets_dir)

    # Create a list to store the tweets
    tweets = []
    tweet_media_mapping = {}
    error_count = 0
    for tweet_json in tqdm(tweet_jsons):
        with open(os.path.join(tweets_dir, tweet_json), encoding = 'utf-8') as f:
            tweet = json.load(f)

        # Create a new dictionary with the same tweet data, adhering to the csv format
        try:
            tweet_data = {
                "tweet_id": tweet["otherPropertiesMap"]["status_id"],
                "created_at": tweet["otherPropertiesMap"]["created_at"],
                "tweet_text": tweet["otherPropertiesMap"]["tweet_text"].replace('\n', ' '),
                "tweet_url": tweet["otherPropertiesMap"]["full_url"],
                "is_quoted_tweet": tweet["otherPropertiesMap"]["is_quoted_tweet"],
                "is_reply_tweet": tweet["otherPropertiesMap"]["is_reply_tweet"],
                "has_media": tweet["otherPropertiesMap"]["has_media"]
            }

            if tweet_data['is_quoted_tweet'] == 'true' or tweet_data['is_reply_tweet'] == 'true':
                tweet_data['target_tweet_id'] = tweet["otherPropertiesMap"]["target_tweet_id"]
                tweet_data['target_tweet_url'] = tweet["otherPropertiesMap"]["target_tweet_url"]


            if tweet["otherPropertiesMap"]["has_media"] == "true":
                tweet_id = int(tweet["otherPropertiesMap"]["status_id"])
                tweet_media_mapping[tweet_id] = deepcopy(EMPTY_MEDIA_DICT)
                for media in tweet["otherPropertiesMap"]["media_details"]:
                    if media["type"] == 'image':
                        tweet_media_mapping[tweet_id]['image_ids'].append(image_or_video_id_from_url(media["url"]))
                    elif media["type"] == 'video':
                        tweet_media_mapping[tweet_id]['video_urls'].append(image_or_video_id_from_url(media["url"]))

        except KeyError as e:
            print(f"Error processing tweet {tweet['otherPropertiesMap']['status_id']}")
            print(f'Missing key {e.args[0]}')
            error_count += 1

            if error_count > 10:
                print('Too many errors processing tweets from jsons, stopping')
                break
            
            continue

        tweets.append(tweet_data)

    # Create a dataframe with the tweets
    df = pd.DataFrame(tweets)

    # Configure appropriate settings for the dataframe
    df['tweet_id'] = df['tweet_id'].astype('Int64')
    df.set_index("tweet_id", inplace = True)
    df["created_at"] = pd.to_datetime(df["created_at"], format = "%a %b %d %H:%M:%S %z %Y")
    df['is_quoted_tweet'] = df['is_quoted_tweet'].map({'true': True, 'false': False})
    df['is_reply_tweet'] = df['is_reply_tweet'].map({'true': True, 'false': False})
    df['has_media'] = df['has_media'].map({'true': True, 'false': False})

    df['target_tweet_id'] = df['target_tweet_id'].replace('null', None)
    df['target_tweet_id'] = df['target_tweet_id'].astype('Int64')
    df['target_tweet_url'] = df['target_tweet_url'].replace('null', None)

    # Write the dataframe to a csv file
    df.to_csv(output_file)

    # Optionally, also write the media mapping to a json file
    if media_output_json:
        with open(media_output_json, 'w') as f:
            json.dump(tweet_media_mapping, f, indent=4)

def validate_image_scraping(media_json_file: str | os.PathLike , image_folder: str | os.PathLike, missing_urls_file: str | os.PathLike = None):
    # Load in the tweets csv to pandas
    with open(media_json_file, 'r') as f:
        tweet_media_mapping = json.load(f)

    # Get all image files as a list
    image_files = [image.split('.')[0] for image in os.listdir(image_folder)]

    # Check if all image ids are in the image files
    all_images, missing_urls, n_missing = [], [], 0
    for tweet_id in tweet_media_mapping:
        for image_id in tweet_media_mapping[tweet_id]['image_ids']:
            all_images.append(image_id)
            if image_id not in image_files:
                missing_urls.append(make_image_url_from_id(image_id))
                n_missing += 1

    if missing_urls_file:
        with open(missing_urls_file, 'w') as f:
            f.write('\n'.join(missing_urls))
    print(f'{n_missing} images are missing')

    print('Total scraped images: ', len(set(image_files)))
    print('Total referenced images', len(set(all_images)))

def check_missing_days(tweets_csv: str | os.PathLike):
    '''
    This function checks if there are any missing days in the tweets. It does this by loading the tweets csv into a pandas dataframe and then checking the 
    difference between the days. If there are any missing days, it prints them out. 

    params:
    - tweets_csv: The csv file containing the tweets
    '''
    # Load in the tweets csv to pandas
    df = pd.read_csv(tweets_csv, index_col = 0, parse_dates = ['created_at'])

    # Get the days in the tweets
    days = df['created_at'].dt.date.unique()

    min_day, max_day = min(days), max(days)
    print(f'Tweets from {min_day} to {max_day}')

    # Check for missing days
    missing_days = []
    for i in range((max_day - min_day).days):
        day = min_day + pd.Timedelta(days = i)
        if day not in days:
            missing_days.append(day)

    if len(missing_days) > 0:
        print(f'{len(missing_days)} missing days:')
        for day in missing_days:
            print(day)
    else:
        print('No missing days')

if __name__ == '__main__':
    # Process Tweets
    # tweets_dir = r'C:\Users\user\Documents\MBTA_Twitter\MBTA_tweets'
    # output_file = r'C:\Users\user\Documents\MBTA_Twitter\mbta_tweets.csv'
    # media_map_output = r'C:\Users\user\Documents\MBTA_Twitter\tweet_media_lookup.json'
    # tweet_jsons_to_csv(tweets_dir, output_file, media_map_output)
    # print('Done!')

    # Validate that all images are scraped
    # media_map_output = r'C:\Users\user\Documents\MBTA_Twitter\tweet_media_lookup.json'
    # image_folder = r'C:\Users\user\Documents\MBTA_Twitter\MBTA_images'
    # missing_urls_file = r'C:\Users\user\Documents\MBTA_Twitter\missing_image_urls.txt'
    # validate_image_scraping(media_map_output, image_folder, missing_urls_file=missing_urls_file)

    # Check for missing days
    tweets_csv = r'C:\Users\user\Documents\MBTA_Twitter\mbta_tweets.csv'
    check_missing_days(tweets_csv)