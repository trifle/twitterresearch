#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2016 Pascal Jürgens and Andreas Jungherr
# See License.txt

"""
Examples for accessing the API
------------------------------
These are some examples demonstrating the use of provided functions for
gathering data from the Twitter API.

Requirements:
    - depends on API access modules rest.py and streaming.py
"""

import rest
import streaming
import database
import logging
import json
import datetime
from pytz import timezone
import peewee
from progress.bar import Bar

MST = timezone("MST")

#
# Setup
#


def hydrate(idlist_file="data/example_dataset_tweet_ids.txt"):
    """
    This function reads a file with tweet IDs and then loads them
    through the API into the database. Prepare to wait quite a bit,
    depending on the size of the dataset.
    """
    ids_to_fetch = set()
    for line in open(idlist_file, "r"):
        # Remove newline character through .strip()
        # Convert to int since that's what the database uses
        ids_to_fetch.add(int(line.strip()))
    # Find a list of Tweets that we already have
    ids_in_db = set(t.id for t in database.Tweet.select(database.Tweet.id))
    # Sets have an efficient .difference() method that returns IDs only present
    # in the first set, but not in the second.
    ids_to_fetch = ids_to_fetch.difference(ids_in_db)
    logging.warning(
        "\nLoaded a list of {0} tweet IDs to hydrate".format(len(ids_to_fetch)))

    # Set up a progressbar
    bar = Bar('Fetching tweets', max=len(ids_to_fetch), suffix='%(eta)ds')
    for page in rest.fetch_tweet_list(ids_to_fetch):
        bar.next(len(page))
        for tweet in page:
            database.create_tweet_from_dict(tweet)
    bar.finish()
    logging.warning("Done hydrating!")


def dehydrate(filename="data/dehydrated_tweet_ids.txt"):
    """
    This function writes the Tweet IDs contained in the current database to
    a file that allows re-hydration with the above method.
    """
    with open(filename, "w") as f:
        for tweet in database.Tweet.select(database.Tweet.id):
            f.write("{0}\n".format(tweet.id))


#
# Helper Functions
#


def print_tweet(tweet):
    """
    Print a tweet as one line:
    user: tweet
    """
    logging.warning(
        u"{0}: {1}".format(tweet["user"]["screen_name"], tweet["text"]))


def print_notice(notice):
    """
    This just prints the raw response, such as:
    {u'track': 1, u'timestamp_ms': u'1446089368786'}}
    """
    logging.error(u"{0}".format(notice))

#
# Examples
#


def import_json(fi):
    """
    Load json data from a file into the database.
    """
    logging.warning("Loading tweets from json file {0}".format(fi))
    for line in open(fi, "rb"):
        data = json.loads(line.decode('utf-8'))
        database.create_tweet_from_dict(data)


def print_user_archive():
    """
    Fetch all available tweets for one user and print them, line by line
    """
    archive_generator = rest.fetch_user_archive("lessig")
    for page in archive_generator:
        for tweet in page:
            print_tweet(tweet)


def save_user_archive_to_file():
    """
    Fetch all available tweets for one user and save them to a text file, one tweet per line.
    (This is approximately the format that GNIP uses)
    """
    with open("lessig-tweets.json", "w") as f:
        archive_generator = rest.fetch_user_archive("lessig")
        for page in archive_generator:
            for tweet in page:
                f.write(json.dumps(tweet) + "\n")
    logging.warning(u"Wrote tweets from @lessig to file lessig-tweets.json")


def save_user_archive_to_database():
    """
    Fetch all available tweets for one user and save them to the database.
    """
    archive_generator = rest.fetch_user_archive("lessig")
    for page in archive_generator:
        for tweet in page:
            database.create_tweet_from_dict(tweet)
    logging.warning(u"Wrote tweets from @lessig to database")


def print_list_of_tweets():
    """
    Fetch a list of three tweets by ID, then print them line by line
    This example can be easily adapted to write the tweets to a file, see above.
    """
    list_generator = rest.fetch_tweet_list(
        [62154131600224256, 662025716746354688, 661931648171302912, ])
    for page in list_generator:
        for tweet in page:
            print_tweet(tweet)


def track_keywords():
    """
    Track two keywords with a tracking stream and print machting tweets and notices.
    To stop the stream, press ctrl-c or kill the python process.
    """
    keywords = ["politics", "election"]
    stream = streaming.stream(
        on_tweet=print_tweet, on_notification=print_notice, track=keywords)


def save_track_keywords():
    """
    Track two keywords with a tracking stream and save machting tweets.
    To stop the stream, press ctrl-c or kill the python process.
    """
    # Set up file to write to
    outfile = open("keywords_example.json", "w")

    def save_tweet(tweet):
        json.dump(tweet, outfile)
        # Insert a newline after one tweet
        outfile.write("\n")
    keywords = ["politics", "election"]
    stream = streaming.stream(
        on_tweet=save_tweet, on_notification=print_notice, track=keywords)


def follow_users():
    """
    Follow several users, printing their tweets (and retweets) as they arrive.
    To stop the stream, press ctrl-c or kill the python process.
    """
    # user IDs are: nytimes: 807095, washingtonpost: 2467791
    # they can be obtained through:
    # users = ["nytimes", "washingtonpost"]
    # users_json = rest.fetch_user_list_by_screen_name(screen_names=users)
    # for u in users_json:
    #   print("{0}: {1}".format(u["screen_name"], u["id"]))
    users = ["807095", "2467791"]
    stream = streaming.stream(
        on_tweet=print_tweet, on_notification=print_notice, follow=users)


def save_follow_users():
    """
    Follow several users, saving their tweets (and retweets) as they arrive.
    To stop the stream, press ctrl-c or kill the python process.
    """
    outfile = open("user_example.json", "w")

    def save_tweet(tweet):
        json.dump(tweet, outfile)
        # Insert a newline after one tweet
        outfile.write("\n")
    users = ["807095", "2467791"]
    stream = streaming.stream(
        on_tweet=save_tweet, on_notification=print_notice, follow=users)


def export_hashtag_counts(interval="day", hashtags=["Bush", "Carson", "Christie", "Cruz", "Fiorina", "Huckabee", "Kasich", "Paul", "Rubio", "Trump"]):
    """
    Create daily counts for given Hashtags. A bit slow. An easy speedup is to convert the list of hashtags to Hashtag database objects and query for them.
    """
    # Create output file
    with open("hashtag_counts.csv", "w") as f:
        # Write header line
        f.write("{0},".format(interval))
        f.write(",".join(hashtags))
        f.write(",\n")
        # Prepare interator over intervals
        # htm is an intermediary model for many-to-many-relationships
        # In this case Tweet -> htm -> Hashtag
        htm = database.Tweet.tags.get_through_model()
        intervals = database.objects_by_interval(
            database.Tweet, interval=interval, start_date=None, stop_date=None)
        for (interval_start, interval_stop), query in intervals:
            # Convert the timestamp to Mountain Standard Time which is
            # the local timezone for the example data
            timestamp = MST.normalize(interval_start).strftime(
                "%Y-%m-%d %H:%M:%S %z")
            f.write("{0},".format(timestamp))
            for tag in hashtags:
                # Match ignoring case
                count = query.join(htm).join(database.Hashtag).where(
                    peewee.fn.Lower(database.Hashtag.tag) == tag.lower()).count()
                f.write("{0},".format(count))
            f.write("\n")


def export_mention_counts(interval="day", usernames=["jebbush", "realbencarson", "chrischristie", "tedcruz", "carlyfiorina", "govmikehuckabee", "johnkasich", "randpaul", "marcorubio", "realdonaldtrump"]):
    """
    Create daily counts for mentions of given Users.
    """
    # Create output file
    with open("mention_counts.csv", "w") as f:
        # Write header line
        f.write("{0},".format(interval))
        f.write(",".join(usernames))
        f.write(",\n")
        # Prepare interator over intervals
        # htm is an intermediary model for many-to-many-relationships
        # In this case Tweet -> htm -> Hashtag
        mtm = database.Tweet.mentions.get_through_model()
        intervals = database.objects_by_interval(
            database.Tweet, interval=interval, start_date=None, stop_date=None)
        for (interval_start, interval_stop), query in intervals:
            # Convert the timestamp to Mountain Standard Time which is
            # the local timezone for the example data
            timestamp = MST.normalize(interval_start).strftime(
                "%Y-%m-%d %H:%M:%S %z")
            f.write("{0},".format(timestamp))
            for user in usernames:
                # Match ignoring case
                count = query.join(mtm).join(database.User).where(
                    peewee.fn.Lower(database.User.username) == user.lower()).count()
                f.write("{0},".format(count))
            f.write("\n")


def export_keyword_counts(interval="day", keywords=["Bush", "Carson", "Christie", "Cruz", "Fiorina", "Huckabee", "Kasich", "Paul", "Rubio", "Trump"]):
    """
    Create daily counts for given Keywords.
    """
    # Create output file
    with open("keyword_counts.csv", "w") as f:
        # Write header line
        f.write("{0},".format(interval))
        f.write(",".join(keywords))
        f.write(",\n")
        # Prepare interator over intervals
        intervals = database.objects_by_interval(
            database.Tweet, interval=interval, start_date=None, stop_date=None)
        for (interval_start, interval_stop), query in intervals:
            # Convert the timestamp to Mountain Standard Time which is
            # the local timezone for the example data
            timestamp = MST.normalize(interval_start).strftime(
                "%Y-%m-%d %H:%M:%S %z")
            f.write("{0},".format(timestamp))
            for word in keywords:
                # Match ignoring case
                kwcount = query.where(
                    peewee.fn.Lower(database.Tweet.text).contains(word.lower())).count()
                f.write("{0},".format(kwcount))
            f.write("\n")


def export_user_counts(interval="day", usernames=["JebBush", "RealBenCarson", "ChrisChristie", "tedcruz", "CarlyFiorina", "GovMikeHuckabee", "JohnKasich", "RandPaul", "marcorubio", "realDonaldTrump"]):
    """
    Create daily counts for given Users.
    """
    # Create output file
    with open("user_counts.csv", "w") as f:
        # Write header line
        f.write("{0},".format(interval))
        f.write(",".join(usernames))
        f.write(",\n")
        # Prepare interator over intervals
        intervals = database.objects_by_interval(
            database.Tweet, interval=interval, start_date=None, stop_date=None)
        for (interval_start, interval_stop), query in intervals:
            # Convert the timestamp to Mountain Standard Time which is
            # the local timezone for the example data
            timestamp = MST.normalize(interval_start).strftime(
                "%Y-%m-%d %H:%M:%S %z")
            f.write("{0},".format(timestamp))
            for username in usernames:
                # Match precise username
                ucount = query.join(database.User).where(
                    database.User.username == username).count()
                f.write("{0},".format(ucount))
            f.write("\n")


def export_total_counts(interval="day"):
    """
    Create hourly counts for Tweets
    """
    # Create output file
    with open("total_counts.csv", "w") as f:
        # Write header line
        f.write("{0},".format(interval))
        f.write("total,")
        f.write("\n")
        # Prepare interator over intervals
        intervals = database.objects_by_interval(
            database.Tweet, interval=interval, start_date=None, stop_date=None)
        for (interval_start, interval_stop), query in intervals:
            # Convert the timestamp to Mountain Standard Time which is
            # the local timezone for the example data
            timestamp = MST.normalize(interval_start).strftime(
                "%Y-%m-%d %H:%M:%S %z")
            f.write("{0},".format(timestamp))
            f.write("{0},".format(query.count()))
            f.write("\n")


def export_featureless_counts(interval="day"):
    """
    Create hourly counts for Tweets without mentions or URLs.
    Complex queries on many-to-many-relationships are very
    contrived with peewee. For the sake of simplicity, this
    function instead
    """
    # Create output file
    with open("featureless_counts.csv", "w") as f:
        # Write header line
        f.write("{0},".format(interval))
        f.write("featureless,")
        f.write("\n")
        # Prepare interator over intervals
        intervals = database.objects_by_interval(
            database.Tweet, interval=interval, start_date=None, stop_date=None)
        for (interval_start, interval_stop), query in intervals:
            # Convert the timestamp to Mountain Standard Time which is
            # the local timezone for the example data
            timestamp = MST.normalize(interval_start).strftime(
                "%Y-%m-%d %H:%M:%S %z")
            f.write("{0},".format(timestamp))
            featureless_count = 0
            for t in query:
                if bool(t.mentions.is_null() and t.urls.is_null() and t.reply_to_tweet is None):
                    featureless_count += 1
            f.write("{0},".format(featureless_count))
            f.write("\n")


def export_mention_totals(n=50):
    """
    Export the N most mentioned users and their respective counts to
    a CSV file.
    """
    start_date = MST.localize(datetime.datetime(2015, 10, 27, 0))
    stop_date = MST.localize(datetime.datetime(2015, 11, 2, 23, 59))
    with open("mention_totals.csv", "w") as f:
        f.write("user, mentions\n")
        for user in database.mention_counts(start_date, stop_date)[:50]:
            f.write("{0}, {1}\n".format(user.username, user.count))


def export_url_totals(n=50):
    """
    Export the N most mentioned URLs and their respective counts to
    a CSV file.
    """
    start_date = MST.localize(datetime.datetime(2015, 10, 27, 0))
    stop_date = MST.localize(datetime.datetime(2015, 11, 2, 23, 59))
    with open("url_totals.csv", "w") as f:
        f.write("url, mentions\n")
        for url in database.url_counts(start_date, stop_date)[:50]:
            f.write("{0}, {1}\n".format(url.url, url.count))


def export_hashtag_totals(n=50):
    """
    Export the N most mentioned hashtags and their respective counts to
    a CSV file.
    """
    start_date = MST.localize(datetime.datetime(2015, 10, 27, 0))
    stop_date = MST.localize(datetime.datetime(2015, 11, 2, 23, 59))
    with open("hashtag_totals.csv", "w") as f:
        f.write("hashtag, mentions\n")
        for hashtag in database.hashtag_counts(start_date, stop_date)[:50]:
            f.write("{0}, {1}\n".format(hashtag.tag, hashtag.count))


def export_retweet_totals(n=50):
    """
    Export the N most retweeted users and their respective counts to
    a CSV file.
    """
    start_date = MST.localize(datetime.datetime(2015, 10, 27, 0))
    stop_date = MST.localize(datetime.datetime(2015, 11, 2, 23, 59))
    with open("retweet_totals.csv", "w") as f:
        f.write("user, retweets\n")
        retweetcounts = database.retweet_counts(
            start_date, stop_date, 50).items()
        for username, count in retweetcounts:
            f.write("{0}, {1}\n".format(username, count))


def top_retweets(n=50):
    """
    Find the most retweeted tweets and display them.
    For readability's sake, this is not done through SQL
    """
    rt_counts = {}
    # all retweets
    retweets = database.Tweet.select(database.Tweet.retweet).where(database.Tweet.retweet.is_null(False)).group_by(database.Tweet.retweet)
    for tweet in retweets:
        rt_counts[tweet.retweet.id] = tweet.retweet.retweets.count()
    from collections import Counter
    c = Counter(rt_counts)

    from collections import OrderedDict
    results = OrderedDict()
    for k, v in c.most_common(n):
        results[database.Tweet.get(id=k).text] = v
    return results


def export_retweet_text(n=50):
    """
    Find the most retweeted tweets and export them to a CSV file
    """

    rt_counts = {}
    # all retweets
    retweets = database.Tweet.select(database.Tweet.retweet).where(database.Tweet.retweet.is_null(False)).group_by(database.Tweet.retweet)
    for tweet in retweets:
        rt_counts[tweet.retweet.id] = tweet.retweet.retweets.count()
    from collections import Counter
    c = Counter(rt_counts)

    with open("retweet_texts.csv", "w") as f:
        f.write("tweet text, count\n")
        for k, v in c.most_common(n):
            tweet_text = database.Tweet.get(id=k).text
            f.write("{0},{1}\n".format(tweet_text.replace("\n", "<newline>"), v))
