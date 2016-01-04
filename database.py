#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2016 Pascal Jürgens and Andreas Jungherr
# See License.txt

"""
Database Access
---------------
Defines the database structure and provides access

Requirements:
    - libraries peewee (database) and dateutil (parsing date strings)

Using SQL-based databases
=========================
This module implements a simple database that can be used to store structured twitter data. There are many other approaches (see below) that might be more appropriate for your use case. SQL-based databases impose strict restrictions on the data that can be stored. In particular, we need to define the structure of the data in advance by designing data *models*. These models guarantee that anything that is stored in the database conforms to our expectations. It is not possible to store incomplete (for example a Tweet missing its ID), duplicate (two Tweets with the same ID) or wrong data (such as writing the Tweet text into the ID field). By explicitly failing, the database makes errors visible that might have severe consequences for research but might otherwise go unnoticed.

The library used in this module (peewee) is one of many so-called "ORMs" (object relational mapper). It facilitates working with the database by abstracting many common tasks into methods that are easier to use and more consistent with the programming language. Other popular ORMs are SQLAlchemy and the Django ORM. Thanks to ORMs, you don't need to learn SQL (a programming language of its own) just to work with your data. Still, there are a few key facts that you will be comfortable with:

- Queries: We interact with databases through *queries*. Queries can fetch specific or all records, insert new data or change the way the database is structured.
- Tables: Just like an excel sheet or an R data frame, database tables represent a set of columns and rows. After defining a *model*, we create its corresponding table.
- Foreign / Many to Many Relationships: Most of the time, our models have some sort of links between them. For example, below we define a model named Tweet and a model named User. Each Tweet must have exactly one author, who in turn is a User object. Because Users and Tweets are stored in different tables, the database manages them via a feature called foreign keys. Instead of storing the entire User in the table that holds Tweets (which would duplicate the user data over and over again and make it hard to keep track of and search for), it just stores a link. If Tweet number 1 had User number 1 with username "alpha" as its author, the database representation of the Tweets "User" field would contain the number 1. Many to Many Relationships are similar but allow storing a list of things, such as a Tweet with multiple Hashtags.
- Joins: The database stores models in different tables. If we are interested in pieces of data on multiple tables, it needs to look at those tables in conjuction. A typical case would be searching for all Tweets from one username. First, we'd lookup the user with the given username, and then filter the Tweets table by this ID. In SQL, such an operation is called a JOIN. [REFERENCE: http://www.codeproject.com/Articles/33052/Visual-Representation-of-SQL-Joins]
- Transactions: We stated that SQL databses only store valid data. They do so by checking any input first and only accepting it once it passes the test. The process of storing data is called a *transaction*: If it succeeds, the data is guaranteed to be present. If it fails, transactions are *rolled back* and the database returns to its previous state.

Peewee works with the most popular SQL baserd databases - among them PostgreSQL (our favorite for lage projects) and MySQL which run as stand-alone server applications. In an attempt to keep this tutorial simple and lightweight, we chose to instead use SQLite, an *in-process database*. SQLite does not need an external application and is transparent and intuitive as to where and how it stores its data. SQLite databases are simply files that can be stored in any directory you want. A program simply declares which database file it wants to use and the ORM handles all the details of storage. This makes the datasets portable, easy to handle and reason about. At the same time, the database allows us to search for specific content without needing to iterate over all the items. There is one caveat: As with raw files (see below), we strongly recommend against accessing the database from multiple programs at the same time. SQLite offers better data consistency guarantees than raw files, but you should still separate data storage and analysis operations.

One final benefit of peewee: Should you ever feel the need for larger, faster storage, you can just download PostgreSQL and swap it against SQLite. Your code will continue to run without any major changes (apart from the database configuration, of course).

Handling Usernames and IDs
==========================
Twitter's data schemata have some implications that are not readily apparent at a first glance. Before modeling the data strucutres used to store tweets, it pays to have a closer look at what the official documentation says about the uniqueness of objects. Tweets and users both have an ID field which contains a large number that is guaranteed to be unique. Ergo, there is no other user sharing that number and no other Tweet sharing one ID. However, the same does not apply for other attributes, notably usernames (screen_name). Although Twitter does not allow users to pick a name that is already chosen, usernames become available once the previous account has been deleted. In practice, this happens surprisingly often. The larger and the longer the timeframe of a data collection, the more likely it is that usernames collide.
The correct solution to this is to always use user IDs, not usernames as identifiers for users. The database schema below does this by defining the user model with an unique ID, but allowing (non-unique) any username. The example dataset provided with the package actually contains seven cases where usernames collide. You can find them by inspecting the list of usernames:

# Create a list of usernames from all Users:
usernames = [user.username for user in User.select('username')]
# Create a counting object from the list
from collections import Counter
namecount = Counter(usernames)

# Show the ten usernames that occur the most often across all users. The number following the string corresponds to the number of unique users with this name
print(namecount.most_common(10))

Out[1]: [('halseycupcakes', 2), ('bootsntwinks', 2), ('kilIdrake', 2), ('junhoestan', 2), ('StephenWolfUNC', 2), ('JackofKent', 2), ('oooh_sebastian', 2), ('neumantj', 1), ('JohnHollings', 1), ('Guidotoons', 1)]

A Special Note on Date and Time
===============================
Date and time are crucial to almost all kinds of data analysis, but at the same time they are notoriously difficult to handle. There are two major sources of confusion and errors, which correspond to the layers of modifications that are performed to get from an universal reference time to local time: (a) a timezone offset is added to a reference time to get local time and (b) if applicable, a daylight saving offset yields the seasonally correct time. (This presentation on date and time may be helpful: [http://www.youtube.com/watch?v=ZroB-e4RXmo]).
When working with twitter, it is crucial to keep two things in mind and separate: The Twitter API always returns date/time in UTC, the universal reference time (no timezone offset, no daylight saving time!). However, depending on your object of study, users will see and use their local time. In many cases, we need to convert the native UTC dates into some other format. Some best practices for handling this are:
- Always store and/or explicitly declare the timezone information of your data
- Convert at the latest possible opportunity
- Store data in the most generic format possible (this usually means UTC)
Within this module, we have to compromise on date handling since SQLite does not support timezones. One option is storing the datetime as a string, but that prevents efficient date range queries. Instead, we chose to store UTC datetime without timezone information. So please keep in mind that you might need to convert both your queries and the resulting data. For example, if you're interested in tweets from New York City on January 1st of 2016, midnight to noon, you would first convert the range to UTC times:

from pytz import timezone
from pytz import utc
from datetime import datetime

# Hint: While it's possible to use timezone abbreviations such as "MST", some of those (such as CST) are ambiguous!
# While the pytz package will prevent you from using them, readers and users of your code might be fooled.
ny = timezone("America/New_York")
start_date = datetime(2015, 1, 1, 0, tzinfo=ny)
stop_date = datetime(2015, 1, 1, 12, tzinfo=ny)

start_date_utc = utc.normalize(start_date)
stop_date_utc = utc.normalize(stop_date)

Alternate ways of storing data
==============================
Caveat emptor: A comprehensive description of all data storage approaches is far beyond the scope of this tutorial. That being said, there are three options we wanted to highlight.

1.  Store tweets as json (in files). The most trivial approach to storing data ist to just put it into a file. In python, one can choose among a handful of serialization formats such as Pickle, json and yaml. Since Tweets are almost always received as json, it is often a good idea to take the raw data as received and store it in a file. The usual approach (that GNIP uses as well) is to write one tweet per line. That allows for easy parsing and handling of the files. Compression can be used to make this kind of storage more space efficient. A typical way of creating such a file would look like this:

with open("my-tweets.json", "w") as f:
    # MY_TWEETS would be the variable or generator function that holds your data
    for tweet in MY_TWEETS:
        f.write(json.dumps(tweet) + "\n")

The advantage of this format is definitely its ease of use and its portability. If you plan on using this for archiving your data, please make sure to safeguard against data corruption and data loss. Store the data in multiple places, use an error-tolerant filesystem such as ZFS or BTRFS or create recovery files with the PAR utility.
The major disadvantage of this format is that for each iteration of your analysis, you need to ingest the whole file. That is a process that can be time-consuming and inefficient, especially as your dataset grows. In addition, multiple programs accessing a single file at the same time are a recipe for disaster.

2.  Schemaless databases  (sometimes called NoSQL databases). There is an abundance of databases that allow data storage without the strict requirements of the schemata that are required by SQL. The most prominent example is mongodb, which stores arbitrary json. This ease of use can be a downside, as schemaless storage means that broken, wrong and partial data can be saved without noticing. Researchers must take special care to ensure that their datasets are consistent.

3.  Distributed databases. Both SQL and schemaless databases may allow running co-ordinated clusters across multiple computers. Using such a large system is usually much too complex, expensive and error-prone to be useful for twitter research. We only recommend evaluating distributed storage if your dataset is larger than 1 TB and/or if you have access to existing infrastructure.



"""

import logging
import datetime
from dateutil import parser
from pytz import utc, timezone

MST = timezone("MST")

import peewee
from playhouse.fields import ManyToManyField

# The name for the database is hardcoded here. Go ahead and change it if you want to.
# If you need to alter the filename programmatically, either load it from some external
# place such as a file or define these models in your own code.

# Set up database
# Attention! Before using the database, we need to create its tables -
# see code at the end of the file
db = peewee.SqliteDatabase('tweets.db', threadlocals=True)
db.connect()

#
# Database Models: Define the structure of our database
#


class BaseModel(peewee.Model):

    """
    Base model for setting the database to use
    """
    class Meta:
        database = db


class Hashtag(BaseModel):

    """
    Hashtag model.
    """
    tag = peewee.CharField(unique=True, primary_key=True)


class URL(BaseModel):

    """
    URL model.
    """
    url = peewee.CharField(unique=True, primary_key=True)


class User(BaseModel):

    """
    Twitter user model.
    Stores the user's unique ID as a primary key along with the username.
    """
    id = peewee.BigIntegerField(unique=True, primary_key=True)
    username = peewee.CharField(null=True)

    def last_tweet(self):
        return Tweet.select().where(Tweet.user == self).order_by(Tweet.id.desc())[0]

    def first_tweet(self):
        return Tweet.select().where(Tweet.user == self).order_by(Tweet.id.asc())[0]


class Tweet(BaseModel):

    """
    Tweet model.
    Stores the tweet's unique ID as a primary key along with the user, text and date.
    """
    id = peewee.BigIntegerField(unique=True, primary_key=True)
    user = peewee.ForeignKeyField(User, related_name='tweets', index=True)
    text = peewee.TextField()
    date = peewee.DateTimeField(index=True)
    tags = ManyToManyField(Hashtag)
    urls = ManyToManyField(URL)
    mentions = ManyToManyField(User)
    reply_to_user = peewee.ForeignKeyField(
        User, null=True, index=True, related_name='replies')
    reply_to_tweet = peewee.BigIntegerField(null=True, index=True)
    retweet = peewee.ForeignKeyField(
        'self', null=True, index=True, related_name='retweets')


#
# Helper functions for loading data into the database
#


def deduplicate_lowercase(l):
    """
    Helper function that performs two things:
    - Converts everything in the list to lower case
    - Deduplicates the list by converting it into a set and back to a list
    """
    lowercase = [e.lower() for e in l]
    deduplicated = list(set(lowercase))
    return deduplicated


def create_user_from_tweet(tweet):
    """
    Function for creating a database entry for
    one user using the information contained within a tweet

    :param tweet:
    :type tweet: dictionary from a parsed tweet
    :returns: database user object
    """
    user, created = User.get_or_create(
        id=tweet['user']['id'],
        defaults={'username': tweet['user']['screen_name']},
    )
    return user


def create_hashtags_from_entities(entities):
    """
    Attention: Casts tags into lower case!
    Function for creating database entries for
    hashtags using the information contained within entities

    :param entities:
    :type entities: dictionary from a parsed tweet's "entities" key
    :returns: list of database hashtag objects
    """
    tags = [h["text"] for h in entities["hashtags"]]
    # Deduplicate tags since they may be used multiple times per tweet
    tags = deduplicate_lowercase(tags)
    db_tags = []
    for h in tags:
        tag, created = Hashtag.get_or_create(tag=h)
        db_tags.append(tag)
    return db_tags


def create_urls_from_entities(entities):
    """
    Attention: Casts urls into lower case!
    Function for creating database entries for
    urls using the information contained within entities

    :param entities:
    :type entities: dictionary from a parsed tweet's "entities" key
    :returns: list of database url objects
    """
    urls = [u["expanded_url"] for u in entities["urls"]]
    urls = deduplicate_lowercase(urls)
    db_urls = []
    for u in urls:
        url, created = URL.get_or_create(url=u)
        db_urls.append(url)
    return db_urls


def create_users_from_entities(entities):
    """
    Function for creating database entries for
    users using the information contained within entities

    :param entities:
    :type entities: dictionary from a parsed tweet's "entities" key
    :returns: list of database user objects
    """
    users = [(u["id"], u["screen_name"]) for u in entities["user_mentions"]]
    users = list(set(users))
    db_users = []
    for id, name in users:
        user, created = User.get_or_create(
            id=id,
            defaults={'username': name},
        )
        db_users.append(user)
    return db_users


def create_tweet_from_dict(tweet, user=None):
    """
    Function for creating a tweet and all related information as database entries
    from a dictionary (that's the result of parsed json)
    This does not do any deduplication, i.e. there is no check whether the tweet is
    already present in the database. If it is, there will be an UNIQUE CONSTRAINT exception.

    :param tweet:
    :type tweet: dictionary from a parsed tweet
    :returns: bool success
    """
    # If the user isn't stored in the database yet, we
    # need to create it now so that tweets can reference her/him
    try:
        if not user:
            user = create_user_from_tweet(tweet)
        tags = create_hashtags_from_entities(tweet["entities"])
        urls = create_urls_from_entities(tweet["entities"])
        mentions = create_users_from_entities(tweet["entities"])
        # Create new database entry for this tweet
        t = Tweet.create(
            id=tweet['id'],
            user=user,
            text=tweet['text'],
            # We are parsing Twitter's date format using a "magic" parser from the python-dateutil package
            # The resulting datetime object has timezone information attached.
            # However, since SQLite cannot store timezones, that information is stripped away.
            # If you use PostgreSQL instead, please refer to the DateTimeTZField in peewee
            # and remove the "strftime" call here
            date=parser.parse(tweet['created_at']).strftime(
                "%Y-%m-%d %H:%M:%S"),
        )
        if tags:
            t.tags = tags
        if urls:
            t.urls = urls
        if mentions:
            t.mentions = mentions
        if tweet["in_reply_to_user_id"]:
            # Create a mock user dict so we can re-use create_user_from_tweet
            reply_to_user_dict = {"user":
                                  {'id': tweet['in_reply_to_user_id'],
                                   'screen_name': tweet['in_reply_to_screen_name'],
                                   }}
            reply_to_user = create_user_from_tweet(reply_to_user_dict)
            t.reply_to_user = reply_to_user
            t.reply_to_tweet = tweet['in_reply_to_status_id']
        if 'retweeted_status' in tweet:
            retweet = create_tweet_from_dict(tweet['retweeted_status'])
            t.retweet = retweet
        t.save()
        return t
    except peewee.IntegrityError as exc:
        logging.error(exc)
        return False

#
# Helper functions to get summary statistics over the given database
#


def database_counts():
    """
    Generate counts for objects in the database.

    :returns: dictionary with counts
    """
    return {
        "tweets": Tweet.select().count(),
        "hashtags": Hashtag.select().count(),
        "urls": URL.select().count(),
        "users": User.select().count(),
    }


def mention_counts(start_date, stop_date):
    """
    Perform an SQL query that returns users sorted by mention count.
    Users are returned as database objects in decreasing order.
    The mention count is available as ".count" attribute.
    """
    # First we get the Table that sits between Tweets and Users
    mentions = Tweet.mentions.get_through_model()
    # The query - note the Count statement creating our count variable
    users = (User.select(User, peewee.fn.Count(mentions.id).alias('count'))
             # join in the intermediary table
             .join(mentions)
             # join in the tweets
             .join(Tweet, on=(mentions.tweet == Tweet.id))
             # filter by date
             .where(Tweet.date >= to_utc(start_date), Tweet.date < to_utc(stop_date))
             # group by user to eliminate duplicates
             .group_by(User)
             # sort by tweetcount
             .order_by(
        peewee.fn.Count(mentions.tweet).desc())
    )
    return users


def url_counts(start_date, stop_date):
    """
    Perform an SQL query that returns URLs sorted by mention count.
    URLs are returned as database objects in decreasing order.
    The mention count is available as ".count" attribute.
    """
    urlmentions = Tweet.urls.get_through_model()
    urls = (URL.select(URL, peewee.fn.Count(urlmentions.id).alias('count'))
            .join(urlmentions)
            .join(Tweet, on=(urlmentions.tweet == Tweet.id))
            .where(Tweet.date >= to_utc(start_date), Tweet.date < to_utc(stop_date))
            .group_by(URL)
            .order_by(peewee.fn.Count(urlmentions.tweet).desc())
            )
    return urls


def hashtag_counts(start_date, stop_date):
    """
    Perform an SQL query that returns hashtags sorted by mention count.
    Hashtags are returned as database objects in decreasing order.
    The mention count is available as ".count" attribute.
    """
    hashtagmentions = Tweet.tags.get_through_model()
    hashtags = (Hashtag.select(Hashtag, peewee.fn.Count(hashtagmentions.id).alias('count'))
                .join(hashtagmentions)
                .join(Tweet, on=(hashtagmentions.tweet == Tweet.id))
                .where(Tweet.date >= to_utc(start_date), Tweet.date < to_utc(stop_date))
                .group_by(Hashtag)
                .order_by(peewee.fn.Count(hashtagmentions.tweet).desc())
                )
    return hashtags


def retweet_counts(start_date, stop_date, n=50):
    """
    Find most retweeted users.
    Instead of performing a rather complex SQL query, we do this in more
    readable python code. It may take a few minutes to complete.

    It's possible to pass in a premade query that will be used as the
    baseline for retweet counts (for example in order to limit
    date ranges).
    """
    from collections import Counter
    rt = Tweet.alias()
    rtu = User.alias()
    baseline = (Tweet.select().
                where(Tweet.date >= to_utc(start_date), Tweet.date < to_utc(stop_date))
                )
    query = (baseline.select(Tweet.id, rt.id, rtu.id)
             .join(rt, on=(Tweet.retweet == rt.id))
             .join(rtu, on=(rt.user == rtu.id))
             )
    c = Counter(
                (tweet.retweet.user.id for tweet in query)
               )
    # We use an ordered dict for the results so that the top results
    # appear first
    from collections import OrderedDict
    results = OrderedDict()
    for k, v in c.most_common(n):
        results[User.get(id=k).username] = v
    return results


#
# Helper functions for querying data
#

def tweetcount_per_user():
    """
    This function executes a query that:
    - joins the User and Tweet tables so we can reason about their Relationship
    - groups by User so we have one result per User
    - counts tweets per user and stores the result in the name "count"
    - orders users by descending tweet count

    The users objects are accessible when looping over the query, such as:
    for user in query:
        print("{0}: {1}".format(user.username, user.count))

    Note that as always, this query can be augmented by appending further operations.
    """
    tweet_ct = peewee.fn.Count(Tweet.id)
    query = (User
             .select(User, tweet_ct.alias('count'))
             .join(Tweet)
             .group_by(User)
             .order_by(tweet_ct.desc(), User.username))
    return query


def first_tweet():
    """
    Find the first Tweet by date
    """
    return Tweet.select().order_by(Tweet.date.asc()).first()


def last_tweet():
    """
    Find the last Tweet by date
    """
    return Tweet.select().order_by(Tweet.date.desc()).first()


def to_utc(dt):
    """
    Helper function to return UTC-based datetime objects.
    If the input datetime object has any timezone information, it
    is converted to UTC. Otherwise, the datetime is taken as-is and
    only the timezone information UTC is added.
    """
    if dt.tzinfo is None:
        logging.warning(
            "You supplied a naive date/time object. Timezone cannot be guessed and is assumed to be UTC. If your date/time is NOT UTC, you will get wrong results!")
        return utc.localize(dt)
    else:
        return utc.normalize(dt)


def objects_by_interval(Obj, date_attr_name="date", interval="day", start_date=None, stop_date=None):
    """
    General helper function that returns objects by date intervals, mainly useful for counting.
    WARNING: If used as-is with SQLite, all date/times in data and queries are UTC-based!
    If you want to use local time for queries, take note that it will be converted correctly
    ONLY if you supply the correct timezone information. In general, as long as you only
    use timezone-aware objects you should be safe.

    :param obj:
    :type obj: database model
    :param date_attr_name:
    :type date_attr_name: the name of the filed containing date/time information on the model obj, as strings
    :param interval:
    :type interval: day (default), hour, minute as string.
    :param start_date:
    :type start_date: date to start from as datetime object, defaults to first date found.
    :param stop_date:
    :type stop_date: date to stop on as datetime object, defaults to last date found.
    :returns: bool success
    """
    # define intervals, then select the one given as a function argument
    interval = {
        "minute": datetime.timedelta(minutes=1),
        "hour": datetime.timedelta(hours=1),
        "day": datetime.timedelta(days=1),
    }.get(interval)
    date_field = getattr(Obj, date_attr_name)
    # Determine first object if no start_date given
    # Todo: Maybe prettify this humongous expression.
    start_date = start_date or MST.localize(datetime.datetime(2015, 10, 27, 0))
    stop_date = stop_date or MST.localize(datetime.datetime(2015, 11, 2, 23, 59))
    # If we wanted to use the first and last element instead of the pre-determined dates,
    # this would be the way to do it:
    # getattr(Obj.select().order_by(date_field.asc()).first(), date_attr_name)
    # getattr(Obj.select().order_by(date_field.desc()).first(), date_attr_name)
    # Ensure UTC times
    start_date = to_utc(start_date)
    stop_date = to_utc(stop_date)
    # This iteration code could be shorter, but using two dedicated variables
    # makes it more intuitive.
    interval_start = start_date
    interval_stop = start_date + interval
    # Notice that this loop stops BEFORE the interval extends beyond stop_date
    # This way, we may get intervals that do not reach stop_date, but on the other hand
    # we never get intervals that are not covered by the data.
    while interval_stop <= stop_date:
        query = Obj.select().where(
            date_field >= interval_start, date_field < interval_stop)
        # First yield the results, then step to the next interval
        yield ((interval_start, interval_stop), query)
        interval_start += interval
        interval_stop += interval


#
# Onetime setup
#


# Set up database tables. This needs to run at least once before using the db.
try:
    db.create_tables([Hashtag, URL, User, Tweet, Tweet.tags.get_through_model(
    ), Tweet.urls.get_through_model(), Tweet.mentions.get_through_model()])
except Exception as exc:
    logging.debug(
        "Database setup failed, probably already present: {0}".format(exc))
