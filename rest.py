#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2016 Pascal Jürgens and Andreas Jungherr
# See License.txt

"""
Access to twitter's REST api for one-time calls
===============================================
Connects to twitter's REST api for regular one/off requests

Requirements:
    - depends on authentication module auth.py
    - libraries request

Background on the twitter REST api
----------------------------------

Twitter offers two distinct modes of access to its data: Realtime streams (see streaming.py) and a so-called REST api. Documentation for both is available on twitter's `developer pages`_. There are two central characteristics that shape our stategies when 'talking' to twitter:

1. A REST api (application programming interface) uses single requests in order to retrieve (GET), publish (POST/PUT) or delete (DELETE) resources. Just like all http transfers, it is stateless - meaning that each transaction is isolated from others and stands for itself. With each request, we need to fully specify what we want to do. It is also our own duty to handle errors by retrying failed requests.
2. Just like any other major platform on the web, twitter needs to protect its resources against misuse. The api enforces a so-called *rate limit*, a maximum number of requests per time interval that users are allowed to perform.

Together, these two sets of rules (one technical, one institutional policy) shift the burden of valid and reliable data collection upon the researcher. We need to meticulously specify what data exactly we are looking for, track which parts of it have been downloaded so far and deal with numerous potential errors.


Implementation Details
----------------------

The technical implementation in this module fits somewhere between an average twitter tutorial and a *production-grade* data collection setup. Its foremost goal is to provide a clean and legible but solid blueprint. In contrast to most tutorials, it handles rate limits and errors gracefully. That is not to say the way we fetch data is the most efficient. We purposefully omitted some optimizations which would greatly increase the codebase and decrease its legibility. Here are some hints for potential improvements:

- Global rate limit: Currently, there is a global rate limit counter; we pause our access as soon as twitter tells us we have only four requests left. By doing this, we ignore the distinct (independent!) rate limits for different types of requests. For example, if you exhausted your rate limit for crawling an user's tweet archive, you can still perform requests for her friend list. If you implement a separate rate limit counter for every possible api endpoint, you can parallelize fetching multiple types of data.
- Only one user, one thread: All modules in this repository rely on the same user credentials defined in the single keyfile. It is possible to have several copies of the code fetch different sets of data with two or more different twitter accounts.

.. `developer pages`_: https://dev.twitter.com/rest/
"""

from requests.exceptions import ReadTimeout, ConnectTimeout
import twitter_auth

import itertools
import json
import time
import datetime
import logging

# --------------
# Set up logging
# --------------


# Get an authentication object from our authentication module
auth = twitter_auth.authorize()

# Set an initial value as the current rate limit.
# This variable will be set to a realistic value once we perform the first request.
# It is defined outside of any functions so the entore code in this module has access to it.
# Changing values from within a function require a "global" statement!
rate_limit = {'calls': 180, 'expires': datetime.datetime.utcnow()}

# API URLs

USER_TIMELINE_URL = 'https://api.twitter.com/1.1/statuses/user_timeline.json'
TWEETS_URL = 'https://api.twitter.com/1.1/statuses/lookup.json'
USERS_URL = 'https://api.twitter.com/1.1/users/lookup.json'
SEARCH_URL = 'https://api.twitter.com/1.1/search/tweets.json'

# ----------------
# HELPER FUNCTIONS
# ----------------


def grouper(iterable, n, fillvalue=None):
    """
    Collect data into fixed-length chunks or blocks
    Recipe from itertools documentation
    https://docs.python.org/2/library/itertools.html
    """
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return itertools.zip_longest(fillvalue=fillvalue, *args)


def wait_for_limit():
    """
    Helper function that waits until the rate limit is reset

    :side effects: Sleeps until rate_limit["expires"] datetime is reached
    """
    # Get the current UTC time
    now = datetime.datetime.utcnow()
    # Calculate timespan from now to reset
    # Somewhat unusual syntax: the bracket forces computation of the
    # time delta, and total_seconds is called on that.
    time_to_reset = (rate_limit['expires'] - now).total_seconds()
    # Sleep (wait) until this time has passed
    logging.error("Rate limit wait triggered, sleeping for {0} seconds".format(time_to_reset))
    time.sleep(time_to_reset)
    return time_to_reset


def throttled_call(*args, **kwargs):
    """
    Helper function for complying with rate limits.

    :parameters: Same as requests.get
    :returns: requests response object
    :side effects: updates global rate_limit count and expires values
    """
    # Declare rate_limit as global so we can write to it
    global rate_limit

    # Try as long as we need to succeed
    while True:
        # Take a break if there are less than 4 calls in the current rate limit timeslot
        now = datetime.datetime.utcnow()
        if rate_limit['calls'] < 5 and rate_limit['expires'] > now:
            wait_for_limit()
        try:
            result = auth.get(*args, **kwargs)
            # Update remaining calls and expiry date with the new number from twitter
            rate_limit['calls'] = int(result.headers.get('x-rate-limit-remaining', 0))
            if 'x-rate-limit-reset' in result.headers:
                rate_limit['expires'] = datetime.datetime.utcfromtimestamp(int(result.headers['x-rate-limit-reset']))
            return result
        # Catch these two errors and continue
        # It is generally a good idea to only catch errors that you anticipate
        # Unknown Exceptions should be allowed to occur so you learn about them!
        except (ReadTimeout, ConnectTimeout):
            logging.error("There was a network timeout, retrying!")
        finally:
            # Wait for one second, regardless of our success.
            # Waiting one second between requests is a generally accepted sane default
            time.sleep(1)

# -----------------------
# DATA FETCHING FUNCTIONS
# -----------------------


def search_tweets(query, **kwargs):
    """
    Fetch tweets from twitter's search.
    Optional parameters are passed on to the requests library

    :param query:
    :type query: string including search operators
    :returns: tuple (result object, list of tweets, search metadata)
    """
    # Set number of fetched tweets to the given amount, else to 200
    kwargs['count'] = kwargs.get('count', 200)
    # Set the query parameter
    kwargs['q'] = query
    result = throttled_call(SEARCH_URL, params=kwargs)
    # Decode JSON
    response_data = json.loads(result.text)
    return (result, response_data['statuses'], response_data['search_metadata'])


def fetch_user_tweets(user, **kwargs):
    """
    Fetch tweets from an user's archive.
    Optional parameters are passed on to the requests library

    :param user:
    :type user: user_id as int or screen_name as str
    :returns: tuple (result object, list of tweets)
    """
    # Set number of fetched tweets to the given amount, else to 200
    kwargs['count'] = kwargs.get('count', 200)
    # Is the user parameter an integer number, ergo an user ID?
    if isinstance(user, int):
        kwargs['user_id'] = user
    # Is the user parameter a string, ergo an user name?
    elif isinstance(user, str):
        kwargs['screen_name'] = user
    result = throttled_call(USER_TIMELINE_URL, params=kwargs)
    # Decode JSON
    return (result, json.loads(result.text))


def fetch_user_archive(user, **kwargs):
    """
    Fetch all available tweets from an user's archive.
    Optional parameters are passed on to the requests library.
    This is a generator function that only does requests if necessary.
    If you just need all tweets, call it like this for a list of tweets::

        from itertools import chain
        results = list(chain.from_iterable(fetch_user_archive("pascal")))

    :param user:
    :type user: user_id as int or screen_name as str
    :returns: generator object yielding pages of tweets
    """
    status = 200
    max_id = None
    while status == 200:
        # If we have a valid max_id, use that; else do a simple normal request
        result, tweets = fetch_user_tweets(user, max_id=max_id, **kwargs) if max_id else fetch_user_tweets(user)
        # Set the status variable - if it's not 200, that's an error and the loop exits
        status = result.status_code
        # if tweets is empty then we reached the end of the archive
        if not tweets:
            break
        elif 'errors' in tweets:
            logging.error("Encountered errors, skipping: {0}".format(tweets['errors']))
            yield False
        # Calculate the new max_id to use in the next request
        max_id = min((int(t['id']) for t in tweets)) - 1
        logging.info("Fetched {0} tweets for {1} - {2} calls remaining".format(len(tweets), user, rate_limit['calls']))
        # Return fetched tweets
        yield tweets


def fetch_tweets(ids, **kwargs):
    """
    Fetch tweets from a list of IDs.
    Optional parameters are passed on to the requests library
    This is used to "hydrate" data sets that contain only IDs.

    :param ids:
    :type ids: tweet_id as str, int or list
    :returns: tuple (result object, list of tweets)
    """
    # This call allows fetching 100 tweets at most
    assert(len(ids) <= 100)
    # The API requires a comma-separated list of tweet IDs
    if isinstance(ids, (int, str)):
        kwargs["id"] = str(ids)
    elif isinstance(ids, (list, tuple)):
        kwargs["id"] = ",".join([str(i) for i in ids])
    # Call API
    result = throttled_call(TWEETS_URL, params=kwargs)
    # Decode JSON
    return (result, json.loads(result.text))


def fetch_tweet_list(ids, **kwargs):
    """
    Fetch an arbitrarily large number of tweets by ID

    :param ids:
    :type ids: tweet_id as str, int or list
    :returns: generator object yielding lists of tweets
    """
    # Split given tweet IDs into blocks of 100 that can
    # be retrieved in one call
    for page in grouper(ids, 100):
        result, tweets = fetch_tweets(page)
        logging.info("Fetched {0} tweets from list - {1} calls remaining".format(len(tweets), rate_limit['calls']))
        yield tweets


def fetch_users(ids=None, screen_names=None, **kwargs):
    """
    Fetch users from a list of IDs or screen_names (max 100 at a time).
    Optional parameters are passed on to the requests library
    This is used to "hydrate" users sets that contain only IDs or screen_names.

    :param ids:
    :type ids: user_id as str, int or list
    :param screen_names:
    :type screen_names: screen_name as str, int or list
    :returns: tuple (result object, list of users)
    """
    # This call allows fetching 100 users at most
    assert(len(ids) <= 100 if ids else len(screen_names) <= 100)
    # The API requires a comma-separated list of user IDs
    if isinstance(ids, (int, str)):
        kwargs["user_id"] = str(ids)
    elif isinstance(ids, (list, tuple)):
        kwargs["user_id"] = ",".join([str(i) for i in ids])
    elif isinstance(screen_names, (int, str)):
        kwargs["screen_name"] = str(screen_names)
    elif isinstance(screen_names, (list, tuple)):
        kwargs["screen_name"] = ",".join([str(s) for s in screen_names])
    # Call API
    result = throttled_call(USERS_URL, params=kwargs)
    # Decode JSON
    return (result, json.loads(result.text))


def fetch_user_list_by_id(ids=None, **kwargs):
    """
    Fetch an arbitrarily large number of users by ID

    :param ids:
    :type ids: user_id as str, int or list
    :returns: generator object yielding lists of tweets
    """
    # Split given tweet IDs into blocks of 100 that can
    # be retrieved in one call
    pages = grouper(ids, 100)
    for page in pages:
        result, users = fetch_users(ids=page)
        logging.info("Fetched {0} users from ID list - {1} calls remaining".format(len(tweets), rate_limit['calls']))
        yield users


def fetch_user_list_by_screen_name(screen_names=None, **kwargs):
    """
    Fetch an arbitrarily large number of users by screen_name

    :param screen_names:
    :type screen_names: screen_name as str, int or list
    :returns: generator object yielding lists of tweets
    """
    # Split given tweet IDs into blocks of 100 that can
    # be retrieved in one call
    pages = grouper(screen_names, 100)
    for page in pages:
        result, users = fetch_users(screen_names=page)
        logging.info("Fetched {0} users from list - {1} calls remaining".format(len(tweets), rate_limit['calls']))
        yield users
