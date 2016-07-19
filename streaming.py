#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2016 Pascal Jürgens and Andreas Jungherr
# See License.txt

"""
Access to twitter's real-time streams
-------------------------------------
Connects to twitter's real-time streams for random sample and tracking streams

Requirements:
    - depends on authentication module auth.py
    - libraries request

Twitter's real-time tweet streams
=================================

The streaming api delivers a continuous stream of incoming tweets - either from a random sample or matching given criteria - and thus follows different paradigms than the REST api. Instead of performing multiple independent requests, a stream is connected once. The connection stays alive as long as both sides keep it open and transports tweets as they are coming in. Under the hood, streams are still http connections, which means they have response headers, are initiated via GET or POST requests and terminate with a status code. There are some idiosyncracies to watch out for:

- Stream connections can exist for a very long time, but don't need to transfer data all along. A filter stream for rarely used words, for example, can go minutes or even hours without yielding a tweet. Usually, both server and client would terminate the connection if they don't receive anything for that long, so twitter periodically sends a "keepalive" signal.
- Just like REST requests, streams are rate-limited. Twitter restricts the amount of tweets delivered via the random sample stream and via tracking streams. A complete stream containing all tweets and a tracking stream that guarantees to deliver all matching tweets are only available commercially via twitter's data broker `gnip`_. Take note that all free sample streams are identical: Connecting multiple times is prohibited and yields no additional data.
- Even though streams are designed to stay open, there are numerous reasons why they can end. It's helpful to distinguish stream termination in two phases: (1) Failures to connect result in an http `error status code`_. For example, if you try to connect with invalid tokens, the connection ends with a status code of 401. (2) Failures *during streaming* instead try to send a `failure explanation`_ before disconnecting. The most common causes for disconnects are connection issues on the users' or twitter's side (such as a restart of the server delivering the stream).
- Twitter provides some very helpful metadata while the stream is running. In between tweets, you will see blank lines as keepalive data, delete notices broadcasting the IDs of deleted tweets and warnings if your application is not fast enough to keep up with the stream. From a scientific point of view, the most important metadata are *limit notices*. Lines such as this::

    {'limit': {'timestamp_ms': '1443095140794', 'track': 16}}

 tell your application, that as of the given timestamp, your tracking stream has omitted 16 matching tweets. Short of buying access, it's not possible to know what exactly was omitted. Still, tracking limit notices allows researchers to roughly quantify the coverage of their tracking data.


Implementation Details
----------------------

Any application that connects to the streaming API needs to somehow interleave connection handling and the actual processing of tweets. There are many ways to go about this: It would be possible, for example, to set up two separate programs, even on different machines, and pass the stream between them. This would be a modular and performant design, but introduces  complexity and additional opportunities for failure. Instead, we opted to use a simple unified design.

In this module, you will find a function for setting up either a random sample or a tracking stream. In addition to the sample/tracking parameters, it takes *two functions* as optional arguments: One function for handling tweets and one for handling non-tweet messages. This design is  called "callback" - our stream handling code hands a tweet over to some processing function which then hands control back to the stream handler.

**It is absolutely vital that the code processing tweets does so quickly.** While the callback functions do their work, the main function is paused and cannot process incoming data. If your code takes too long, twitter will disconnect the stream and - if that happens too often - disable your API access altogether. So make sure you only perform the absolute minimum work while streaming. Post-processing is best handled after the data collection.


.. _`failure explanation`: https://dev.twitter.com/streaming/overview/messages-types#disconnect_messages
..  _`error status code`: https://dev.twitter.com/streaming/overview/connecting
.. _`gnip`: https://www.gnip.com

"""

import twitter_auth
import json
import datetime
import time
import logging


auth = twitter_auth.authorize()

# We use these global variables to store information about errors
last_error_date = None
last_error_wait = 0


# Stream URLs

SAMPLE_URL = "https://stream.twitter.com/1.1/statuses/sample.json"
FILTER_URL = "https://stream.twitter.com/1.1/statuses/filter.json"
# Set basic defaults: Warn about slow processing and don't filter "low quality" content
DEFAULT_PARAMETERS = {'stall_warnings': 'true', 'filter_level': 'none'}


class IrrecoverableStreamException(Exception):

    """
    Exception (error) that is raised when the stream encounters an error that cannot be recovered from.
    """
    pass


def backoff(errorcode=None):
    """
    Helper function for waiting on errors.
    Follows recommendations from twitter developer documentation:
    https://dev.twitter.com/streaming/overview/connecting

    :param errorcode:
    :type errorcode: int
    :returns: sleep time in seconds
    :returns type: int
    """
    global last_error_date
    global last_error_wait

    now = datetime.datetime.utcnow()
    # Somewhat unusual syntax: the bracket forces computation of the
    # time delta, and total_seconds is called on that
    if last_error_date:
        time_since_error = (now - last_error_date).total_seconds()
    else:
        time_since_error = 1800  # The exact amount doesn't matter
        # since we only check whether there was an error in the last
        # half hour below
    last_error_date = now

    # If the last error was more than 30 minutes ago, reset the wait duration to one minute
    base_sleep = last_error_wait if time_since_error < 1800 else 1

    # Rate limiting error
    if errorcode == 420:
        # Use a minimum of 60 seconds or double the previous amount of backoff
        sleep = max(base_sleep, 30) * 2

    # Technical error on twitter's side
    elif errorcode in [503]:
        # Use a minimum of 5 seconds or double the previous amount of backoff
        sleep = max(base_sleep, 2.5) * 2
        # Limit backoff to a maximum of 320 seconds
        sleep = min(sleep, 320)

    # Irrecoverable errors, cannot continue
    elif errorcode in [401, 403, 404, 406, 413, 416]:
        logging.error(u"Connection HTTP error {0}".format(errorcode))
        raise IrrecoverableStreamException
    # We don't handle any other errors
    else:
        raise ValueError

    last_error_wait = sleep
    logging.error("Waiting for {0} seconds on error {1}".format(sleep, errorcode))
    time.sleep(sleep)
    return sleep


def stream(on_tweet=None, on_notification=None, track=None, follow=None):
    """
    Connect to sample stream.
    Handles connecting, json parsing, error handling and disconnecting.

    Example usage:
        def pt(tweet):
            print(tweet["text"])
        streaming.stream(on_tweet=pt, track=["#cdu", "#spd"])

    Note: While Twitter allows combinations of track and follow parameters,
    we encourage chosing one of both and performing any additional filtering
    in post processing. This makes reasoning about sampling and erros easier.

    :param on_tweet:
    :type on_tweet: function
    :param on_notification:
    :type on_notification: function
    :param track:
    :type track: list of tracking strings
    :param follow:
    :type follow: list of user IDs to track
    """
    # Use default values
    parameters = DEFAULT_PARAMETERS
    url = FILTER_URL
    # Do we track phrases?
    if track:
        parameters['track'] = track
    # Do we track users?
    elif follow:
        parameters['follow'] = follow
    # Else, it's a sample stream
    else:
        url = SAMPLE_URL
    while True:
        stream = auth.post(url, data=parameters, stream=True)
        if stream.status_code != 200:
            stream.close()
            backoff(int(stream.status_code))
        try:
            for line in stream.iter_lines():
                # Skip blank lines
                if line:
                    try:
                        data = json.loads(line.decode())
                        # Twitter tells us it's disconnecting the stream
                        if 'disconnect' in data:
                            stream.close()
                            backoff(data['disconnect'].get('code', 1))
                        # A tweet
                        elif "text" in data:
                            if on_tweet:
                                on_tweet(data)
                        # A non-tweet message
                        elif data and on_notification:
                            on_notification(data)
                    except Exception as e:
                        logging.error("Error! Encountered Exception {0} but continuing in order not to drop stream,".format(e))
        # Stop if users press ctrl-c
        except (KeyboardInterrupt, SystemExit):
            logging.error("User stopped program, exiting!")
            stream.close()
            raise
        except Exception as e:
            logging.error("Error! Encountered Exception {0} but continuing in order not to drop stream,".format(e))



def test():
    """
    Verify prerequisites for streaming API access by connecting to a tracking stream for approximately 10 seconds.

    :returns: tweetcount (int)
    """

    r = auth.post(FILTER_URL, data={"track": "if"}, stream=True)
    logging.debug("Connection status code: {0}".format(r.status_code))
    if r.status_code != 200:
        logging.debug("ERROR, closing connection. ", r)
        r.close()
    now = datetime.datetime.utcnow()
    stop_at = now + datetime.timedelta(seconds=10)
    tweetcount = 0
    for e in r.iter_lines():
        if datetime.datetime.utcnow() > stop_at:
            logging.debug("Finished, exiting. Got {0} tweets over ~10 seconds".format(tweetcount))
            return tweetcount
        if e:
            # The line from twitter is a bytestring, so we decode it to uicode
            js = json.loads(e.decode())
            # It's only a tweet if it has a "text" field
            if "text" in js:
                tweetcount += 1
                logging.debug(js["text"])
            else:
                logging.debug("\t {0}".format(js))

if __name__ == "__main__":
    test()
