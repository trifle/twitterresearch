#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2016 Pascal Jürgens and Andreas Jungherr
# See License.txt

"""
Basic Authentication
====================

Handles authorization with twitter API

Requirements:
    - libraries requests, requests_oauthlib, yaml and os (for finding and reading the auth keys)
    - twitter application credentials from apps.twitter.com -> Keys and Access Tokens -> Your Access Tokens

Talking to twitter
==================

Twitter uses version 1 of the widely used oauth procedure for getting access to its api. The process consists of multiple steps and generates a set of *tokens* that can be used similar to passwords. Each set of tokens is associated with one user or application. Requests issued with these are subject to the user account's limits (especially rate limits). This means it doesn't matter what computer the request comes from - all devices and IPs using the same tokens share the same allowance of requests.

The easiest way to obtain access is called `Application-only authentication`_: After creating a new application on twitter's developer pages, it is possible to create tokens for the application. You can generate your own on the tab *Keys and Access Tokens* under *Your Access Tokens*.


.. _`Application-only authentication`: https://dev.twitter.com/oauth/application-only

More information is available at https://dev.twitter.com/oauth/

"""

import yaml
import os
import logging
from requests_oauthlib import OAuth1Session


def find_keyfile(directory="."):
    """
    Helper function: Find the keyfile in a list of possible locations.
    The function iterates recursively through the directory and
    its subdirectories, emitting full paths for matching files.

    :returns: generator for keyfile paths
    """
    for root, dirnames, filenames in os.walk(directory):
        for filename in filenames:
            if filename == 'keys.yaml':
                yield os.path.join(root, filename)


def authorize(filepath=None):
    """
    Create an authorization object for use with the requests
    library. Takes the path to a yaml-encoded file containing twitter API keys.

    :param filepath:
    :type filepath: str
    :returns: OAuth1Session
    """
    # Try to find the file if no path was given
    # find_keyfile returns a generator and .next() gives the first match
    try:
        filepath = filepath or next(find_keyfile())
    except StopIteration:
        raise Exception("No Keyfile found - please place keys.yaml with your tokens in the project directory or pass a custom filepath to the authorize() function")
    # Load credentials from keyfile
    with open(filepath, 'r') as f:
        keys = yaml.load(f)

    # Create authentication object
    auth_object = OAuth1Session(client_key=keys["client_key"],
                                client_secret=keys["client_secret"],
                                resource_owner_key=keys["resource_owner_key"],
                                resource_owner_secret=keys["resource_owner_secret"])
    return auth_object


def test():
    """
    Verify prerequisites for REST API access, specifically that the required libraries are installed and that there are valid oauth keys.

    :returns: boolean.
    """

    auth = authorize()
    response = auth.get("https://api.twitter.com/1.1/account/settings.json")
    logging.info(response.ok)


if __name__ == "__main__":
    test()
