# twitterresearch
Best practices for social scientific twitter research


# Getting Started

1.  Install requirements: `pip install -r requirements.txt`
2.  (optional) install ipython `pip install -U ipython`
3.  start python
4.  Import libraries, such as `import rest, streaming`
5.  Use functions, for example:

```
archive = rest.fetch_user_archive("lessig")
for page in archive:
    for tweet in page:
        print(u"{0}: {1}".format(tweet["user"]["screen_name"], tweet["text"]))

```

6. There is a set of working usage examples in the file examples.py