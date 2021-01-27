# twitterresearch
A starter kit with code for data collection, preparation, and analysis of digital trace data collected on Twitter.

# ! HEADS-UP !
*2021-01-27*
Twitter today announced a much-welcome change in their API licensing. Researchers can now apply for privileged access via the developer dashboard, and receive many of the previously paid features for free, as long as the research is non-commercial. Access requires a project applicaiton. Find out more in this [blog post](https://blog.twitter.com/developer/en_us/topics/tips/2021/enabling-the-future-of-academic-research-with-the-twitter-api.html).

(The following warning no longer applies, but you should still investigate the use of the V2 API which this library has not yet been adapted to.)
Twitter has announced changes to its API that will likely severely limit free access to data: [Coverage](https://www.theverge.com/2018/4/6/17206524/twitter-tweetbot-twitterrific-apps-features-api-changes). If you intend to work with moderate to large datasets in the future, do your research on the paid options now.

**This kit complements a full-length tutorial which can be found on [SSRN](http://ssrn.com/abstract=2710146)**

# Getting Started

*There is a set of working usage examples in the file examples.py*

1.  Install requirements: `pip3 install -r requirements.txt`
** IMPORTANT NOTE: A new version of the peewee library has been released and breaks some APIs; take care to use the requirements file **
2.  (optional) install ipython `pip3 install -U ipython`
3.  start python
4.  Import libraries, such as `import rest, streaming`
5.  Use functions, for example:

```
archive = rest.fetch_user_archive("lessig")
for page in archive:
    for tweet in page:
        print(u"{0}: {1}".format(tweet["user"]["screen_name"], tweet["text"]))

```

## FAQ

- I'm getting unicode errors on windows, what can I do?
    Make sure that your windows install of python can print unicode characters to screen without throwing errors. If in doubt, [this library](https://anaconda.org/pypi/win_unicode_console) might help.


You are free to use and adapt the scripts provided in our twitterresearch package in your research. If you do so, please cite the package by providing the following information:

Pascal Jürgens and Andreas Jungherr. 2016. twitterresearch [Computer software]. Retrieved from https://github.com/trifle/twitterresearch

If you want to cite this tutorial please provide the following information:

Pascal Jürgens and Andreas Jungherr. 2016. A Tutorial for Using Twitter Data in the Social Sciences: Data Collection, Preparation, and Analysis. Available at SSRN: http://ssrn.com/abstract=2710146
