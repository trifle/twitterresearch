# twitterresearch
A starter kit with code for data collection, preparation, and analysis of digital trace data collected on Twitter.

**This kit complements a full-length tutorial which can be found on [SSRN](http://ssrn.com/abstract=2710146)**

# Getting Started

*There is a set of working usage examples in the file examples.py*

1.  Install requirements: `pip3 install -r requirements.txt`
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


You are free to use and adapt the scripts provided in our twitterresearch package in your research. If you do so, please cite the package by providing the following information:

Pascal Jürgens and Andreas Jungherr. 2016. twitterresearch [Computer software]. Retrieved from https://github.com/trifle/twitterresearch

If you want to cite this tutorial please provide the following information:

Pascal Jürgens and Andreas Jungherr. 2016. A Tutorial for Using Twitter Data in the Social Sciences: Data Collection, Preparation, and Analysis. Available at SSRN: http://ssrn.com/abstract=2710146
