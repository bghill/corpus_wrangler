===============
Corpus Wrangler
===============


.. image:: https://img.shields.io/pypi/v/corpus_wrangler.svg
        :target: https://pypi.python.org/pypi/corpus_wrangler

.. image:: https://img.shields.io/travis/bghill/corpus_wrangler.svg
        :target: https://travis-ci.com/bghill/corpus_wrangler

.. image:: https://readthedocs.org/projects/corpus-wrangler/badge/?version=latest
        :target: https://corpus-wrangler.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status


.. image:: https://pyup.io/repos/github/bghill/corpus_wrangler/shield.svg
     :target: https://pyup.io/repos/github/bghill/corpus_wrangler/
     :alt: Updates


A simple utility library for downloading and handling large corpora (like Wikipedia).

General Motivation
------------------
There is a lifecycle to processing large text corpora:

* Download a sample (if available), figure out how you want to parse it (this is a bunch of experimenting)
* Download the entire corpus and process
* Find an issue that wasn't present in the sample, figure out fix, reprocess
* Six months later find you need to change something and you want to pull the latest data anyways

It is tempting to treat working with a corpus as a one time event that only warrants a throw away script. The
list above reminds us that it just isn't true. This library gives some simple tooling to help speed this work
along and add conveniences you might not bother with if you think of corpus wrangling as a one time event.
Beyond some simple parallelization, it also aims to add some conveniences:

* Large data == a slow download

 * Once you know how you want to parse things, there is no reason not to parallelize downloads with parsing

* Large data should mean parallel parsing

 * A lots of data can be easily parsed on a single machine, but that doesn't mean it should be parsed serially too!
 * Every hour lost to simple work is one less NLP experiment you get to run

* Slow decompression algorithms

 * Hosts (like Wikimedia) typically chose compression algorithms for maximum compression size
 * Fast algorithms can achieve 70% of the same compression but achieve near RAM access speeds in decompression
 * Never presume you will only need to visit the raw data once, you'll make your future self waste time waiting for decompression
 * This uses lz4 as it is super fast (and can allow multicore decompression on the command line)

* Knowing whether you are working on the latest data

 * Some text collections are periodically updated, might as well let a script check if I'm using the latest

* Code as documentation

 * Figuring out what to download

  * It isn't hard but I don't want to re-learn a host's file naming system when I update in 6 months

 * Reproducibility means you should store the code used to make current and past datasets

  * Using a convenience library can keep the code simple to read and clean

* Checksums

 * We should check this, but rarely do, might as well make it trivial to do so

Specific motivation
-------------------
The original use of a library is helpful to understand its design. In two words: word vectors. If this isn't your need,
I've tried to make the library generic enough that it is useful for you as well.

Yes, pre-made vectors do exist, but when they lack key vocabulary you need, making your own
is relatively simple with GloVe and word2vec.  Quite often folks like to start with Wikipedia as source for
their word vectors. As anyone who has done this from scratch can attest, that isn't as simple as one would like. Parse
the XML files naively and you need to load the entire file into memory (a waste). Once you have your page contents,
they are still filled with Wikitext markup as well as unresolved Wiki Template tags. Even after you find something like
`mwparserfromhell`_ to help with that headache, you are now faced with messes like fractional sentences, because the
second half of the sentence was a math equation.

This isn't to say that folks haven't posted their own solutions. I'm adding this one specifically because what I found
elsewhere wasn't as customizable or transparent as I'd like. I want to run with the defaults as a first pass to get a
rough cut to start with for my NLP pipeline, and while a later, when the algo is taking forever, I can come back and
experiment with small changes. For example, maybe I want to normalize all numbers in the corpus (either always numerals,
always written numbers, or a token like <NUMBER>). Making the data for experiments should fast and convenient.

* Free software: MIT license
* Documentation: https://corpus-wrangler.readthedocs.io.


Features
--------

* TODO

Credits
-------

This package was started with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
.. _mwparserfromhell: https://github.com/earwig/mwparserfromhell
