======================
Performance Statistics
======================

Some idea of what to expect using this library.

Wikipedia Data
--------------
As of Oct 2020:

Compressed bz2 sizes:

* Single multistream file size: 17.5 GB
* Smaller multistream files size:  total, max, min

Compressed lz4 sizes:

* Smaller multistream files size:  total, max, min


Do you need to get the latest of Wikipedia? Here's some basic stats on the rate of `Wikipedia size and growth`_.

Compression speedups
--------------------

Working with just the first file (232MB in bz2), I got the following on my i7-7820X CPU @ 3.60GHz (reading from SSD):

* bzcat entire file to /dev/null: 26s
* decompress entire bz2 file (as text) in Python: 31s
* decompress entire bz2 and recompress w/ lz4 in Python: 49s
* decompress entire lz4 file (as text) in Python: 4s
* read entire raw XML file from disk in Python: 2s

.. _Wikipedia size and growth: https://en.wikipedia.org/wiki/Wikipedia:Size_of_Wikipedia
