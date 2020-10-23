=====================
Downloading Wikipedia
=====================

Just some consolidated notes to save you time figuring it out yourself.

* XML/SQL Wikipedia content dumps are produced twice a month (1st/20th)

 * Second dump is considered 'partial' and only contains information about current revisions

   * Seems complete to me

* The main servers only keep the last 7 dumps

* Wikipedia offers the page contents either as a single file or as multiple smaller files

 * This library defaults to use multiple smaller files as it allows parsing early files while downloading the rest
 * In the smaller files, their names each end in a suffix that indicates the range of page IDs stored in a given file

   * e.g. enwiki-20201001-pages-articles-multistream1.xml-p1p41242.bz2 holds all pages with IDs from 1 - 41,242

* Wikipedia has indexed where pages are in the compressed data

 * The Bzip file design allows for the data to be broken into parts (called streams) yet still contain all these compressed streams in one file
 * Wikipedia publishes the addresses the "stream" containing each page inside the compressed data

   * This means that you don't have to decompress an entire file to get a single page, just the stream that contains it
   * Each stream contains 100 pages
   * This is what is meant by multistream in a filename
   * The indices are labeled as multistream-index in their filename

     * Indices are composed of byte_offset:page_id:page_name (e.g. 617:10:AccessibleComputing)
     * Page ID != page count since pages can be deleted

   * The loss in bz2 compression size caused by this is roughly 10%

 * There are several hundred streams in each file
 * BZ2 files expand to approximately 3.5x


Details on the lib
------------------
* We don't actually pull from the 'latest' directory

 * The RSS XML files imply that the files in latest are copies of the most recent dump date
 * Files in 'latest/' don't have their date in the name (which means trouble figuring things out 6 months later)
 * The HTML page listing the latest files:
   * has a different layout so require custom page parsing
   * includes occasional files from earlier dumps (older copies of the same data)
 * While these things are easy enough to overcome, there seems to be no gain in adding extra code to do so

See:
* https://en.wikipedia.org/wiki/Wikipedia:Size_of_Wikipedia
* https://en.wikipedia.org/wiki/Wikipedia:Modelling_Wikipedia%27s_growth#Data_set_for_number_of_articles
