# -*- coding: utf-8 -*-
"""A corpus wrangler for Wikimedia.

A set of classes to track, download, and parse Wikimedia dumps.

  Typical usage example:

    from corpus_wrangler import wikimedia as ww

    wiki_ct = ww.CorpusTracker()

    wiki_article_sets = wiki_ct.get()

    add filter
    add tokenizer
    add cleaner
    add paginator
    add sentencizer

    for article_set in wiki_article_sets:
        for doc in article_set:
            for sent in doc:
                # write sentence to output in a chosen format

"""
import os
import re
from importlib import resources

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup


class CorpusFiles:
    """A set of _files from a wikimedia dump.

    The file set can be either local or a target set of files to be
    downloaded. The set is stored as a yielding iterator, to allow
    for files to be processed as soon as they are downloaded.
    """

    def __init__(self):
        """Initialize the storage for file records."""
        self._dump_columns = ["pages", "stubs", "logging", "abstracts", "sql", "checksum",
                              "titles", "namespaces", "articles", "index", "rev_metadata",
                              "current", "history", "set", "indexed"]
        self._columns = ["name", "path", "size", "file_type", "wiki", "date"] + self._dump_columns
        self._files = pd.DataFrame(columns=self._columns)

    def add_files(self, path, file_list, wiki_names):
        """Process a list of files and add each to the CorpusFile records.

        Record the file name, path, size, and file type. If the file follows the naming
        convention of Wikimedia, parse the file name to record the nature of the file
        contents.

        Args:
            path (str): Filesystem path for the provided file list
            file_list (list): A list of file names to be processed
            wiki_names (list): A list of known wiki names
        """
        file_rows = []
        for file in file_list:
            size = os.path.getsize(os.path.join(path, file))
            # get file extension
            [fname, ext] = os.path.splitext(file)
            (wiki, date, name) = self._parse_dump_info(fname, wiki_names)
            row = {"name": file, "path": path, "size": size, "file_type": ext,
                   "wiki": wiki, "date": date}
            if name:  # this file belongs to a wiki dump, get the details
                self._parse_wiki_file_names(name, row)
            file_rows.append(row)

        self._files = self._files.append(pd.DataFrame.from_dict(file_rows, orient='columns'))

    def _parse_dump_info(self, name, wiki_names):  # noqa: no-self-use  pylint: disable=no-self-use
        """Return the name of the wiki this file comes from as well as the dump date."""
        name_parts = name.split('-', 2)
        if name_parts[0] in wiki_names:
            if len(name_parts) > 1 and len(name_parts[1]) == 8 and name_parts[1].isnumeric():
                if len(name_parts) == 3:
                    return name_parts[0], name_parts[1], name_parts[2]
                return name_parts[0], name_parts[1], None
            return name_parts[0], None, None
        return None, None, None

    def _parse_wiki_file_names(self, name, description):  # noqa: no-self-use  pylint: disable=no-self-use,too-many-branches,too-many-statements
        """Parse the file name to infer a description of the file contents.

        Granted, this will always be limited in the face of user naming conventions.

        While this is large and clunky, it is largely a side-effect of the number of file
        types Wikimedia creates. This has ended up being more robust and faster than
        creating a grammar and using ANTLR to parse the names.

        Args:
            name (str): File name with the wiki name and date removed along with the file extension
            description (dict): Dict with what has been recorded about this file so far
        """
        if name.startswith("pages"):
            description["pages"] = True
            if name.startswith("pages-meta"):
                description["rev_metadata"] = True
                if name.startswith("pages-meta-history"):
                    description["history"] = True
                    if re.match(r"^pages-meta-history(\d|\d\d)\.xml-p\d+p\d+$", name):
                        description["set"] = True
                elif name.startswith("pages-meta-current"):
                    description["current"] = True
                    if re.match(r"^pages-meta-current(\d|\d\d)\.xml-p\d+p\d+$", name):
                        description["set"] = True
            elif name.startswith("pages-articles"):
                description["articles"] = True  # pages-articles.xml
                if name.startswith("pages-articles-multistream"):
                    description["indexed"] = True  # pages-articles-multistream.xml
                    if re.match(r"^pages-articles-multistream(\d|\d\d)\.xml-p\d+p\d+$", name):
                        description["set"] = True
                    elif name.startswith("pages-articles-multistream-index"):
                        description["index"] = True  # pages-articles-multistream-index.txt
                        if re.match(r"^pages-articles-multistream-index(\d|\d\d)\.txt-p\d+p\d+$", name):  # noqa: E501 pylint: disable=line-too-long
                            description["set"] = True
                elif re.match(r"^pages-articles(\d|\d\d)\.xml-p\d+p\d+$", name):
                    description["set"] = True
            elif name.startswith("pages-logging"):
                description["logging"] = True  # pages-logging.xml
                if re.match(r"^pages-logging(\d|\d\d)\.xml$", name):
                    description["set"] = True
        elif name.startswith("stub"):
            if name.startswith("stub-articles"):
                description["articles"] = True  # articles.xml
                if re.match(r"^stub-articles(\d|\d\d)\.xml$", name):
                    description["set"] = True
            elif name.startswith("stub-meta"):
                description["rev_metadata"] = True
                if name.startswith("stub-meta-current"):
                    description["current"] = True  # stub-meta-current.xml
                    if re.match(r"^stub-meta-current(\d|\d\d)\.xml$", name):
                        description["set"] = True
                elif name.startswith("stub-meta-history"):
                    description["history"] = True  # stub-meta-history.xml
                    if re.match(r"^stub-meta-history(\d|\d\d)\.xml$", name):
                        description["set"] = True
        elif name.startswith("abstract"):
            description["abstracts"] = True
            if re.match(r"^abstract(\d|\d\d)\.xml$", name):
                description["set"] = True
        elif ".sql" in name:
            description["sql"] = True
        elif name in ["md5sums", "sha1sums"]:
            description["checksum"] = True
        elif name.startswith("all-titles"):
            description["titles"] = True
        elif name == "siteinfo-namespaces.json":
            description["namespaces"] = True

    def _filter_nones(self, array):  # pylint: disable=no-self-use
        """Remove None values that get returned by unique(), casts to a list."""
        array = array[array != np.array(None)]
        return list(array)

    def get_wikis(self):
        """Return a list of all wikis represented in the CorpusFiles set.

        Returns:
            A string list of wiki names
        """
        return self._filter_nones(self._files.wiki.unique())

    def get_dumps(self, wiki):
        """Return a list of all dumps for a given wiki in the CorpusFiles set.

        Args:
            wiki (str): The name of the wiki to search for dump files from
        Returns:
            A string list of dump dates
        """
        return self._filter_nones(self._files[self._files['wiki'] == wiki].date.unique())

    def get_file_count(self, wiki=None, date=None):
        """Return the number of files in this CorpusFiles' records.

        With no arguments, returns total file count. With only the wiki name, files
        associated with that wiki. With a wiki name and date, the file account for
        that dump.

        Args:
            wiki (str): The name of a wiki with files present in this CorpusFiles
            date (str): The dump date of a wiki with files present in this CorpusFiles

        Returns:
            int
        """
        if wiki:
            if date:
                return self._files[(self._files.wiki == wiki) & (self._files.date == date)].shape[0]
            return self._files[self._files.wiki == wiki].shape[0]
        return self._files.shape[0]

    def get_unknown_files(self):
        """Return all files which don't seem to be related to any wiki.

        Returns:
            A list of file names
        """
        return list(self._files[self._files.wiki.isna()].name)

    def get_unknown_wiki_files(self, wiki):
        """Return a list of files which belong to a wiki but lack dump date.

        Args:
            wiki (str): The name of a wiki with files present in this CorpusFiles
        Returns:
            A list of file names
        """
        return list(self._files[(self._files.wiki == wiki) & self._files.date.isna()].name)

    def get_unknown_dump_files(self, wiki, date):
        """Return a list of unknown files which belong to a wiki and dump.

        These files are labelled with a wiki name and dump date but don't fit the known
        Wikimedia dump naming conventions.

        Args:
            wiki (str): The name of a wiki with files present in this CorpusFiles
            date (str): The dump date of a wiki with files present in this CorpusFiles
        Returns:
            A list of file names
        """
        return list(self._files[(self._files.wiki == wiki)
                                & (self._files.date == date)
                                & ~self._files[self._dump_columns].any(axis=1)].name)

    def get_checksum_files(self, wiki, date):
        """Return a list of all checksum files which belong to a wiki and dump.

        Args:
            wiki (str): The name of a wiki with files present in this CorpusFiles
            date (str): The dump date of a wiki with files present in this CorpusFiles
        Returns:
            A list of checksum file names
        """
        return list(self._files[(self._files.wiki == wiki)
                                & (self._files.date == date)
                                & self._files.checksum].name)


class CorpusTracker:  # pylint: disable=too-many-instance-attributes
    """Tracks Wikimedia corpus _files locally and online.

    A class to track what Wikimedia _files exist locally, as well as what _files
    are available online. While the defaults of this class are set to pull the
    articles from the English wikipedia, everything is parameterized to make it
    simple to pull whatever parts you chose from any wikimedia hosted wiki.
    """

    # Defer to online lists, but this is for offline file name parsing
    known_wikis = []
    with resources.open_text("data", "known_wikis.txt") as fin:
        for w in fin:
            known_wikis.append(w.rstrip())

    def __init__(self, local_dirs=None, url="http://dumps.wikimedia.org", wiki_name="enwiki",  # noqa: E501 pylint: disable=too-many-arguments,line-too-long
                 date="latest", online=True, verbose=True):
        """Object initialization.

        Initialization scans the given local directories for pre-existing wikimedia _files.
        It then looks online to get a list of which wikis are available.

        Args:
            local_dirs (list): Local directories containing local corpus _files (Default: cwd)
            wiki_name (string): which wiki to look at (Default: "enwiki")
            date (string): which date to inspect (Default: "latest"), format "YYYYMMDD"
            online (bool): Object instantiation should check for online status (Default: True)
            verbose (bool): Print summary stats after instantiation (Default: True)
        """
        self.local_files = CorpusFiles()
        self.offline_wikis = None
        self.online = False
        self.online_wikis = None
        # check and scan local dirs
        self.local_dirs = ['.']
        if local_dirs:
            self.local_dirs = local_dirs
        self._check_dir_permissions()
        for path in self.local_dirs:
            self._scan_dir_for_file_sets(path)
        self.offline_wikis = self.local_files.get_wikis()
        # set online status
        self.url = url
        if online:
            self.online = self.is_online()
        if self.online:
            self.online_wikis = self._get_online_wikis()
        self.wiki_name = wiki_name
        # self.url = url + '/' + self.wiki_name
        self.date = date
        self.date_loc = None
        self.date_err = None
        self.online_dates = None
        self.verbose = verbose
        if self.verbose:
            self.print_status(False)

    def is_online(self):
        """Check if wikimedia can be reached.

        See if we are working in offline mode. It eats the ConnectionError exception,
        as this is assumed to be a solid indicator of being offline. All other exceptions imply you
        are online but the server has problems. The user should know about that.

        Returns:
            bool: Was the object able to get a response from wikimedia.
        Raises:
            HTTPError: unsuccessful status code returned, but hey, the server is up!
            Timeout: a request timeout
            TooManyRedirects: HTTP request exceeds the configured number of maximum redirections
        """
        try:
            resp = requests.get(self.url)
            if resp.ok:
                resp.raise_for_status()
        except requests.exceptions.ConnectionError:
            return False
        return True

    def _get_online_wikis(self):
        """Parse backup-index.html page for a list of all available wikis."""
        index = requests.get(self.url + "/backup-index.html").text
        soup_index = BeautifulSoup(index, "lxml")
        wikis = []
        for list_item in soup_index.find_all("li"):
            a_href = list_item.find_all('a')
            if a_href:
                wikis.extend(a_href[0].contents)
        return wikis

    def _check_dir_permissions(self):
        """Confirm R/Q permission of each directory."""
        for path in self.local_dirs:
            if not os.path.exists(path):
                raise FileNotFoundError
            if not os.path.isdir(path):
                raise NotADirectoryError
            if not os.access(path, os.R_OK | os.W_OK):
                raise PermissionError

    def _scan_dir_for_file_sets(self, path):
        """Walk through a directory and identify all wiki related files."""
        # get a list of just the _files
        file_list = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]

        # get a list of known wikis
        wiki_names = CorpusTracker.known_wikis
        if self.online and self.online_wikis:
            wiki_names = self.online_wikis

        self.local_files.add_files(path, file_list, wiki_names)

    def print_status(self, verbose=True):
        """List what the tracker is aware of.

        Brief (verbose == False) mode, is used for the summary after initilization.

        Args:
            verbose (bool): Print long or brief form of summary.
        """
        tab = '\t'
        print("Local:")
        if verbose:
            print(tab + "Dirs: " + self.local_dirs)
        print(tab + "Number of files: " + str(self.local_files.get_file_count()))
        print(tab + "Number of wikis with files: " + str(len(self.offline_wikis)))
        if not verbose:
            for wiki in self.offline_wikis:
                print(tab + tab + wiki + ":")
                dumps = self.local_files.get_dumps(wiki)
                for dump in dumps:
                    count = self.local_files.get_file_count(wiki, dump)
                    print(tab + tab + tab + dump + ": " + str(count) + " files")
