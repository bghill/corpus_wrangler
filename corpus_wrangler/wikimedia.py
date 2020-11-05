# -*- coding: utf-8 -*-
"""A corpus wrangler for Wikimedia.

A set of classes to track, download, and parse Wikimedia dumps.

  Typical usage example:

    from corpus_wrangler import wikimedia as ww

    wiki_ct = ww.CorporaTracker()

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

import pandas as pd
import requests
from bs4 import BeautifulSoup


# ----------------------------------------------------------
# Module  static utilities and global resources
# ----------------------------------------------------------

# When offline, we need a list of known wiki names
_known_wikis = []
with resources.open_text("data", "known_wikis.txt") as fin:
    for w in fin:
        _known_wikis.append(w.rstrip())


def _unique_list(df_col):
    """Return a list of unique values without NA."""
    return list(df_col.unique().dropna())


def _parse_dump_info(name, wiki_names):
    """Return the name of the wiki this file comes from as well as the dump date."""
    name_parts = name.split('-', 2)
    if name_parts[0] in wiki_names:
        if len(name_parts) > 1 and len(name_parts[1]) == 8 and name_parts[1].isnumeric():
            if len(name_parts) == 3:
                return name_parts[0], name_parts[1], name_parts[2]
            return name_parts[0], name_parts[1], None
        return name_parts[0], None, None
    return None, None, None


def _parse_wiki_file_names(name, description):  # pylint: disable=too-many-branches,too-many-statements
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
                    if re.match(r"^pages-articles-multistream-index(\d|\d\d)\.txt-p\d+p\d+$", name):
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


class CorpusFiles:
    """A set of files from a wikimedia dump.

    The file set can be either local or a target set of files to be
    downloaded. The set is stored as a yielding iterator, to allow
    for files to be processed as soon as they are downloaded.
    """

    _description_columns_types = {k: "boolean" for k in ["pages", "stubs", "logging", "abstracts", "sql", "checksum",
                                                         "titles", "namespaces", "articles", "index", "rev_metadata",
                                                         "current", "history", "set", "indexed"]}
    _description_columns = list(_description_columns_types.keys())
    _descriptions_init = {k: False for k in _description_columns}
    _id_columns_types = {"name": "string", "path": "string", "size": "UInt64", "file_type": "string"}
    _id_columns = list(_id_columns_types.keys())

    def __init__(self, name, file_list, unknown_list):
        """Initialize the storage for file records."""
        self._files = None
        self.name = name
        self.add_files(file_list, unknown_list)

    def add_files(self, file_list, unknown_list):
        """Process a list of file path names and add each to the CorpusFile records.

        Record the file name, path, size, and file type. If the file follows the naming
        convention of Wikimedia, parse the file name to record the nature of the file
        contents. Files that don't follow the naming convention are added to the unknown_list.

        Args:
            file_list (list): A list of file names (including paths) to be processed
            unknown_list (list): A list which will be appended with the names of file that
            don't match the expected naming convention.
        """
        file_rows = []
        for file in file_list:
            size = os.path.getsize(file)
            # get file path, name, extension
            [path, file_name] = os.path.split(file)
            [name, ext] = os.path.splitext(file_name)
            try:
                [_, _, file_descript] = name.split('-', 2)
            except ValueError:
                file_descript = ''
            row = {"name": file_name, "path": path, "size": size, "file_type": ext}
            row.update(CorpusFiles._descriptions_init.copy())
            _parse_wiki_file_names(file_descript, row)
            # Detect files who didn't match anything in the name parser
            known_name_fmt = False
            for k in CorpusFiles._description_columns:
                known_name_fmt |= row[k]
            if known_name_fmt:
                file_rows.append(row)
            else:
                unknown_list.append(file)

        if self._files is None:
            self._files = pd.DataFrame.from_dict(file_rows, orient='columns')
        else:
            self._files.append(pd.DataFrame.from_dict(file_rows, orient='columns'))
        self._files = self._files.astype(CorpusFiles._id_columns_types).astype(CorpusFiles._description_columns_types)

    def get_file_count(self):
        """Return the number of files tracked in this corpus."""
        return self._files.shape[0]

    def get_checksum_files(self):
        """Return a list of all checksum files in this corpus."""
        return list(self._files[self._files.checksum].name)


class CorporaTracker:  # pylint: disable=too-many-instance-attributes
    """Tracks Wikimedia corpus files locally and online.

    A class to track what Wikimedia files exist locally, as well as what files
    are available online. While the defaults of this class are set to pull the
    articles from the English wikipedia, everything is parameterized to make it
    simple to pull whatever parts you chose from any wikimedia hosted wiki.
    """

    def __init__(self, local_dirs=None, url="http://dumps.wikimedia.org", wiki_name="enwiki",  # noqa: E501 pylint: disable=too-many-arguments,line-too-long
                 date="latest", online=True, verbose=True):
        """Object initialization.

        Initialization scans the given local directories for pre-existing wikimedia files.
        It then looks online to get a list of which wikis are available.

        Args:
            local_dirs (list): Local directories containing local corpus files (Default: cwd)
            wiki_name (string): which wiki to look at (Default: "enwiki")
            date (string): which date to inspect (Default: "latest"), format "YYYYMMDD"
            online (bool): Object instantiation should check for online status (Default: True)
            verbose (bool): Print summary stats after instantiation (Default: True)
        """
        self.corpora = {}
        self.unknown_files = {"orphans": []}
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
        self.offline_wikis = list(self.corpora.keys())
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

    def get_local_wikis(self):
        """Return a list of wikis that have at least one file from a dump stored locally."""
        return list(self.corpora.keys())

    def get_local_dumps(self, wiki=None):
        """Return a list of dump dates that have at least one file from that dump stored locally."""
        if wiki:
            return list(self.corpora[wiki].keys())
        dumps = []
        for wiki_name in self.corpora:
            dumps.append(list(self.corpora[wiki_name].keys()))
        return dumps

    def get_unknown_files(self, wiki=None, date=None):
        """Return a list of files that don't fit the wikimedia naming convention.

        An unknown file is any local file that don't fit the Wikimedia naming convention.
        With no arguments, this returns a list of all unknown local files. When, a
        wiki name is provided, this return a list of all unknown local files whose name
        at least starts with the wiki name. When a wiki name and dump date are provided,
        this returns a list of all files which start with the wiki name and dump date,
        but otherwise don't fit the Wikimedia dump naming conventions.

        Args:
            wiki (str): The name of a wiki with files present in this CorpusFiles
            date (str): The dump date of a wiki with files present in this CorpusFiles
        """
        files = []
        if wiki and date:
            return self.unknown_files[wiki][date]
        if wiki:
            for dump in self.unknown_files[wiki]:
                files.append(self.unknown_files[wiki][dump])
            return files

        for wiki_name in self.unknown_files:
            if wiki_name == "orphans":
                files.append(self.unknown_files[wiki_name])
            else:
                for dump in self.unknown_files[wiki_name]:
                    files.append(self.unknown_files[wiki_name][dump])
        return files

    def get_local_checksum_files(self, wiki=None, date=None):
        """Return a list of known checksum files.

        With no arguments, this returns a list of all checksum files. With a wiki name,
        this returns all checksums for any dump from that wiki. With a name and d dump
        date, only the checksum files for that dump are listed.

        Args:
            wiki (str): The name of a wiki with files present in this CorpusFiles
            date (str): The dump date of a wiki with files present in this CorpusFiles
        """
        files = []
        if wiki and date:
            return self.corpora[wiki][date].get_checksum_files()
        if wiki:
            for dump in self.corpora[wiki]:
                files.append(self.corpora[wiki][dump].get_checksum_files())
            return files

        for wiki_name in self.corpora:
            for dump in self.corpora[wiki_name]:
                files.append(self.corpora[wiki][dump])
        return files

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

    def _scan_dir_for_file_sets(self, path):  # pylint: disable=too-many-branches
        """Walk through a directory and identify all wiki related files."""
        # get a list of just the files
        with os.scandir(path) as entries:
            file_list = [e.path for e in entries if e.is_file()]
        # file_list = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]

        # get a list of known wikis
        wiki_names = _known_wikis
        if self.online and self.online_wikis:  # These won't be available yet
            wiki_names = self.online_wikis

        # sort the files into corpus lists
        corpus_lists = {}
        for file_path in file_list:
            [_, file] = os.path.split(file_path)
            (wiki, date, name) = _parse_dump_info(file, wiki_names)
            if wiki and date and name:
                if wiki not in corpus_lists:
                    corpus_lists[wiki] = {}
                    corpus_lists[wiki][date] = [file_path]
                elif date not in corpus_lists[wiki]:
                    corpus_lists[wiki][date] = [file_path]
                else:
                    corpus_lists[wiki][date].append(file_path)
                # Create an unknown list for each new dump date
                if wiki not in self.unknown_files:
                    self.unknown_files[wiki] = {date: []}
                elif date not in self.unknown_files[wiki]:
                    self.unknown_files[wiki][date] = []
            elif wiki and date:
                if wiki not in self.unknown_files:
                    self.unknown_files[wiki] = {date: [file_path]}
                elif date not in self.unknown_files[wiki]:
                    self.unknown_files[wiki][date] = [file_path]
                else:
                    self.unknown_files[wiki][date].append(file_path)
            elif wiki:
                if wiki not in self.unknown_files:
                    self.unknown_files[wiki] = {"orphans": [file_path]}
                else:
                    self.unknown_files[wiki]["orphans"].append(file_path)
            else:
                self.unknown_files["orphans"].append(file_path)

        # Add files to the relevant CorpusFiles objects
        for wiki in corpus_lists:
            for date in corpus_lists[wiki]:
                if wiki not in self.corpora:
                    self.corpora[wiki] = {}
                    self.corpora[wiki][date] = CorpusFiles(wiki + '-' + date,
                                                           corpus_lists[wiki][date],
                                                           self.unknown_files[wiki][date])
                elif date not in self.corpora[wiki]:
                    self.corpora[wiki][date] = CorpusFiles(wiki + '-' + date,
                                                           corpus_lists[wiki][date],
                                                           self.unknown_files[wiki][date])
                else:
                    self.corpora[wiki][date].add_files(corpus_lists[wiki][date],
                                                       self.unknown_files[wiki][date])

    def get_local_file_count(self):
        """Return the total count of files found in the provided local directories.

        This includes both wiki files and unknown files.
        """
        count = 0
        # All files tracked across corpora
        for wiki in self.corpora:
            for date in self.corpora[wiki]:
                count += self.corpora[wiki][date].get_file_count()
        for wiki in self.unknown_files:
            if wiki == "orphans":
                count += len(self.unknown_files[wiki])
            else:
                for files in self.unknown_files[wiki].values():
                    count += len(files)
        return count

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
        print(tab + "Number of files: " + str(self.get_local_file_count()))
        print(tab + "Number of wikis with files: " + str(len(self.offline_wikis)))
        if not verbose:
            for wiki in self.corpora:
                print(tab + tab + wiki + ":")
                for dump in self.corpora[wiki]:
                    count = self.corpora[wiki][dump].get_file_count()
                    print(tab + tab + tab + dump + ": " + str(count) + " files")
