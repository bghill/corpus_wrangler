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
from hurry.filesize import size as pretty_size


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


def _check_dir_permissions(dirs):
    """Confirm R/Q permission of each directory."""
    for path in dirs:
        if not os.path.exists(path):
            raise FileNotFoundError
        if not os.path.isdir(path):
            raise NotADirectoryError
        if not os.access(path, os.R_OK | os.W_OK):
            raise PermissionError


def _parse_dump_info(name):
    """Return the name of the wiki this file comes from as well as the dump date."""
    name_parts = name.split('-', 2)
    if name_parts[0] in _known_wikis:
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


_file_info_keys = ["path", "name", "file_type", "size", "wiki", "date", "description"]


def _get_file_info(pathname):
    size = os.path.getsize(pathname)
    [path, file_name] = os.path.split(pathname)
    [name, ext] = os.path.splitext(file_name)
    wiki, date, description = _parse_dump_info(name)
    return path, name, ext, size, wiki, date, description


_SHORT_SUMMARY_TEMPLATE = """\
Corpus name:  {name}
Dirs:  {paths}
Totals:  {file_count} files, {total_size}"""


class FileSet:  # pylint: disable=too-few-public-methods
    """A record of a set of files.

    This is a base class for specific records for a collections of files.
    """

    _id_columns_types = {"path": "string", "name": "string", "file_type": "string", "size": "UInt64"}
    _id_columns = list(_id_columns_types)

    def __init__(self):
        """Initialize object."""
        self._files = None

    def get_file_count(self):
        """Return the number of files tracked by this set."""
        return self._files.shape[0]

    def _add_rows(self, file_rows):
        """Append a list of dicts each describing a file to the existing record."""
        if self._files is None:
            self._files = pd.DataFrame.from_dict(file_rows, orient='columns')
        else:
            self._files = self._files.append(pd.DataFrame.from_dict(file_rows, orient='columns'))

    def _summary_stats(self):
        """Collect summary statistics for this set of files."""
        paths = list(self._files.path.unique().dropna())
        count = self.get_file_count()
        size = pretty_size(self._files.size.sum())
        return paths, count, size


class UnknownFiles(FileSet):
    """A record of all files who don't conform to a known wikimedia naming convention."""

    _id_columns_types = {**FileSet._id_columns_types, "wiki": "string", "date": "string"}
    _id_columns = list(_id_columns_types)

    def add_files(self, file_list):
        """Append a list of dicts each describing an unknown file to the existing record.

        Args:
            file_list (list): A list of dicts, each dict contains path, name, file_type, size,
                            wiki name, dump date. Either of the last two can be empty.
        """
        for file_dict in file_list:
            del file_dict["description"]
        self._add_rows(file_list)

    def get_files(self, wiki=None, date=None):
        """Return a list of files.

        Can return either a list of all unknown files, or if a wiki name is given, only those
        files that start with that wiki name. If both wiki name and dump date are given, the
        list is only unknown files that start with both the wikiname and the dump data.
        """
        if wiki and date:
            return self._files[(self._files.wiki == wiki)
                               & (self._files.date == date)][["path", "name"]].agg('/'.join, axis=1).to_list()
        if wiki:
            return self._files[(self._files.wiki == wiki)][["path", "name"]].agg('/'.join, axis=1).to_list()
        return self._files[["path", "name"]].agg('/'.join, axis=1).to_list()

    def summary(self, verbose=True):
        """Print summary statistics for this set of files."""
        paths, count, size = self._summary_stats()
        print(_SHORT_SUMMARY_TEMPLATE.format(name="Unknown files", paths=paths,
                                             file_count=count, total_size=size))
        if verbose:
            pass


class CorpusFiles(FileSet):
    """A set of files from a wikimedia dump.

    The file set can be either local or a target set of files to be
    downloaded. The set is stored as a yielding iterator, to allow
    for files to be processed as soon as they are downloaded.
    """

    _id_columns_types = FileSet._id_columns_types.copy()
    _id_columns = list(_id_columns_types)

    _description_columns_types = {k: "boolean" for k in ["pages", "stubs", "logging", "abstracts", "sql", "checksum",
                                                         "titles", "namespaces", "articles", "index", "rev_metadata",
                                                         "current", "history", "set", "indexed"]}
    _description_columns = list(_description_columns_types)
    _descriptions_init = {k: False for k in _description_columns}

    def __init__(self, name, file_list, unknowns):
        """Initialize the storage for file records."""
        super().__init__()
        self.name = name
        self.add_files(file_list, unknowns)

    def add_files(self, file_list, unknowns):
        """Append a list of dicts each describing an unknown file to the existing record.

        Record the file name, path, size, and file type. If the file follows the naming
        convention of Wikimedia, parse the file name to record the nature of the file
        contents. Files that don't follow the naming convention are added to unknowns.

        Args:
            file_list (list): A list of dicts, each dict contains path, name, file_type, size,
                            wiki name, dump date. Either of the last two can be empty.
            unknowns (UnknownFiles): Unknown file tracker for files that don't match the
                            expected naming convention.
        """
        file_rows = []
        unknown_list = []
        for file_dict in file_list:
            description = file_dict["description"]
            features = CorpusFiles._descriptions_init.copy()
            _parse_wiki_file_names(description, features)
            if any(features.values()):
                del file_dict["wiki"]
                del file_dict["date"]
                del file_dict["description"]
                file_rows.append({**file_dict, **features})
            else:  # these files didn't match anything in the parser - unknowns
                unknown_list.append(file_dict)
        self._add_rows(file_rows)
        self._files = self._files.astype(CorpusFiles._id_columns_types).astype(CorpusFiles._description_columns_types)
        unknowns.add_files(unknown_list)

    def get_checksum_files(self):
        """Return a list of all checksum files in this corpus."""
        return list(self._files[self._files.checksum].name)

    def summary(self, verbose=False):
        """Print summary statistics for this set of files."""
        paths, count, size = self._summary_stats()
        print(_SHORT_SUMMARY_TEMPLATE.format(name=self.name, paths=paths,
                                             file_count=count, total_size=size))
        if verbose:
            pass


class CorporaTracker:
    """Tracks Wikimedia corpus files locally and online.

    A class to track what Wikimedia files exist locally, as well as what files
    are available online. While the defaults of this class are set to pull the
    articles from the English wikipedia, everything is parameterized to make it
    simple to pull whatever parts you chose from any wikimedia hosted wiki.
    """

    def __init__(self, local_dirs=None, url="http://dumps.wikimedia.org",  # pylint: disable=too-many-arguments
                 wiki_name="enwiki", online=True, verbose=True):
        """Object initialization.

        Initialization scans the given local directories for pre-existing wikimedia files.
        It then looks online to get a list of which wikis are available.

        Args:
            local_dirs (list): Local directories containing local corpus files (Default: cwd)
            wiki_name (string): which wiki to look at (Default: "enwiki")
            online (bool): Object instantiation should check for online status (Default: True)
            verbose (bool): Print summary stats after instantiation (Default: True)
        """
        self._unknown_files = UnknownFiles()
        self.online = False
        self.online_wikis = None
        # check and scan local dirs
        if not local_dirs:
            local_dirs = ['.']
        _check_dir_permissions(local_dirs)
        self._local_corpora = Corpora(local_dirs, self._unknown_files)
        # set online status
        self.url = url
        if online:
            self.online = self.is_online()
        if self.online:
            self.online_wikis = self._get_online_wikis()
        self.wiki_name = wiki_name
        # self.url = url + '/' + self.wiki_name
        self.verbose = verbose
        if self.verbose:
            self.print_summary(False)

    def get_local_wikis(self):
        """Return a list of wikis that have at least one file from a dump stored locally."""
        return self._local_corpora.get_wikis()

    def get_local_dumps(self, wiki=None):
        """Return a list of dump dates that have at least one file from that dump stored locally."""
        return self._local_corpora.get_dumps(wiki)

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
        return self._unknown_files.get_files(wiki, date)

    def get_local_dirs(self):
        """Return a  list of the directories scanned for files."""
        return self._local_corpora.get_dirs()

    def get_local_checksum_files(self, wiki=None, date=None):
        """Return a list of known checksum files.

        With no arguments, this returns a list of all checksum files. With a wiki name,
        this returns all checksums for any dump from that wiki. With a name and d dump
        date, only the checksum files for that dump are listed.

        Args:
            wiki (str): The name of a wiki with files present in this CorpusFiles
            date (str): The dump date of a wiki with files present in this CorpusFiles
        """
        return self._local_corpora.get_checksum_files(wiki, date)

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

    def get_local_file_count(self):
        """Return the total count of files found in the provided local directories.

        This includes both wiki files and unknown files.
        """
        return self._local_corpora.get_file_count() + self._unknown_files.get_file_count()

    def print_summary(self, verbose=True):
        """List what the tracker is aware of.

        Brief (verbose == False) mode, is used for the summary after initialization.

        Args:
            verbose (bool): Print long or brief form of summary.
        """
        tab = '\t'
        print("Local:")
        if verbose:
            print(tab + "Dirs: " + self.get_local_dirs())
        print(tab + "Number of files: " + str(self.get_local_file_count()))
        print(tab + "Number of wikis with files: " + str(len(self.get_local_wikis())))
        print(self._local_corpora.summary(verbose))
        print(self._unknown_files.summary(verbose))


class Corpora:
    """Tracks all wiki dumps stored locally."""

    def __init__(self, local_dirs, unknown_files):
        """Object initialization.

        Args:
            local_dirs (list): A list of directory paths
            unknown_files (UnknownFiles): An UnknownFiles record to send mystery files to
        """
        self._corpora = {}
        self._unknown_files = unknown_files
        # check and scan local dirs
        self._local_dirs = local_dirs
        for path in self._local_dirs:
            self._scan_dir_for_file_sets(path)

    def _scan_dir_for_file_sets(self, path):  # pylint: disable=too-many-branches
        """Walk through a directory and identify all wiki related files."""
        # get a list of just the files
        with os.scandir(path) as entries:
            file_list = [_get_file_info(e.path) for e in entries if e.is_file()]
            # exit if no files to process
            if not file_list:
                return

        # Push into a dataframe to simplify querying for all corpus names
        file_df = pd.DataFrame(file_list, columns=_file_info_keys)

        # Look at the mix of unknown files to wiki files
        unknown_cnt = file_df[file_df.date.isna()].shape[0]
        if unknown_cnt == file_df.shape[0]:  # all files are unknown
            self._unknown_files.add_files(file_df.to_dict('records'))
            return

        if unknown_cnt > 0:  # mix of wiki files and unknowns split
            file_df, unknowns_df = [x for _, x in file_df.groupby(file_df.wiki.isna() | file_df.date.isna())]
            # File obvious unknown files into UnknownFiles
            self._unknown_files.add_files(unknowns_df.to_dict('records'))

        # Create storage for each wiki
        for wiki in list(file_df.groupby(["wiki"]).groups):
            if wiki not in self._corpora:
                self._corpora[wiki] = {}

        # Create a CorpusFiles for each dump
        for (wiki, date), files in file_df.groupby(["wiki", "date"]):
            if date not in self._corpora[wiki]:
                self._corpora[wiki][date] = CorpusFiles(wiki + '-' + date, files.to_dict('records'),
                                                        self._unknown_files)
            else:
                self._corpora[wiki][date].add_files(files.to_dict('records'), self._unknown_files)

    def get_wikis(self):
        """Return a list of wikis that have at least one file from a dump stored locally."""
        return list(self._corpora)

    def get_dumps(self, wiki):
        """Return a list of dump dates that have at least one file from that dump stored locally."""
        if wiki:
            return list(self._corpora[wiki])

        dumps = []
        for wiki_name in self._corpora:
            dumps.append(list(self._corpora[wiki_name]))
        return dumps

    def get_dirs(self):
        """Return a list of the directories scanned for files."""
        return self._local_dirs

    def get_file_count(self, wiki=None, date=None):
        """Return a count of files detected."""
        count = 0
        if wiki and date:
            return self._corpora[wiki][date].get_file_count()
        if wiki:
            for date_name in self._corpora[wiki]:
                count += self._corpora[wiki][date_name].get_file_count()
        else:
            for wiki_name in self._corpora:
                for date_name in self._corpora[wiki_name]:
                    count += self._corpora[wiki_name][date_name].get_file_count()
        return count

    def summary(self, wiki=None, date=None, verbose=True):
        """Return a string summarizing all the local identified wiki files."""
        status = ""
        if wiki and date:
            return self._corpora[wiki][date].summary(verbose)
        if wiki:
            for date_name in self._corpora[wiki]:
                status += self._corpora[wiki][date_name].summary(verbose)
        else:
            for wiki_name in self._corpora:
                for date_name in self._corpora[wiki_name]:
                    status += self._corpora[wiki_name][date_name].summary(verbose)
        return status

    def get_checksum_files(self, wiki=None, date=None):
        """Return a list of checksum files."""
        files = []
        if wiki and date:
            return self._corpora[wiki][date].get_checksum_files()
        if wiki:
            for date_name in self._corpora[wiki]:
                files.append(self._corpora[wiki][date_name].get_checksum_files())
        else:
            for wiki_name in self._corpora:
                for date_name in self._corpora[wiki_name]:
                    files.append(self._corpora[wiki_name][date_name].get_checksum_files())
        return files
