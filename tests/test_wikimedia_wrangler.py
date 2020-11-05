#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=redefined-outer-name,invalid-name
"""Tests for `Wikimedia` module."""
import pytest
import requests
import requests_mock

from corpus_wrangler import wikimedia as ww


# ---------------------------------------
# Test fixtures
# ---------------------------------------
@pytest.fixture
def request_mock():
    """Simplify test functions by removing another context."""
    with requests_mock.Mocker() as mocker:
        yield mocker


# ---------------------------------------
# Utility functions
# ---------------------------------------
def set_wikimedia_pages(mocker, wiki_list_file="./tests/data/wikimedia_wikilist.html",
                        dump_list_file="./tests/data/dump_list.html",
                        dump_status_file="./tests/data/dumpstatus.json"):
    """Read in test data needed to successfully initialize CorporaTracker in online mode."""
    with open(wiki_list_file, "rt") as w_l_f, \
         open(dump_list_file, "rt") as d_l_f, \
         open(dump_status_file, "rt") as d_s_f:
        mocker.get("http://dumps.wikimedia.org/", text="Success")
        mocker.get("http://dumps.wikimedia.org/backup-index.html", text=w_l_f.read())
        mocker.get("http://dumps.wikimedia.org/enwiki", text=d_l_f.read())
        dump_status_json = d_s_f.read()
        for uri in ["http://dumps.wikimedia.org/enwiki/20201020/dumpstatus.json",
                    "http://dumps.wikimedia.org/enwiki/20201001/dumpstatus.json",
                    "http://dumps.wikimedia.org/enwiki/20200920/dumpstatus.json",
                    "http://dumps.wikimedia.org/enwiki/20200901/dumpstatus.json",
                    "http://dumps.wikimedia.org/enwiki/20200820/dumpstatus.json",
                    "http://dumps.wikimedia.org/enwiki/20200801/dumpstatus.json",
                    "http://dumps.wikimedia.org/enwiki/20200720/dumpstatus.json"]:
            mocker.get(uri, text=dump_status_json)


# ---------------------------------------
# Local file checks
# ---------------------------------------
def test_local_dir_perm_ok(fs):
    """Permissions check, handles dirs with R/W permissions."""
    fs.create_dir("/data1", perm_bits=0o777)
    fs.create_dir("/data2", perm_bits=0o777)

    wct = ww.CorporaTracker(local_dirs=["/data1", "/data2"], online=False, verbose=False)
    assert not wct.online


def test_local_dir_perm_wrong(fs):
    """Permissions check, catches dir without R/W permissions."""
    fs.create_dir("/data1", perm_bits=0o777)
    fs.create_dir("/data2", perm_bits=0o444)  # read-only

    with pytest.raises(PermissionError):
        ww.CorporaTracker(local_dirs=["/data1", "/data2"], online=False, verbose=False)


def test_local_dir_bad_path(fs):
    """Permissions check, catches nonsense path."""
    fs.create_dir("/data1", perm_bits=0o777)
    fs.create_dir("/data2", perm_bits=0o777)

    with pytest.raises(FileNotFoundError):
        ww.CorporaTracker(local_dirs=["/data1", "foo"], online=False, verbose=False)


def test_local_dir_scan(fs):
    """Scan of local directories works across directories."""
    fs.create_file("/data1/enwiki-20201001-md5sums.txt")
    fs.create_file("/data1/enwiki-20201020-md5sums.txt")
    fs.create_file("/data2/frwiki-20201020-md5sums.txt")
    fs.create_file("/data2/enwiki-20201020-md5sums.txt")

    wct = ww.CorporaTracker(local_dirs=["/data1", "/data2"], online=False, verbose=False)
    assert len(wct.get_local_wikis()) == 2
    assert len(wct.get_local_dumps("enwiki")) == 2


def test_wiki_file_identification(fs):
    """Parsing of wiki file names catches all known names."""
    fs.add_real_file("./tests/data/local_filelist_enwiki.txt")
    with open("./tests/data/local_filelist_enwiki.txt", "rt") as filelist:
        for filename in filelist:
            fs.create_file("/data/" + filename.rstrip())
    wct = ww.CorporaTracker(local_dirs=["/data"], online=False, verbose=False)
    assert wct.get_local_file_count() == 1836
    assert len(wct.offline_wikis) == 1
    assert len(wct.get_unknown_files()) == 3
    assert len(wct.get_unknown_files("enwiki")) == 2
    assert len(wct.get_unknown_files("enwiki", "20201001")) == 1
    assert len(wct.get_local_checksum_files("enwiki", "20201001")) == 2


# ---------------------------------------
# Detect whether online or offline
# ---------------------------------------
def test_is_online_offline(request_mock):
    """Instantiation correctly detects offline status."""
    request_mock.get("http://dumps.wikimedia.org", exc=requests.exceptions.ConnectTimeout)

    wct = ww.CorporaTracker(verbose=False)
    assert not wct.is_online()


def test_is_online_online(request_mock):
    """Instantiation correctly detects online status."""
    set_wikimedia_pages(request_mock)

    wct = ww.CorporaTracker(verbose=False)
    assert wct.is_online()


def test_is_online_problem(request_mock):
    """Instantiation correctly throws exceptions for unexpected connectivity issue."""
    request_mock.get("http://dumps.wikimedia.org", exc=requests.exceptions.HTTPError)

    with pytest.raises(requests.exceptions.HTTPError):
        ww.CorporaTracker(url="http://dumps.wikimedia.org", verbose=False)


# -----------------------------------------
# Get a list of wiki names online and offline
# -----------------------------------------
def test_get_online_wikis(request_mock):
    """Parse names of available online wikis."""
    set_wikimedia_pages(request_mock)

    wct = ww.CorporaTracker(verbose=False)
    assert "enwiki" in wct.online_wikis
    assert len(wct.online_wikis) == 5


def test_get_online_wikis_mid_dump(request_mock):
    """Parsing the list of online wikis must handle the additional text during dump days."""
    set_wikimedia_pages(request_mock, wiki_list_file="./tests/data/wikimedia_wikilist_middump.html")

    wct = ww.CorporaTracker(verbose=False)
    assert "enwiki" in wct.online_wikis
    assert len(wct.online_wikis) == 5


def test_get_offline_wikis(fs):
    """Local wiki names are found."""
    fs.create_file("/data1/frwiki-20201020-md5sums.txt")
    fs.create_file("/data2/enwiki-20201020-md5sums.txt")

    wct = ww.CorporaTracker(local_dirs=["/data1", "/data2"], online=False, verbose=False)
    assert len(wct.offline_wikis) == 2
    assert "enwiki" in wct.offline_wikis
