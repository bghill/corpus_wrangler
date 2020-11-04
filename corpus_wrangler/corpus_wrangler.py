# -*- coding: utf-8 -*-
"""The base classes for creating a corpus wrangler.

A set of classes to track, download, and parse a corpus.

    See the wikimedia module for an implementation example.

"""


class DownloadTargets:
    """An Iterator class."""

    def __init__(self, targets):
        """Initialize object."""
        self._url_list = targets
        self._index = 0

    def __next__(self):
        """Return the next target to download."""
        if self._index < len(self._url_list):
            result = self._url_list[self._index]
            self._index += 1
            return result
        return StopIteration

    def get(self):  # pylint: disable=no-self-use
        """Return a pointer to a set of files."""
        return
