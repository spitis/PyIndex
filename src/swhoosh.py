from whoosh.index import Index as whooshIndex
from whoosh.reading import IndexReader as whooshIndexReader
from whoosh.matching import Matcher as whooshMatcher
import bisect
from .manager import StaticIndexManager
from .postings import Postings
try:
    from .svbcomp import load2
except ImportError:
    from ._compress import load2

class Index(StaticIndexManager, whooshIndex):
    """A thin wrapper over StaticIndexManager that also subclasses the whoosh index"""

    def is_empty(self):
        return self._lastDocId == -1

    def doc_count(self):
        return self._lastDocId + 1

    def reader(self):
        return IndexReader(self)

    def writer(self):
        raise NotImplementedError("Use the add_documents methods on this object instead.")

class IndexReader(whooshIndexReader):

    def __init__(self, index):
        self.schema = index._schema
        self.index = index

    def __contains__(self, term):
        fieldname, text = term
        if fieldname not in self.index._idxFields:
            return False
        return text in self.index._idx[fieldname]._termDict

    def __iter__(self):
        raise NotImplementedError('Not sure what this fn is for')

    def iter_from(self, fieldname, text):
        raise NotImplementedError('Not sure what this fn is for')

    def lexicon(self, fieldname):
        if fieldname not in self.index._idxFields:
            return None
        return self.index._idx[fieldname]._termDict.keys()

    def field_terms(self, fieldname):
        """Not sure how this is different from lexicon, so just returning lexicon"""
        return self.lexicon(fieldname)

    def stored_fields(self, docnum):
        return self.index._storedIdx[docnum]

    # N (total number of documents)
    def doc_count_all(self):
        return self.index.doc_count()

    def doc_count(self):
        return self.index.doc_count()

    # N_t (number of documents a given term appears in)
    def doc_frequency(self, fieldname, text):
        """num of docs term appears in"""
        try:
            return self.index._idx[fieldname]._termDict[text][0][1]
        except KeyError:
            return 0

    def frequency(self, fieldname, text):
        """total term frequency in collection"""
        raise self.index._idx[fieldname]._termDict[text][0][0]

    # l_d (length of document, given some field)
    def doc_field_length(self, docnum, fieldname, default=0):
        if docnum in self.index._storedIdx:
            res = self.index._storedIdx[docnum].get('dl_'+fieldname)
        return res or default

    # l_avg
    def avg_field_length(self, fieldname):
        """average length of each document in terms of this field"""
        return self.field_length(fieldname) / self.doc_count_all()

    def field_length(self, fieldname):
        """Total number of tokens in the field, across all documents"""
        return self.index._totalDocLen.get(fieldname)

    def max_field_length(self, fieldname):
        """Not sure what this is for -- it's never called"""
        raise NotImplementedError

    def term_info(self, fieldname, text):
        return MiniTermInfo(self.index._idx[fieldname].terminfo(text))

    def has_vector(self, docnum, fieldname):
        return False

    def vector(self, docnum, fieldname, format_=None):
        raise NotImplementedError

    def postings(self, fieldname, text, scorer=None):
        """returns a matcher"""
        return Matcher(self.index._idx[fieldname]._postings(text), scorer)

class Matcher(whooshMatcher):

    def __init__(self, postings, scorer):
        self._postings = postings
        self._scorer = scorer
        self._current = 0 # this is not a docId
        self._last = len(postings) - 1

    def is_active(self):
        return self._current <= self._last

    def id(self):
        return self._postings[self._current][0]

    def next(self):
        self._current += 1

    def value(self):
        return self.postings[self._current]

    def value_as(self, astype):
        if astype == 'positions':
            return load2(self.postings[self._current][2])
        elif astype == 'frequency':
            return self.postings[self._current][1]
        else:
            raise NotImplementedError('Value as ' + str(astype) + ' is not implemented!')

    def value_as_positions(self):
        return load2(self._postings[self._current][2])

    def weight(self):
        """weight seems to mean term frequency in current document"""
        return self._postings[self._current][1]

    def score(self):
        return self._scorer.score(self)

    def skip_to(self, id):
        self._current = bisect.bisect_left(self._postings, (id,))

    def supports_block_quality(self):
        return False

    def max_quality(self):
        if self._scorer:
            return self._scorer.max_quality()
        else:
            raise NotImplementedError

    # Not whoosh methods
    def first_doc(self):
        return self._postings[0]

    def last_doc(self):
        return self._postings[-1]

    def next_doc(self):
        idx = self._current
        self._current += 1
        return self._postings[idx]

    def prev_doc(self):
        self._current -= 1
        return self._postings[self._current]

    def all_docs(self):
        self._current = 0
        while self._current <= self._last:
            yield self._postings[self._current]
            self._current += 1

    def reset(self):
        self._current = -1

class MiniTermInfo(object):
    """For compatibility with whoosh"""

    def __init__(self, term_info):
        self._term_info = term_info

    def max_weight(self):
        return self._term_info[2]

    def min_length(self):
        return self._term_info[3]
