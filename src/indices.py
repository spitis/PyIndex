import whoosh, pickle, os, sys, heapq, array
from collections import defaultdict
from .postings import Postings
try:
    from .svbcomp import *
except ImportError:
    from ._compress import *

"""
Indexes represent an index over a single field (e.g., the body text of a document, or the document's
abstract). Indexes have a _termDict that maps from terms to (terminfo, postings) tuples.

Current contents of terminfo is a 4-tuple:
0. overall frequency of the term,
1. num_docs term appears in,
2. maximum frequency of term in all docs the term appears in,
3. minimum document length of all docs the term appears in

Postings depend on the index format:
    * existence: 1-tuple of (docId)
    * frequency: 2-tuple of (docId, frequency)
    * positions: 3-tuple of (docId, frequency, compressedPositions)

You uncompress compressedPositions with the "load2" function. It will uncompressed to an
    array.array('I',[...])

Documents are always added to a RAMIndex, never directly to a DiskIndex. The RAMIndex is then
either written to disk or merged with one or more DiskIndex with the three utility functions:
    * BaseIndex.write_index_to_disk
    * BaseIndex.merge_indices_ordered
    * BaseIndex.merge_indeces_unordered
"""

DEFAULT_MERGE_BUFFERSIZE = 2*1024*1024

class BaseIndex():
    """Abstract class for Indexes
    Subclasses must have attribute:
    * _termDict"""

    def __contains__(self, term):
        return term in self._termDict

    def postings(self, term):
        """returns postings for a term (term should already be analyzed (e.g., stemmed))"""
        return Postings(self._postings(term))

    def _postings(self, term):
        raise NotImplementedError

    def terminfo(self, term):
        """
        term info is a tuple (see top for contents)
        """
        return self._termDict[term][0]

    def _terminfo_with_postings(self, term):
        """returns tuple of (terminfo, postings)"""
        return self._termDict[term]

    def _deleteTerm(self, term):
        """This is for merging - deletes a term. Note: will not clear ondisk postings."""
        del self._termDict[term]

    @staticmethod
    def write_index_to_disk(rIdx, indexFilePrefix, bufferSize = DEFAULT_MERGE_BUFFERSIZE):
        """expects a RAMIndex; returns a DiskIndex"""
        postingsFile = open(indexFilePrefix + '.postings', 'wb', buffering=bufferSize)
        cursor = 0
        newTermDict = {}
        for term, terminfo in rIdx._termDict.items():
            pickledPostings = pickle.dumps(terminfo[1])
            size = len(pickledPostings)
            newTermDict[term] = (terminfo[0], cursor, size)
            cursor += size
            postingsFile.write(pickledPostings)

        postingsFile.close()

        with open(indexFilePrefix + '.termdict', 'wb') as f:
            pickle.dump(newTermDict, f)

        return DiskIndex(indexFilePrefix)

    @staticmethod
    def merge_indices_ordered(indexFilePrefix, indices, bufferSize = DEFAULT_MERGE_BUFFERSIZE):
        """Writes the indices to disk in order. Deletes the old indices.
        Faster than merging unordered indices, because it assumes that indices are in
        order (e.g., all postings in the first index have a lesser docId than any
        posting in the second index)) -- it's the caller's job to make sure the docIds
        are in order."""
        postingsFile = open(indexFilePrefix + '.postings.MERGED', 'wb', buffering=bufferSize)
        cursor = 0
        newTermDict = {}
        closed_indices = []
        while indices:
            for term in indices[0]._termDict.keys():
                ti, p = indices[0]._terminfo_with_postings(term)
                (f, c, max_f, min_l) = ti
                for idx in indices[1:]:
                    if term in idx:
                        ti_, p_inc = idx._terminfo_with_postings(term)
                        (f_, c_, max_f_, min_l_) = ti_
                        f += f_
                        c += c_
                        max_f = max(max_f, max_f_)
                        min_l = min(min_l, min_l_)
                        p += p_inc
                        idx._deleteTerm(term)
                pickledPostings = pickle.dumps(p)
                size = len(pickledPostings)
                newTermDict[term] = ((f, c, max_f, min_l), cursor, size)
                cursor += size
                postingsFile.write(pickledPostings)
            i = indices.pop(0)
            if hasattr(i, '_indexFilePrefix'):
                i.close()
                closed_indices.append(i)

        postingsFile.close()

        # delete the old indexes, save the the termdict, and rename the file
        for idx in closed_indices:
            os.remove(idx._indexFilePrefix + '.postings')
            # existence of .postings is guaranteed, but this might fail if it is empty
            if os.path.exists(idx._indexFilePrefix + '.termdict'):
                os.remove(idx._indexFilePrefix + '.termdict')

        with open(indexFilePrefix + '.termdict', 'wb') as f:
            pickle.dump(newTermDict, f)

        os.rename(indexFilePrefix + '.postings.MERGED', indexFilePrefix + '.postings')

        return DiskIndex(indexFilePrefix)

    @staticmethod
    def merge_indices_unordered(indexFilePrefix, indices, bufferSize = DEFAULT_MERGE_BUFFERSIZE):
        """Writes the indices to disk. Deletes the old indices.
        Unordered means that postings will be written to disk in order regardless
        of the order of the indices. If the caller knows that the indexes are ordered
        (e.g., all postings in the first index have smaller docIds than the second,
        then merge_indices_ordered will be faster."""
        postingsFile = open(indexFilePrefix + '.postings.MERGED', 'wb', buffering=bufferSize)
        cursor = 0
        newTermDict = {}
        closed_indices = []
        while indices:
            for term in indices[0]._termDict.keys():
                ti, p  = indices[0]._terminfo_with_postings(term)
                (f, c, max_f, min_l) = ti
                p = [p]
                for idx in indices[1:]:
                    if term in idx:
                        ti_, p_inc = idx._terminfo_with_postings(term)
                        (f_, c_, max_f_, min_l_) = ti_
                        f += f_
                        c += c_
                        max_f = max(max_f, max_f_)
                        min_l = min(min_l, min_l_)
                        p.append(p_inc)
                        idx._deleteTerm(term)
                p = list(heapq.merge(*p))
                pickledPostings = pickle.dumps(p)
                size = len(pickledPostings)
                newTermDict[term] = ((f, c, max_f, min_l), cursor, size)
                cursor += size
                postingsFile.write(pickledPostings)

            i = indices.pop(0)
            if hasattr(i, '_indexFilePrefix'):
                i.close()
                closed_indices.append(i)

        postingsFile.close()

        # delete the old indexes, save the the termdict, and rename the file
        for idx in closed_indices:
            os.remove(idx._indexFilePrefix + '.postings')
            # existence of .postings is guaranteed, but this might fail if it is empty
            if os.path.exists(idx._indexFilePrefix + '.termdict'):
                os.remove(idx._indexFilePrefix + '.termdict')

        with open(indexFilePrefix + '.termdict', 'wb') as f:
            pickle.dump(newTermDict, f)

        os.rename(indexFilePrefix + '.postings.MERGED', indexFilePrefix + '.postings')

        return DiskIndex(indexFilePrefix)

class RAMIndex(BaseIndex):
    """
    Abstract class for indexes held in RAM. Can be used as an ExistenceIndex.

    * Dictionary to (terminfo, postings) is a standard Python dictionary.
    * Terminfo is a 4-tuple in the format specified at the top of this file.
    * Postings are a sorted list of (docId).
    """

    def __init__(self, field):
        """Field is a whoosh Field that supports positions"""
        self._termDict = {}
        self._size = 0
        self._analyzer = field.analyzer

    def __sizeof__(self):
        return self._size

    def add_document(self, docText, docId):
        """Computes postings for the docText and adds it to the _termDict
        Should also manage the size."""
        terminfo, postings = self._termDict.setdefault(docText.encode('utf-8'), ([0,0,1,1],[]))
        terminfo[0] += 1 #frequency (total)
        terminfo[1] += 1 #doc_frequency
        postings.append((docId))
        self._size += 32

    def _postings(self, term):
        """returns raw postings"""
        return self._termDict[term][1]

class FrequencyIndex(RAMIndex):
    """
    Static inverted index for a field that supports frequency, held in RAM.

    * Dictionary to (terminfo, postings) is a standard Python dictionary.
    * Terminfo is a 4-tuple in the format specified at the top of this file.
    * Postings are a sorted list of (docId, frequency).
    """

    def _add_tokenStream(self, tokenStream, docId):
        """adds an iterable of whoosh Tokens (tokenStream) as a new document"""

        #first builds the posting for the document
        termPostings = defaultdict(lambda: [docId, 0])
        docLen = 0
        for t in tokenStream:
            docLen += 1
            p = termPostings[t.text]
            p[1] += 1

        #next merge the posting into the main termDict
        for term, posting in termPostings.items():
            posting = tuple(posting)
            self._size += 64
            terminfo, postings = self._termDict.setdefault(term.encode('utf-8'),
                        [[0,0,0,float('inf')],[]])
            terminfo[0] += posting[1] #frequency (total)
            terminfo[1] += 1 #doc_frequency
            terminfo[2] = max(posting[1],terminfo[2]) #max_weight
            terminfo[3] = min(docLen, terminfo[3]) #min_length
            postings.append(posting)

        return docLen

    def add_document(self, docText, docId):
        """returns number of tokens added (document length)"""
        return self._add_tokenStream(self._analyzer(docText), docId)

class PositionIndex(RAMIndex):
    """
    Static inverted index for a field that supports positions, held in RAM.

    * Dictionary to (terminfo, postings) is a standard Python dictionary.
    * Terminfo is a 4-tuple in the format specified at the top of this file.
    * Postings are a sorted list of (docId, frequency, compressedPostings).
    """

    def _add_tokenStream(self, tokenStream, docId):
        """adds an iterable of whoosh Tokens (tokenStream) as a new document"""

        #first builds the posting for the document
        termPostings = defaultdict(lambda: [docId, 0, []])
        docLen = 0
        for t in tokenStream:
            docLen += 1
            p = termPostings[t.text]
            p[1] += 1
            p[2].append(t.pos)

        #next merge the posting into the main termDict
        for term, posting in termPostings.items():
            posting[2] = dump2(array.array('I', posting[2]), posting[1])
            posting = tuple(posting)
            self._size += 96 + sys.getsizeof(posting[2])
            terminfo, postings = self._termDict.setdefault(term.encode('utf-8'),
                        [[0,0,0,float('inf')],[]])
            terminfo[0] += posting[1] #frequency (total)
            terminfo[1] += 1 #doc_frequency
            terminfo[2] = max(posting[1],terminfo[2]) #max_weight
            terminfo[3] = min(docLen, terminfo[3]) #min_length
            postings.append(posting)

        return docLen

    def add_document(self, docText, docId):
        """returns number of tokens added (document length)"""
        return self._add_tokenStream(self._analyzer(docText, positions=True), docId)

class DiskIndex(BaseIndex):
    """
    Static inverted index stored in a file on disk (posting format independent).
    To add documents to this index, we add them to a RAMIndex and then merge
    that RAMIndex into this Index.

    The termDict does not point directly to a postings, but rather to a tuple of (start, size),
    indicating the position of the postings in the postings file.
    """

    def __init__(self, indexFilePrefix):
        self._indexFilePrefix = indexFilePrefix
        self._file = open(self._indexFilePrefix + '.postings', 'a+b')
        if os.path.exists(indexFilePrefix + '.termdict'):
            with open(indexFilePrefix + '.termdict', 'rb') as f:
                self._termDict = pickle.load(f)
        else:
            self._termDict = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._file.close()

    def close(self):
        self._file.close()

    def _postings(self, term):
        """returns raw postings"""
        _, start, size = self._termDict[term]
        self._file.seek(start)
        return pickle.loads(self._file.read(size))

    def _terminfo_with_postings(self, term):
        terminfo, start, size = self._termDict[term]
        self._file.seek(start)
        return (terminfo, pickle.loads(self._file.read(size)))
