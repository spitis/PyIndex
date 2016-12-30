from .indices import *
import whoosh, sys, pickle, os, numpy as np, multiprocessing as mp

"""This is the equivalent of Whoosh's "Index". It manges multiple underlying indexes according
to a schema."""

class _IndexBuilderProcess(mp.Process):
    """This is a version of StaticIndexManager stripped to the essentials, made to work as a
    child process for StaticIndexManager.add_documents_multiprocessing()."""
    def __init__(self, workerId, indexFilePrefix, schema, inQueue, outQueue, availableQueue,
                ramLimitBytes, checkLimitEvery):
        super().__init__()
        self._workerId = workerId
        self._indexFilePrefix = indexFilePrefix + '-mp-' + str(workerId)
        self._schema = schema
        self.schema = schema
        self._inQueue = inQueue
        self._outQueue = outQueue
        self._availableQueue = availableQueue
        self._ramLimitBytes = ramLimitBytes
        self._checkLimitEvery = checkLimitEvery

        self._totalDocLen = {}

        self._idx = {}
        self._idxFields = set()
        self._storedFields = set()
        self._storedIdx = {}

        for fieldname, field in self._schema.items():
            if field.format:
                if (field.format.__class__ == whoosh.formats.Positions or\
                     field.format.__class__ == whoosh.formats.Frequency):
                    self._idxFields.add(fieldname)
                    self._idx[fieldname] = DiskIndex(self._indexFilePrefix + '_' + fieldname)
                    self._totalDocLen[fieldname] = 0
                elif field.format.__class__ == whoosh.formats.Existence:
                    self._idxFields.add(fieldname)
                    self._idx[fieldname] = DiskIndex(self._indexFilePrefix + '_' + fieldname)
            if field.stored:
                self._storedFields.add(fieldname)

    def create_ram_index(self, fieldname, num = 0):
        schema = self._schema
        if schema[fieldname].format.__class__ == whoosh.formats.Positions:
            return PositionIndex(self._schema[fieldname])
        elif schema[fieldname].format.__class__ == whoosh.formats.Frequency:
            return FrequencyIndex(self._schema[fieldname])
        elif schema[fieldname].format.__class__ == whoosh.formats.Existence:
            return RAMIndex(self._schema[fieldname])
        else:
            raise NotImplementedError("Unsupported field class!")

    def run(self):
        # initialize RAM indices
        rIdx, dIdx = {}, {}
        for fieldname in self._idxFields:
            rIdx[fieldname] = self.create_ram_index(fieldname)
            dIdx[fieldname] = []

        RAMstore = {}

        dl = {fieldname:('dl_' + fieldname) for fieldname in self._idxFields}

        i = 0
        # iterate over documents, adding them to RAM indices and to storedIndex
        while True:
            self._availableQueue.put(self._workerId)
            docs = self._inQueue.get()
            if docs is None:
                break

            for docId, doc in docs:
                i += 1
                stored_value = {}
                for fieldname, value in doc.items():
                    if fieldname in self._idxFields:
                        docLen = rIdx[fieldname].add_document(value, docId)
                        if docLen:
                            stored_value[dl[fieldname]] = docLen
                            self._totalDocLen[fieldname] += docLen
                    if fieldname in self._storedFields:
                        stored_value[fieldname] = value

                if len(stored_value):
                    RAMstore[docId] = stored_value

                # if ram limit exceeded, flush the largest index to disk and start a new one
                if i % self._checkLimitEvery == 0:
                    rIdx_keys, rIdx_values = zip(*rIdx.items())
                    sizes = np.array(list(map(sys.getsizeof, rIdx_values)))
                    while np.sum(sizes) > self._ramLimitBytes:
                        fieldname = rIdx_keys[np.argmax(sizes)]

                        # save the RAM index to disk
                        dIdx[fieldname].append(BaseIndex.write_index_to_disk(rIdx[fieldname],
                                            self._indexFilePrefix + '_' + fieldname + '_' + str(i)))

                        rIdx[fieldname] = self.create_ram_index(fieldname)
                        rIdx_keys, rIdx_values = zip(*rIdx.items())
                        sizes = np.array(list(map(sys.getsizeof, rIdx_values)))

        # merge the RAM indices into the main index
        for fieldname in self._idxFields:
            self._idx[fieldname] = BaseIndex.merge_indices_ordered(self._indexFilePrefix + '_' + fieldname,
                                    [self._idx[fieldname]] + dIdx[fieldname] + [rIdx[fieldname]])
        self._storedIdx.update(RAMstore)

        # now return the indexes to the main process and finish
        dIdxPaths = {k:v._indexFilePrefix for k, v in self._idx.items()}
        for idx in self._idx.values():
            idx.close()

        storedPicklePath = self._indexFilePrefix + ".stored"
        with open(self._indexFilePrefix + ".stored", 'wb') as f:
            pickle.dump(self._storedIdx, f)

        self._outQueue.put((dIdxPaths, storedPicklePath, self._totalDocLen))

class StaticIndexManager():
    """Managers one or more field-oriented indices, together with document metadata.

    Proper use is:

        index = StaticIndexManager(indexFilePrefix, schema)
        index.load()

        ... do stuff ...

        index.save_and_close()
    """

    def __init__(self, indexFilePrefix, schema, reset = False):
        self._indexFilePrefix = indexFilePrefix
        self._schema = schema

        self._lastDocId = -1
        self._totalDocLen = {}

        self._idx = {}
        self._idxFields = set()
        self._storedFields = set()
        self._storedIdx = {}

        for fieldname, field in self._schema.items():
            if field.format:
                if (field.format.__class__ == whoosh.formats.Positions or\
                     field.format.__class__ == whoosh.formats.Frequency):
                    self._idxFields.add(fieldname)
                    self._totalDocLen[fieldname] = 0
                elif field.format.__class__ == whoosh.formats.Existence:
                    self._idxFields.add(fieldname)
            if field.stored:
                self._storedFields.add(fieldname)

        if not reset and os.path.exists(indexFilePrefix + '.manager'):
            raise Exception("Index already exists! To overwrite, call with reset = True.\
                Otherwise, load the .manager file with pickle.")
        elif reset:
            for fieldname in self._idxFields:
                if os.path.exists(self._indexFilePrefix + '_' + fieldname + '.postings'):
                    os.remove(self._indexFilePrefix + '_' + fieldname + '.postings')
                if os.path.exists(self._indexFilePrefix + '_' + fieldname + '.termdict'):
                    os.remove(self._indexFilePrefix + '_' + fieldname + '.termdict')
            if os.path.exists(self._indexFilePrefix + '.stored'):
                os.remove(self._indexFilePrefix + '.stored')

    def load(self):
        """Instantiates each index, or creates new ones if they cannot be instantiated."""

        for fieldname in self._idxFields:
            self._idx[fieldname] = DiskIndex(self._indexFilePrefix + '_' + fieldname)
        if os.path.exists(self._indexFilePrefix + '.stored'):
            with open(self._indexFilePrefix + '.stored', 'rb') as f:
                self._storedIdx = pickle.load(f)

    def close(self):
        """Closes the indexes (so they are not also pickled during saving)"""
        self._storedIdx = None
        for idx in self._idx.values():
            idx.close()
        self._idx = {}

    def save_and_close(self):
        if len(self._storedIdx):
            with open(self._indexFilePrefix + '.stored', 'wb') as f:
                pickle.dump(self._storedIdx, f)
        self.close()
        with open(self._indexFilePrefix + '.manager', 'wb') as f:
            pickle.dump(self, f)

    def save(self):
        if len(self._storedIdx):
            with open(self._indexFilePrefix + '.stored', 'wb') as f:
                pickle.dump(self._storedIdx, f)
        self.close()
        with open(self._indexFilePrefix + '.manager', 'wb') as f:
            pickle.dump(self, f)
        self.load()

    def create_ram_index(self, fieldname, num = 0):
        schema = self._schema
        if schema[fieldname].format.__class__ == whoosh.formats.Positions:
            return PositionIndex(self._schema[fieldname])
        elif schema[fieldname].format.__class__ == whoosh.formats.Frequency:
            return FrequencyIndex(self._schema[fieldname])
        elif schema[fieldname].format.__class__ == whoosh.formats.Existence:
            return RAMIndex(self._schema[fieldname])
        else:
            raise NotImplementedError("Unsupported field class!")

    def add_documents(self, documents, ramLimitBytes = 4 * 1024 * 1024 * 1024,
                      checkLimitEvery = 100):
        """
        Documents is an iterable of dictionaries with each of the relevant schema fields.
        E.g., if schema has filepath and text, each doc should be {'filepath': x, 'text': y}

        Note: inefficient if there are not a lot of documents being added, as this builds
        a new index for added documents, then merges it into the main index. It also
        repickles and saves the storedIndex.
        """
        # initialize RAM indices
        rIdx, dIdx = {}, {}
        for fieldname in self._idxFields:
            rIdx[fieldname] = self.create_ram_index(fieldname)
            dIdx[fieldname] = []

        RAMstore = {}

        dl = {fieldname:('dl_' + fieldname) for fieldname in self._idxFields}

        # iterate over documents, adding them to RAM indices and to storedIndex
        for i, doc in enumerate(documents):
            stored_value = {}
            self._lastDocId += 1
            docId = self._lastDocId
            for fieldname, value in doc.items():
                if fieldname in self._idxFields:
                    docLen = rIdx[fieldname].add_document(value, docId)
                    if docLen:
                        stored_value[dl[fieldname]] = docLen
                        self._totalDocLen[fieldname] += docLen
                if fieldname in self._storedFields:
                    stored_value[fieldname] = value

            if len(stored_value):
                RAMstore[docId] = stored_value

            # if ram limit exceeded, flush the largest index to disk and start a new one
            if i % checkLimitEvery == 0:
                rIdx_keys, rIdx_values = zip(*rIdx.items())
                sizes = np.array(list(map(sys.getsizeof, rIdx_values)))
                while np.sum(sizes) > ramLimitBytes:
                    fieldname = rIdx_keys[np.argmax(sizes)]

                    # save the RAM index to disk
                    dIdx[fieldname].append(BaseIndex.write_index_to_disk(rIdx[fieldname],
                                        self._indexFilePrefix + '_' + fieldname + '_' + str(i)))

                    rIdx[fieldname] = self.create_ram_index(fieldname)
                    rIdx_keys, rIdx_values = zip(*rIdx.items())
                    sizes = np.array(list(map(sys.getsizeof, rIdx_values)))

        # merge the RAM indices into the main index
        for fieldname in self._idxFields:
            self._idx[fieldname] = BaseIndex.merge_indices_ordered(self._indexFilePrefix + '_' + fieldname,
                                    [self._idx[fieldname]] + dIdx[fieldname] + [rIdx[fieldname]])

        self._storedIdx.update(RAMstore)

    def add_documents_multiprocessing(self, documents, num_procs = None, chunkSize = 5,
                    ramLimitBytes = 3 * 1024 * 1024 * 1024, checkLimitEvery = 100):
        """
        Similar to add_documents, but with multiprocessing.

        Notes:
        * ramLimitBytes and checkLimitEvery are per process.

        """
        if num_procs is None:
            num_procs = mp.cpu_count()

        pool = []
        inQueues = []
        outQueue = mp.Queue()
        availableQueue = mp.Queue()
        for i in range(num_procs):
            inQueue = mp.Queue()
            inQueues.append(inQueue)
            worker = _IndexBuilderProcess(i, self._indexFilePrefix, self._schema, inQueue, outQueue,
                                         availableQueue, ramLimitBytes, checkLimitEvery)
            worker.start()
            pool.append(worker)

        docQueue = []
        for d in documents:
            self._lastDocId += 1
            docQueue.append((self._lastDocId, d))
            if len(docQueue) >= chunkSize:
                workerId = availableQueue.get()
                inQueues[workerId].put(docQueue)
                docQueue = []

        if len(docQueue):
            workerId = availableQueue.get()
            inQueues[workerId].put(docQueue)

        # terminate the subprocesses
        for queue in inQueues:
            queue.put(None)

        for worker in pool:
            worker.join()

        # get the results
        idxPaths, storeds = [], []
        for i in range(num_procs):
            idxPath, storedPath, docLens = outQueue.get()
            for fieldname, docLen in docLens.items():
                self._totalDocLen[fieldname] += docLen
            with open(storedPath, 'rb') as f:
                storeds.append(pickle.load(f))
            os.remove(storedPath)
            idxPaths.append(idxPath)

        # merge the differ process indices into the main index
        for fieldname in self._idxFields:
            dIdx = [DiskIndex(i[fieldname]) for i in idxPaths]
            self._idx[fieldname] = BaseIndex.merge_indices_unordered(self._indexFilePrefix + '_' + fieldname,
                                    [self._idx[fieldname]] + dIdx)
        for stored in storeds:
            self._storedIdx.update(stored)
