# A leaner, faster backend for Whoosh

[Whoosh](http://whoosh.readthedocs.io/en/latest/index.html) is great, but was working a bit too slow for my purposes (which require fast access to postings and slightly big data (~200GB)). Rather than jump ship to Lucene or some other IR framework, I built this backend for Whoosh, which in addition to having fast postings access also ended up being quite a bit faster than Whoosh's default backend.

### Benefits of this backend

- over 50% reduction in indexing speed
- over 50% reduction in query time
- substantial reduction in index size
- supports the default BM25 scoring, and should play nice with much of Whoosh
- codebase is small and should be easy to understand/modify

### Limitations of this backend

- very lean / only supports a static index; there is no delete document, only add documents; there is no support for segments, so adding individual documents requires the entire index to be rewritten (i.e., should add documents in bulk).
- no block quality (TODO)
- bare minimum testing (in the notebook)
- the reduction in index size, and a small part of the speed boost is from using [this](https://github.com/lemire/streamvbyte) and [this](https://github.com/lemire/MaskedVByte) via a cython wrapper for postings compression. Very fast, but requires a recent Intel processor (e.g., Haswell). You may need to recompile the cython for this to work in your environment (run `python setup.py build_ext --inplace` in the streamvbyte directory to make a compatible .so file). It will fall back on pickle if you don't have this, which is not nearly as good (but still faster than default whoosh).
- built in Python 3.5 with no eye for backward compatibility, and will not work with Python 2 without modification
- takes up a lot of memory! All stored data is held in memory, and entire postings are read into memory; so this takes up a lot more memory than Whoosh (fixing this is a TODO)
- lots of Whoosh features are not supported (e.g., term vectors, "unique" properties in the schema, etc.)

### Todo
- add block quality and stop storing all postings in memory

The IPython notebook has the benchmark calculations + shows how to use this backend with Whoosh.

### Benchmarks

Datasets used are text collections from [this site](http://dhresourcesforprojectbuilding.pbworks.com/w/page/69244469/Data%20Collections%20and%20Datasets).

- TCP-ECCO (170mb uncompressed) can be downloaded [here](https://github.com/Early-Modern-OCR/TCP-ECCO-texts/archive/master.zip)
- Lincoln (700kb uncompressed) can be downloaded [here](http://oldsite.english.ucsb.edu/faculty/ayliu/unlocked/lincoln/lincoln-speeches-and-writings.zip)

##### Index time

|Dataset   |Index time (single process)   |Index time (multi process)     |
|---|---|---|
| Swhoosh (Lincoln) | ~0.32s  | -  |
| Whoosh (Lincoln) | ~1.03s  | -  |
| Swhoosh (TCP-ECCO)  | ~66.6s  | ~27.7s  |
| Whoosh (TCP-ECCO)  | ~175.1s  | ~147.7s  |

##### Index Size

|Dataset   |Index size  |
|---|---|
| Swhoosh (Lincoln) | 700kb  |
| Whoosh (Lincoln) | 1.5mb  |
| Swhoosh (TCP-ECCO)  | 102mb  |
| Whoosh (TCP-ECCO)  | 170mb  |

##### Query Time (on TCP-ECCO)

###### 3 word queries (disjunctive / OR)

- Swhoosh time per query: 3.83 ms
- Whoosh time per query: 9.07 ms

Swhoosh was 58% percent faster.

###### 6 word queries (disjunctive / OR)

- Swhoosh time per query: 5.54 ms
- Whoosh time per query: 14.36 ms

Swhoosh was 61% percent faster.

###### 30 word queries (disjunctive / OR)

- Swhoosh time per query: 48.19 ms
- Whoosh time per query: 92.54 ms

Swhoosh was 48% percent faster.
