from cpython cimport array
import array
from libc.stdlib cimport malloc, free
cimport numpy as np
import numpy as np

cdef extern from "inttypes.h":
    ctypedef unsigned int uint32_t
    ctypedef unsigned char uint8_t
    ctypedef unsigned long uint64_t

cdef extern from "streamvbytedelta.h":
    size_t streamvbyte_delta_encode(uint32_t *x, uint32_t length, uint8_t *out, uint32_t prev)
    size_t streamvbyte_delta_decode(uint8_t *x, uint32_t *out, uint32_t length, uint32_t prev)

cdef extern from "varintdecode.h":
    size_t masked_vbyte_decode_delta(const uint8_t *x, uint32_t* out, uint64_t length, uint32_t prev)

cdef extern from "varintencode.h":
    size_t vbyte_encode_delta(uint32_t *x, size_t length, uint8_t *bout, uint32_t prev)

cdef uint32_t PREV = 0

def dump1(array.array arr, uint32_t length):
    cdef uint32_t *x = arr.data.as_uints
    cdef uint8_t *out = <uint8_t *>malloc(length * sizeof(uint32_t))
    res = streamvbyte_delta_encode(x, length, out, PREV)
    cdef bytes b = out[:res]
    free(out)
    return b

def dump2(array.array arr, size_t length):
    cdef uint32_t *x = arr.data.as_uints
    cdef uint8_t *out = <uint8_t *>malloc(length * sizeof(uint32_t))
    res = vbyte_encode_delta(x, length, out, PREV)
    cdef bytes b = out[:res]
    free(out)
    return b

def load1(bytes b, uint32_t length):
    cdef size_t size = length * sizeof(uint32_t)
    cdef uint32_t *rec = <uint32_t *>malloc(size)
    streamvbyte_delta_decode(b, rec, length, PREV)
    a = array.array('I',[])
    a.frombytes((<char *>rec)[:size])
    free(rec)
    return a

def load2(bytes b, uint64_t length):
    cdef size_t size = length * sizeof(uint32_t)
    cdef uint32_t *rec = <uint32_t *>malloc(size)
    masked_vbyte_decode_delta(b, rec, length, PREV)
    a = array.array('I',[])
    a.frombytes((<char *>rec)[:size])
    free(rec)
    return a

def load1_to_np(bytes b, uint32_t length):
    cdef size_t size = length * sizeof(uint32_t)
    cdef uint32_t *rec = <uint32_t *>malloc(size)
    streamvbyte_delta_decode(b, rec, length, PREV)
    a = np.fromstring((<char *>rec)[:size], dtype=np.uint32)
    free(rec)
    return a

def load2_to_np(bytes b, uint32_t length):
    cdef size_t size = length * sizeof(uint32_t)
    cdef uint32_t *rec = <uint32_t *>malloc(size)
    masked_vbyte_decode_delta(b, rec, length, PREV)
    a = np.fromstring((<char *>rec)[:size], dtype=np.uint32)
    free(rec)
    return a
