"""
 think wrapper around pickle for compatibility if svb not working, but should really
 try to get svb working -- it's much much better
"""
import pickle

def dump1(a, l):
    return pickle.dumps(a)

def dump2(a, l):
    return pickle.dumps(a)

def load1(s, l):
    return pickle.loads(s)

def load2(s, l):
    return pickle.loads(s)
