from distutils.core import setup, Extension
from Cython.Build import cythonize

ext = Extension(name="svbcomp", sources=["svbcomp.pyx"],
                include_dirs=["include"],
                extra_objects=["streamvbytedelta.o","streamvbyte.o","varintdecode.o","varintencode.o"])

setup(ext_modules=cythonize(ext))
