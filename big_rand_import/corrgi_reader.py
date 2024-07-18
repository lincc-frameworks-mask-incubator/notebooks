import numpy as np
import pandas as pd
from hipscat_import.catalog.file_readers import InputReader


class CorrgiRandNumpyReaderChunked(InputReader):
    def __init__(self, chunksize=1_000_000):
        self.chunksize = chunksize

    def read(self, input_file, read_columns=None):
        data = np.load(input_file)
        total_rows = len(data)
        read_rows = 0
        while read_rows < total_rows:
            chunk = data[read_rows : read_rows+self.chunksize]
            yield pd.DataFrame(chunk, columns=["ra", "dec", "z", "weight"])
            read_rows += self.chunksize

class CorrgiRandNumpyReader(InputReader):
    def read(self, input_file, read_columns=None):
        data = np.load(input_file)
        yield pd.DataFrame(data, columns=["ra", "dec", "z", "weight"])