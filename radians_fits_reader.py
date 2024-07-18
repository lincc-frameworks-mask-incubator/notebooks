import numpy as np
from hipscat_import.catalog.file_readers import FitsReader

RA_COLUMN = "coord_ra"
DEC_COLUMN = "coord_dec"

class RadiansFitsReader(FitsReader):
    def __init__(self, *args, ra_column, dec_column, **kwargs):
        super().__init__(*args, **kwargs)
        self.ra_column = ra_column
        self.dec_column = dec_column

    def read(self, *args, **kwargs):
        for chunk in super().read(*args, **kwargs):
            chunk = chunk.copy()
            chunk[self.ra_column] = np.degrees(chunk[self.ra_column])
            chunk[self.dec_column] = np.degrees(chunk[self.dec_column])
            yield chunk