from .fillers import Fillers
from .zones import zaehlsprengel, countries, geonames


class MetaFiller(Filler):
    def after_insert(self):
        self.db.add_filler(
            zaehlsprengel.ZaehlsprengelFiller(data_folder=self.data_folder)
        )
        self.db.add_filler(
            zaehlsprengel.SimplifiedZSFiller(data_folder=self.data_folder)
        )
        self.db.add_filler(
            zaehlsprengel.PopulationZSFiller(data_folder=self.data_folder)
        )
        self.db.add_filler(zaehlsprengel.PLZFiller(data_folder=self.data_folder))
        self.db.add_filler(geonames.GeonamesFiller(data_folder=self.data_folder))
        self.db.add_filler(countries.CountriesFiller(data_folder=self.data_folder))
