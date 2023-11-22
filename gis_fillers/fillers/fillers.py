from db_fillers import Filler as TemplateFiller
from .loc_resolver import LocationResolver
import copy


class Filler(TemplateFiller):
    def __init__(self, loc_resolve=False, loc_db=None, loc_resolver_args=[], **kwargs):
        self.loc_resolve = loc_resolve
        self.loc_db = loc_db
        if isinstance(loc_resolver_args, dict):
            loc_resolver_args = [loc_resolver_args]
        self.loc_resolver_args = copy.deepcopy(loc_resolver_args)
        TemplateFiller.__init__(self, **kwargs)

    def after_insert(self):
        if self.loc_resolve:
            for lr_args in self.loc_resolver_args:
                if isinstance(self.loc_db, str):
                    self.loc_db = self.db.get_gis_db(schema=self.loc_db)
                self.db.add_filler(LocationResolver(source_db=self.loc_db, **lr_args))
