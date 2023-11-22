import pytest
import os
import glob

import gis_fillers as gf
from gis_fillers import Database
from gis_fillers.fillers import zones, loc_resolver
from gis_fillers.getters import zone_getters, generic_getters

conninfo = {
    "host": "localhost",
    "port": 5432,
    "database": "test_gis_fillers",
    "user": "postgres",
    "data_folder": os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "data_folder"
    ),
}


@pytest.fixture
def maindb():
    db = Database(**conninfo)
    db.init_db()
    db.add_filler(zones.geonames.GeonamesFiller())
    db.fill_db()
    yield db
    db.connection.close()


getters_list = [
    (zone_getters.PopulationGetter, dict(zone_level="bezirk", simplified=False)),
    (zone_getters.PopulationDensityGetter, dict(zone_level="bezirk", simplified=False)),
    (
        generic_getters.AreaPointsGetter,
        dict(zone_level="bezirk", location_list=["101", "918", "902"] * 10),
    ),
    (
        generic_getters.AreaPointsGetter,
        dict(zone_level="country", location_list=["AT", "FR", "TR"] * 10),
    ),
    (
        generic_getters.AreaPointsGetter,
        dict(
            zone_level="country",
            location_list=["AT", "FR", "TR"] * 10,
            noise_size=0.2,
            add_noise=True,
        ),
    ),
    (
        generic_getters.AreaPointsGetter,
        dict(
            zone_level="gemeinde",
            location_ref_type="name",
            location_list=["Neunkirchen", "Mistelbach", "Graz"] * 10,
        ),
    ),
    (
        generic_getters.AddressPointsGetter,
        dict(
            location_list=["josefstadter str. 39", "wien", "ortaköy, istanbul"] * 10,
            nominatim_host=None,
            nominatim_user_agent="gis_fillers_test",
            unsafe_nominatim=True,
        ),
    ),
    (
        generic_getters.AddressPointsGetter,
        dict(
            location_list=["josefstadter str. 39", "wien", "ortaköy, istanbul"] * 10,
            nominatim_host=None,
            nominatim_user_agent="gis_fillers_test",
            unsafe_nominatim=False,
            skippable=True,
        ),
    ),
    (
        generic_getters.AddressPointsGetter,
        dict(
            location_list=[
                ("josefstadter str.", " 39"),
                ("wien", None),
                ("ortaköy", "istanbul"),
            ]
            * 10,
            nominatim_host=None,
            nominatim_user_agent="gis_fillers_test",
        ),
    ),
    (
        generic_getters.ZipPointsGetter,
        dict(
            location_list=[("FR", "33400"), ("AT", "1080")] * 10,
        ),
    ),
]


@pytest.fixture(params=getters_list)
def getter(request):
    return request.param


def test_getters(maindb, getter):
    getter[0](db=maindb, **getter[1]).get_result()


def test_loc_solver(maindb):
    maindb.cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS test_loc_solver(
            id1 BIGSERIAL,
            id2 BIGSERIAL,
            address TEXT,
            geom GEOMETRY(POINT,4326),
            PRIMARY KEY(id1,id2)
            );

        INSERT INTO test_loc_solver(address,geom) VALUES 
        ('josefstadter str. 39',NULL),
        ('josefstadter str. 39',NULL),
        ('josefstadter str. 39',NULL),
        ('josefstadter str. 39',NULL),
        ('wien',NULL),
        ('ortaköy,istanbul',NULL),
        ('ortaköy,istanbul',NULL),
        ('üsküdar,istanbul',NULL),
        ('üsküdar,istanbul',NULL),
        ('üsküdar,istanbul',NULL)
        ;
        """
    )
    maindb.add_filler(
        loc_resolver.LocationResolver(
            id_col=("id1", "id2"),
            source_db=maindb,
            loc_col="address",
            query_table="test_loc_solver",
            resolver_args=dict(
                nominatim_host=None,
                nominatim_user_agent="gis_fillers_test",
                unsafe_nominatim=True,
            ),
        )
    )
    maindb.fill_db()
    maindb.connection.commit()
