import pytest
import os
import glob

import gis_fillers as gf
from gis_fillers import Database
from gis_fillers.fillers import zones
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


def test_connect():
    db = Database(**conninfo)


def test_init():
    db = Database(**conninfo)
    db.init_db()


# def test_clean():
#     db = Database(**conninfo)
#     db.clean_db()
#     db.init_db()


@pytest.fixture
def maindb():
    db = Database(**conninfo)
    db.init_db()
    yield db
    db.connection.close()


def test_countries(maindb):
    maindb.add_filler(zones.countries.CountriesFiller())
    maindb.fill_db()


def test_zs(maindb):
    maindb.add_filler(zones.zaehlsprengel.ZaehlsprengelFiller())
    maindb.fill_db()


def test_simplified_zs(maindb):
    maindb.add_filler(
        zones.zaehlsprengel.SimplifiedZSFiller(
            simplify_engine="topojson",
            geojson_gis_info_name="topojson_{YEAR}0101.geojson",
        )
    )
    maindb.fill_db()


# def test_simplified_zs_mapshaper(maindb):
#   maindb.add_filler(zones.zaehlsprengel.SimplifiedZSFiller(simplify_engine='mapshaper',geojson_gis_info_name="mapshaper_{YEAR}0101.geojson",))
#   maindb.fill_db()


def test_plz(maindb):
    maindb.add_filler(zones.zaehlsprengel.PLZFiller())
    maindb.fill_db()


res_list = [
    3,
    4,
]


@pytest.fixture(params=res_list)
def res(request):
    return request.param


country_list = ["AT", "FR"]


@pytest.fixture(params=country_list)
def country(request):
    return request.param


# def test_hexagons(maindb, res, country):
#     maindb.add_filler(
#         zones.hexagons.HexagonsFiller(
#             res=res, target_zone=country, target_zone_level="country"
#         )
#     )
#     maindb.fill_db()


bezirk_list = [918, 922]


@pytest.fixture(params=bezirk_list)
def bezirk(request):
    return request.param


res_bezirk_list = [
    7,
    8,
]


@pytest.fixture(params=res_bezirk_list)
def res_bezirk(request):
    return request.param


# def test_hexagons_bezirk(maindb, res_bezirk, bezirk):
#     maindb.add_filler(
#         zones.hexagons.HexagonsFiller(
#             res=res_bezirk, target_zone=bezirk, target_zone_level="bezirk"
#         )
#     )
#     maindb.fill_db()


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
        ),
    ),
]


@pytest.fixture(params=getters_list)
def getter(request):
    return request.param


def test_getters(maindb, getter):
    getter[0](db=maindb, **getter[1]).get_result()
