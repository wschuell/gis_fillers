# GIS fillers
Here are gathered tools and scripts to rebuild a database of GIS elements from publicly available data sources


### Install

To install the library, and be able to import it elsewhere (including in the provided scripts), run:
```
python3 setup.py develop
```

If you plan to integrate simplified geometries for the Zaehslprengel, you need to install mapshaper through npm: `npm install -g mapshaper`


### Structure

The main database structure is wrapped in a class `database.Database`. It then accepts `Fillers` objects, that will eventually populate it when calling `db.fill_db()`. Fillers can be for example for geographical zones, for a set of nodes, of edges, ... With this structure you can choose specifically which data you want, combine it with others similar sources, and test behavior on dummy data.

### Scripts

Scripts are located in the `scripts` folder. They are simple examples of how to fill a database, with a small description.

### Tests

Tests are located in `tests/testmodule`. At the root of the repository, you can simply run `pytest`. They need a local PostgreSQL database, with PostGIS available, on port 5432, named `test_gis_fillers`, for user `postgres`. Password is read from your `~/.pgpass` file.

Alternatively if you do not want to run the list of tests but just want to check the basic behavior, you can use scripts and specify another database.

### Available raw data

- Zaehlsprengel/population/PLZ data is automatically downloaded from statistik.at
- Countries data are downloaded from https://gisco-services.ec.europa.eu