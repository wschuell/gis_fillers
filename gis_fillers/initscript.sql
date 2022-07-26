

CREATE EXTENSION IF NOT EXISTS POSTGIS;
SET DATESTYLE TO PostgreSQL,European;

CREATE TABLE IF NOT EXISTS data_sources(
id SERIAL PRIMARY KEY,
name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS zone_levels(
id SERIAL PRIMARY KEY,
name TEXT UNIQUE,
pretty_name TEXT
);


CREATE TABLE IF NOT EXISTS zones(
id BIGSERIAL,
name TEXT NOT NULL,
level INT NOT NULL REFERENCES zone_levels(id) ON DELETE CASCADE,
code TEXT,
PRIMARY KEY(level,id),
UNIQUE(code,level)
);

CREATE INDEX IF NOT EXISTS zones_name_idx ON zones(name,level,id);
CREATE INDEX IF NOT EXISTS zones_id_idx ON zones(id,level);


CREATE TABLE IF NOT EXISTS zone_parents(
child BIGINT,
child_level INT,
FOREIGN KEY (child_level,child) REFERENCES zones(level,id) ON DELETE CASCADE,
parent BIGINT,
parent_level INT,
FOREIGN KEY (parent_level,parent) REFERENCES zones(level,id) ON DELETE CASCADE,
share REAL DEFAULT NULL,
PRIMARY KEY(child_level,child,parent_level,parent)
);

CREATE INDEX IF NOT EXISTS zonep_child_idx ON zone_parents(parent_level,parent,child_level,child);


CREATE TABLE IF NOT EXISTS gis_types(
id SERIAL PRIMARY KEY,
name TEXT UNIQUE
);



CREATE TABLE IF NOT EXISTS gis_data(
zone_id BIGINT,
zone_level INT,
FOREIGN KEY (zone_id,zone_level) REFERENCES zones(id,level) ON DELETE CASCADE,
gis_type INT REFERENCES gis_types(id) ON DELETE CASCADE,
geom GEOMETRY,
center GEOMETRY(POINT,4326),
PRIMARY KEY(zone_level,zone_id,gis_type)
);

CREATE INDEX IF NOT EXISTS gd_idx ON gis_data(gis_type,zone_level,zone_id);



CREATE TABLE IF NOT EXISTS zone_attribute_types(
id SERIAL NOT NULL PRIMARY KEY,
name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS zone_attributes(
zone BIGINT NOT NULL,
zone_level INT NOT NULL,
FOREIGN KEY (zone_level,zone) REFERENCES zones(level,id) ON DELETE CASCADE,
attribute INT NOT NULL REFERENCES zone_attribute_types(id) ON DELETE CASCADE,
real_value REAL,
bool_value BOOLEAN,
int_value BIGINT,
str_value TEXT,
json_value JSONB,
--scenario INT REFERENCES scenarios(id) ON DELETE CASCADE,
updated_at TIMESTAMP DEFAULT CURRENT_DATE,
--PRIMARY KEY(scenario,updated_at,zone,attribute)
PRIMARY KEY(updated_at,zone,attribute)
);

CREATE INDEX IF NOT EXISTS zs_attr_zone_idx ON zone_attributes(zone);
CREATE INDEX IF NOT EXISTS zs_attr_attr_idx ON zone_attributes(attribute);
--CREATE INDEX IF NOT EXISTS zs_attr_scenario_idx ON zone_attributes(scenario);
--CREATE INDEX IF NOT EXISTS zs_attr_completenodate_idx ON zone_attributes(zone_level,zone,attribute,scenario);
CREATE INDEX IF NOT EXISTS zs_attr_completenodate_idx ON zone_attributes(zone_level,zone,attribute);

-- CREATE INDEX IF NOT EXISTS pd_p_idx ON pop_distances(pop_node);
-- CREATE INDEX IF NOT EXISTS pd_p_d_idx ON pop_distances(pop_node,distance);

CREATE INDEX IF NOT EXISTS gis_geom_idx
  ON gis_data
  USING GIST (geom);
CREATE INDEX IF NOT EXISTS gis_center_idx
  ON gis_data
  USING GIST (center);
CREATE TABLE IF NOT EXISTS plz_gemeinde(
plz BIGINT NOT NULL,
-- plz BIGINT PRIMARY KEY,
gemeinde BIGINT NOT NULL,
gemeinde_name TEXT,
PRIMARY KEY(plz,gemeinde)
);




CREATE INDEX IF NOT EXISTS zone_code_level_key ON zones(code,level);
CREATE INDEX IF NOT EXISTS zone_id_idx ON zones(id,level);
CREATE INDEX IF NOT EXISTS zone_name_idx ON zones(name,level,id);


--CREATE INDEX IF NOT EXISTS zs_attr_completenodate_idx2 ON zone_attributes(zone_level,zone,attribute,scenario,int_value);
CREATE INDEX IF NOT EXISTS zs_attr_completenodate_idx2 ON zone_attributes(zone_level,zone,attribute,int_value);


CREATE TABLE IF NOT EXISTS _exec_info(
id BIGSERIAL PRIMARY KEY,
exec_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
content TEXT,
content_hash TEXT
);

CREATE TABLE IF NOT EXISTS _fillers_info(
id BIGSERIAL PRIMARY KEY,
exec_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
class TEXT,
args TEXT,
status TEXT
);