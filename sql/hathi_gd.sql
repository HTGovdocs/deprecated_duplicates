use ht_repository;

create table hathi_gd (
  id      INT      not null auto_increment,
  gov_doc TINYINT  not null,
  htid    VARCHAR(30) not null unique,
  hashsum CHAR(64) not null unique,
  primary key (id)
);

create table hathi_str (
  id  INT not null auto_increment,
  str VARCHAR(750) not null,
  primary key (id)
);

create table hathi_oclc (
  gd_id  INT not null,
  str_id INT not null,
  primary key (gd_id, str_id),
  foreign key (gd_id) references hathi_gd(id),
  foreign key (str_id) references hathi_str(id)
);

create table hathi_lccn (
  gd_id  INT not null,
  str_id INT not null,
  primary key (gd_id, str_id),
  foreign key (gd_id) references hathi_gd(id),
  foreign key (str_id) references hathi_str(id)
);

create table hathi_issn (
  gd_id  INT not null,
  str_id INT not null,
  primary key (gd_id, str_id),
  foreign key (gd_id) references hathi_gd(id),
  foreign key (str_id) references hathi_str(id)
);

create table hathi_isbn (
  gd_id  INT not null,
  str_id INT not null,
  primary key (gd_id, str_id),
  foreign key (gd_id) references hathi_gd(id),
  foreign key (str_id) references hathi_str(id)
);

create table hathi_title (
  gd_id  INT not null,
  str_id INT not null,
  primary key (gd_id, str_id),
  foreign key (gd_id) references hathi_gd(id),
  foreign key (str_id) references hathi_str(id)
);

create table hathi_enumc (
  gd_id  INT not null,
  str_id INT not null,
  primary key (gd_id, str_id),
  foreign key (gd_id) references hathi_gd(id),
  foreign key (str_id) references hathi_str(id)
);

create table hathi_pubdate (
  gd_id  INT not null,
  str_id INT not null,
  primary key (gd_id, str_id),
  foreign key (gd_id) references hathi_gd(id),
  foreign key (str_id) references hathi_str(id)
);

create table hathi_publisher (
  gd_id  INT not null,
  str_id INT not null,
  primary key (gd_id, str_id),
  foreign key (gd_id) references hathi_gd(id),
  foreign key (str_id) references hathi_str(id)
);

create table hathi_sudoc (
  gd_id  INT not null,
  str_id INT not null,
  primary key (gd_id, str_id),
  foreign key (gd_id) references hathi_gd(id),
  foreign key (str_id) references hathi_str(id)
);

create index hathi_gd_str on hathi_str (str) using btree;
create index hathi_gd_htid on hathi_gd (htid) using btree;

-- create or replace view v_hathi_attr as
-- SELECT hx.gd_id, 'pubdate' AS attr, hs.str AS val FROM hathi_pubdate AS hx JOIN hathi_str AS hs ON (hx.str_id = hs.id)
-- UNION
-- SELECT hx.gd_id, 'publisher' AS attr, hs.str AS val FROM hathi_publisher AS hx JOIN hathi_str AS hs ON (hx.str_id = hs.id)
-- UNION
-- SELECT hx.gd_id, 'enumc' AS attr, hs.str AS val FROM hathi_enumc AS hx JOIN hathi_str AS hs ON (hx.str_id = hs.id)
-- UNION
-- SELECT hx.gd_id, 'title' AS attr, hs.str AS val FROM hathi_title AS hx JOIN hathi_str AS hs ON (hx.str_id = hs.id)
-- UNION
-- SELECT hx.gd_id, 'isbn' AS attr, hs.str AS val FROM hathi_isbn AS hx JOIN hathi_str AS hs ON (hx.str_id = hs.id)
-- UNION
-- SELECT hx.gd_id, 'issn' AS attr, hs.str AS val FROM hathi_issn AS hx JOIN hathi_str AS hs ON (hx.str_id = hs.id)
-- UNION
-- SELECT hx.gd_id, 'lccn' AS attr, hs.str AS val FROM hathi_lccn AS hx JOIN hathi_str AS hs ON (hx.str_id = hs.id)
-- UNION
-- SELECT hx.gd_id, 'oclc' AS attr, hs.str AS val FROM hathi_oclc AS hx JOIN hathi_str AS hs ON (hx.str_id = hs.id)
-- ;