use ht_repository;

-- Dropping indexes
drop index hathi_gd_str on hathi_str;
drop index hathi_record on hathi_gd;
drop index related_checksum on hathi_related;
drop index related_gd_id on hathi_related;

drop index hathi_sudoc_gd_id  on hathi_sudoc;
drop index hathi_sudoc_str_id on hathi_sudoc;

drop index hathi_publisher_gd_id  on hathi_publisher;
drop index hathi_publisher_str_id on hathi_publisher;

drop index hathi_pubdate_gd_id  on hathi_pubdate;
drop index hathi_pubdate_str_id on hathi_pubdate;

drop index hathi_enumc_gd_id  on hathi_enumc;
drop index hathi_enumc_str_id on hathi_enumc;

drop index hathi_title_gd_id  on hathi_title;
drop index hathi_title_str_id on hathi_title;

drop index hathi_isbn_gd_id  on hathi_isbn;
drop index hathi_isbn_str_id on hathi_isbn;

drop index hathi_issn_gd_id  on hathi_issn;
drop index hathi_issn_str_id on hathi_issn;

drop index hathi_lccn_gd_id  on hathi_lccn;
drop index hathi_lccn_str_id on hathi_lccn;

drop index hathi_oclc_gd_id  on hathi_oclc;
drop index hathi_oclc_str_id on hathi_oclc;


-- Drop existing tables.
drop table if exists hathi_enumc;
drop table if exists hathi_isbn;
drop table if exists hathi_issn;
drop table if exists hathi_lccn;
drop table if exists hathi_oclc;
drop table if exists hathi_pubdate;
drop table if exists hathi_publisher;
drop table if exists hathi_sudoc;
drop table if exists hathi_title;

drop table if exists hathi_str;
drop table if exists hathi_related;
drop table if exists hathi_gd;
drop table if exists hathi_input_file;

-- Keep track of files used as input.
-- PK is used as FK in hathi_gd, to ensure that
-- one record from one file is only used once.
create table hathi_input_file (
  id        INT          not null auto_increment,
  file_path VARCHAR(200) not null,
  date_read DATETIME     not null,
  primary key (id)
);

-- Base record, from whence attributes and values radiate.
create table hathi_gd (
  id        INT         not null auto_increment,
  gov_doc   TINYINT     not null,
  file_id   INT         not null, -- The file we got the record from
  lineno    INT         not null, -- The line in the input file where we got the record.
  record_id VARCHAR(50) not null, -- Whatever the unique id is of the base record in the file.
  item_id  VARCHAR(50)  null,     -- If the record has holdings, the id of the item.
  hashsum   CHAR(64)    not null unique,
  primary key (id),
  foreign key (file_id) references hathi_input_file(id)
);

-- All values are stored as string ids, actual values stored here.
create table hathi_str (
  id  INT not null auto_increment,
  str VARCHAR(750) not null,
  primary key (id)
);

-- All oclcs are a link between [hathi_gd]<-[hathi_oclc]->[hathi_str].
-- The same goes for all other hathi_* attribute-value tables.
create table hathi_oclc (
  gd_id  INT not null,
  str_id INT not null,
  marc_field CHAR(4) not null,
  primary key (gd_id, str_id),
  foreign key (gd_id) references hathi_gd(id),
  foreign key (str_id) references hathi_str(id)
);

create table hathi_lccn (
  gd_id  INT not null,
  str_id INT not null,
  marc_field CHAR(4) not null,
  primary key (gd_id, str_id),
  foreign key (gd_id) references hathi_gd(id),
  foreign key (str_id) references hathi_str(id)
);

create table hathi_issn (
  gd_id  INT not null,
  str_id INT not null,
  marc_field CHAR(4) not null,
  primary key (gd_id, str_id),
  foreign key (gd_id) references hathi_gd(id),
  foreign key (str_id) references hathi_str(id)
);

create table hathi_isbn (
  gd_id  INT not null,
  str_id INT not null,
  marc_field CHAR(4) not null,
  primary key (gd_id, str_id),
  foreign key (gd_id) references hathi_gd(id),
  foreign key (str_id) references hathi_str(id)
);

create table hathi_title (
  gd_id  INT not null,
  str_id INT not null,
  marc_field CHAR(4) not null,
  primary key (gd_id, str_id),
  foreign key (gd_id) references hathi_gd(id),
  foreign key (str_id) references hathi_str(id)
);

create table hathi_enumc (
  gd_id  INT not null,
  str_id INT not null,
  marc_field CHAR(4) not null,
  primary key (gd_id, str_id),
  foreign key (gd_id) references hathi_gd(id),
  foreign key (str_id) references hathi_str(id)
);

create table hathi_pubdate (
  gd_id  INT not null,
  str_id INT not null,
  marc_field CHAR(4) not null,
  primary key (gd_id, str_id),
  foreign key (gd_id) references hathi_gd(id),
  foreign key (str_id) references hathi_str(id)
);

create table hathi_publisher (
  gd_id  INT not null,
  str_id INT not null,
  marc_field CHAR(4) not null,
  primary key (gd_id, str_id),
  foreign key (gd_id) references hathi_gd(id),
  foreign key (str_id) references hathi_str(id)
);

create table hathi_sudoc (
  gd_id  INT not null,
  str_id INT not null,
  marc_field CHAR(4) not null,
  primary key (gd_id, str_id),
  foreign key (gd_id) references hathi_gd(id),
  foreign key (str_id) references hathi_str(id)
);

-- Contains ids and hashes. Ids with the same hash are (at least) related.
create table hathi_related (
  gd_id    INT      not null,
  checksum CHAR(32) not null,
  foreign key (gd_id) references hathi_gd(id)
);

create index hathi_gd_str on hathi_str (str) using btree;
create index hathi_record on hathi_gd (record_id) using btree;
create index related_checksum on hathi_related (checksum) using hash;
create index related_gd_id on hathi_related (gd_id) using btree;

create index hathi_sudoc_gd_id  on hathi_sudoc (gd_id)  using btree;
create index hathi_sudoc_str_id on hathi_sudoc (str_id) using btree;

create index hathi_publisher_gd_id  on hathi_publisher (gd_id)  using btree;
create index hathi_publisher_str_id on hathi_publisher (str_id) using btree;

create index hathi_pubdate_gd_id  on hathi_pubdate (gd_id)  using btree;
create index hathi_pubdate_str_id on hathi_pubdate (str_id) using btree;

create index hathi_enumc_gd_id  on hathi_enumc (gd_id)  using btree;
create index hathi_enumc_str_id on hathi_enumc (str_id) using btree;

create index hathi_title_gd_id  on hathi_title (gd_id)  using btree;
create index hathi_title_str_id on hathi_title (str_id) using btree;

create index hathi_isbn_gd_id  on hathi_isbn (gd_id)  using btree;
create index hathi_isbn_str_id on hathi_isbn (str_id) using btree;

create index hathi_issn_gd_id  on hathi_issn (gd_id)  using btree;
create index hathi_issn_str_id on hathi_issn (str_id) using btree;

create index hathi_lccn_gd_id  on hathi_lccn (gd_id)  using btree;
create index hathi_lccn_str_id on hathi_lccn (str_id) using btree;

create index hathi_oclc_gd_id  on hathi_oclc (gd_id)  using btree;
create index hathi_oclc_str_id on hathi_oclc (str_id) using btree;
