use ht_repository;

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
drop table if exists hathi_gd;
drop table if exists hathi_input_file;

-- Keep track of files used as input.
-- PK is used as FK in hathi_gd, to ensure that
-- one record from one file is only used once.
create table hathi_input_file (
  id        INT          not null auto_increment,
  file_path VARCHAR(200) not null,
  checksum  CHAR(32)     not null unique,
  date_read DATETIME     not null,
  primary key (id)
);

-- Base record, from whence attributes and values radiate.
create table hathi_gd (
  id        INT         not null auto_increment,
  gov_doc   TINYINT     not null,
  file_id   INT         not null,
  record_id VARCHAR(50) not null, -- Whatever the unique id is in the file
  hashsum   CHAR(64)    not null unique,
  primary key (id),
  foreign key (file_id) references hathi_input_file(id),
  unique key file_record_ids (file_id, record_id)
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