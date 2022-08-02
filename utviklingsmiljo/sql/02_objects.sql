ALTER SESSION SET CONTAINER = XEPDB1;

CREATE SEQUENCE kafka.isdialogmote_sequence
MINVALUE 1 MAXVALUE 999999999999
START WITH 1 INCREMENT BY 1 CACHE 20;

CREATE TABLE kafka.raw_isdialogmote (
  kafka_message       BLOB,
  kafka_key           VARCHAR2(255 CHAR),
  kafka_topic         VARCHAR2(255 CHAR)  NOT NULL,
  kafka_offset        NUMBER              NOT NULL,
  kafka_mottatt_dato  DATE                NOT NULL,
  kafka_partisjon     NUMBER              NOT NULL,
  kafka_hash          VARCHAR2(255 CHAR)  NOT NULL,
  lastet_dato         DATE                NOT NULL,
  kildesystem         VARCHAR2(255 CHAR)  NOT NULL
);

ALTER TABLE kafka.raw_isdialogmote
  ADD CONSTRAINT isdialog_json
  CHECK (kafka_message IS JSON);
