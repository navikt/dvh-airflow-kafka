ALTER SESSION SET CONTAINER = XEPDB1;

CREATE SEQUENCE kafka.isdialogmote_sequence
MINVALUE 1 MAXVALUE 999999999999
START WITH 1 INCREMENT BY 1 CACHE 20;

CREATE TABLE kafka.raw_isdialogmote (
  kafka_message       BLOB,
  kafka_message_bytes  BLOB,
  kafka_key           VARCHAR2(255 CHAR),
  kafka_topic         VARCHAR2(255 CHAR)  NOT NULL,
  kafka_offset        NUMBER              NOT NULL,
  kafka_mottatt_dato  DATE                NOT NULL,
  kafka_partisjon     NUMBER              NOT NULL,
  kafka_hash          VARCHAR2(255 CHAR),
  lastet_dato         DATE                NOT NULL,
  kildesystem         VARCHAR2(255 CHAR)  NOT NULL
);

ALTER TABLE kafka.raw_isdialogmote
  ADD CONSTRAINT isdialog_json
  CHECK (kafka_message IS JSON);


CREATE TABLE kafka.dvh_person_ident_off_id AS (
  SELECT 
    1 AS FK_PERSON1 
    ,'1234' AS OFF_ID
    ,0 AS SKJERMET_KODE
    ,TO_DATE('2020-01-01', 'YYYY-MM-DD') AS GYLDIG_FRA_DATO
    ,TO_DATE('9999-12-31', 'YYYY-MM-DD') AS GYLDIG_TIL_DATO 
  FROM DUAL
  UNION  
  SELECT 
    6 AS FK_PERSON1 
    ,'6789' AS OFF_ID
    ,6 AS SKJERMET_KODE
    ,TO_DATE('2020-01-01', 'YYYY-MM-DD') AS GYLDIG_FRA_DATO
    ,TO_DATE('9999-12-31', 'YYYY-MM-DD') AS GYLDIG_TIL_DATO 
  FROM DUAL 
);
