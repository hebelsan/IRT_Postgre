CREATE FUNCTION tsvector_concat_raw(tsvector, tsvector) RETURNS tsvector
    AS 'tsvector_concat_raw', 'tsvector_concat_raw'
    LANGUAGE C STRICT;
