CREATE FUNCTION tsvector_add_offset(tsvector, integer) RETURNS tsvector
    AS 'tsvector_add_offset', 'tsvector_add_offset'
    LANGUAGE C STRICT;
