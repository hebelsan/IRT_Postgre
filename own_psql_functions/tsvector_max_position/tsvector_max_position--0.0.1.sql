CREATE FUNCTION tsvector_max_position(tsvector) RETURNS int
    AS 'tsvector_max_position', 'tsvector_max_position'
    LANGUAGE C STRICT;
