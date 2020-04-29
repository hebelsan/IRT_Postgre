CREATE FUNCTION tsvector_num_lexemes(tsvector) RETURNS int
    AS 'tsvector_num_lexemes', 'tsvector_num_lexemes'
    LANGUAGE C STRICT;
