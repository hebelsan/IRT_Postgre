# Custom Postgres C Functions

### Why ?

Postgres does not offer any option to add an offset to a ts_vector.
Therefore i had do write my own function.
These functions are heavily inspired by the standard functions *tsvector_concat()* (https://doxygen.postgresql.org/tsvector__op_8c.html#ac1deff26077f95fa67915aacd621ecab) and *add_pos()* https://doxygen.postgresql.org/tsvector__op_8c.html#a520d2cdf10a499d5ec4080ceb53e7068 of the file *[tsvector_op.c](https://doxygen.postgresql.org/tsvector__op_8c.html)*.

The second functionality missing for my project is the concatenation of tsvectors **without** increasing the position index.
Because by default if you concat two tsvectors the second positions will become larger by the max position value of the first tsvector.
This might make sense for other applications but it does not for mine.
Since in my applications only the sections containing the searched word are returned i need to set up the correct position in the document beforehand and when concatenating the tsvectors the position values should not be touched.

Actually i'm a bit disappointed that this functionality is not given per default, but on the other i do understand the problem of ever growing api library.
And the implementation of my custom functions wasn't too difficult as it turned out.

### installation

- check that you have the developer version installed
- go into the directory containing the Makefile 
- sudo make install
- log in the database as superuser (sudo -u postgres psql mydb)
- Link the C Function to Postgres e.g.:
  CREATE FUNCTION tsvector_add_offset(tsvector, integer) RETURNS tsvector
      AS 'tsvector_add_offset', 'tsvector_add_offset'
      LANGUAGE C STRICT;
- test function:
  select tsvector_add_offset(to_tsvector('english', 'hello jonn whats going on'), 5);