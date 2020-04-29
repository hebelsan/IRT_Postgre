import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from psycopg2 import sql
from os import path
from itertools import product

from config import config
import sys
sys.path.append("../wiki")
from wiki_pipeline import WikiScratcher2


##
#   Ranking functions
#   # http://ra.ethz.ch/CDstore/www2002/refereed/643/node7.html
##
# (p', q') satisfying T such that p < p' < q' < q or p < p' < q' < q
def check_CDR_rule(res):
    for tup_a in res:
        for tup_b in res:
            if tup_a[0] == tup_b[0] and tup_a[1] != tup_b[1]:
                if tup_a[1] < tup_b[1] and tup_b in res:
                    res.remove(tup_b)
                elif tup_a in res:
                    res.remove(tup_a)
            if tup_a[1] == tup_b[1] and tup_a[0] != tup_b[0]:
                if tup_a[0] > tup_b[0] and tup_b in res:
                    res.remove(tup_b)
                elif tup_a in res:
                    res.remove(tup_a)
    return res

def rank(search_term, positions, num_words):
    if "&" in search_term:
        return calc_rank_and(positions) / num_words
    if "|" in search_term:
        return calc_rank_or(positions) / num_words
    
    ## single term -> just count appearance of word
    word_count = positions[0]
    return len(word_count) / num_words
    

def calc_rank_and(positions):
    res = []
    LAMBDA = 16  # source says 16
    rank = 0
    
    for comb in product(*positions):
        comb = sorted(comb)
        first = comb[0]
        last = comb[len(positions)-1]
        res.append((first, last))
    # remove duplicates
    res = set(res)
    res = check_CDR_rule(list(res))
    
    for pos_pair in res:
        tmp = pos_pair[1] - pos_pair[0] + 1
        if tmp > LAMBDA:
            rank += LAMBDA / tmp
        else:
            rank += 1
    
    return rank
    
def calc_rank_or(positions):
    # a document containing most or all of the query terms should be ranked higher
    # than a document containing fewer terms, regardless of the frequency of term occurrence
    # --> document categories
    return 0


##
#   helper function for db connection
##
def get_connection():
    params = config()
    # connect to the PostgreSQL server
    print("Connecting to the PostgreSQL database...")
    return psycopg2.connect(**params)


##
#   turns a tsvector from postgres to dictionary in python
#   example input: 'corona':13C 'differ':4C 'exampl':9C 'name':12C 'type':5C 'various':3C 'virus':7C,11C
##
def cast_tsv(text, cur):
    if text is None:
        return None
    
    # create dictionary
    dict = {}
    text = text.replace("'", "")
    word_array = text.split(" ")
    for word_locations in word_array:
        word_loc = word_locations.split(":")
        word = word_loc[0]
        locations = word_loc[1].split(",")
        int_locations = []
        # convert location string to integer
        for loc in locations:
            int_locations.append(int(loc))
        dict[word] = int_locations
    
    if dict:
        return dict
    else:
        raise InterfaceError("bad tsv representation: %r" % text)


##
#   helper function for db_create_tables()
##
def create_tables(conn, cur):
    # create table wiki_pages: page title is weight as A
    cur.execute("CREATE TABLE wiki_pages( \
                    id SERIAL PRIMARY KEY, \
                    page_title text UNIQUE NOT NULL, \
                    num_sections smallint, \
                    num_words integer, \
                    num_lexems integer);")
                    
    # create table page sections: section title is weight as B and section text as C
    cur.execute("CREATE TABLE page_sections( \
                    id BIGSERIAL PRIMARY KEY, \
                    page_id INTEGER NOT NULL, \
                    sec_title text, \
                    sec_text text, \
                    num_words integer, \
                    FOREIGN KEY (page_id) REFERENCES wiki_pages (id) ON DELETE CASCADE);")
    
    # create table for searching
    cur.execute("CREATE TABLE search_pages ( \
                    id integer NOT NULL, \
                    all_tsv tsvector, \
                    FOREIGN KEY (id) REFERENCES wiki_pages (id) ON DELETE CASCADE \
                );")
    # gin index on tsv
    cur.execute("CREATE INDEX idx_all_tsv \
                    ON search_pages \
                    USING GIN (all_tsv);")
    # Update triggers
    cur.execute("CREATE OR REPLACE FUNCTION func_copy_page() RETURNS TRIGGER AS \
                    $BODY$ \
                    BEGIN \
                        IF char_length(new.page_title) > 0 THEN \
                            INSERT INTO \
                                search_pages (id, all_tsv) \
                                VALUES (new.id, strip(to_tsvector('english', COALESCE(new.page_title,''))) ); \
                        END IF; \
                        RETURN new; \
                    END; \
                    $BODY$ \
                    language plpgsql;")
    cur.execute("CREATE TRIGGER trig_copy_page \
                    AFTER INSERT ON wiki_pages \
                    FOR EACH ROW \
                    EXECUTE PROCEDURE func_copy_page();")
    cur.execute("CREATE OR REPLACE FUNCTION func_copy_section() RETURNS TRIGGER AS \
                    $BODY$ \
                    BEGIN \
                        IF char_length( new.sec_title) > 0 THEN \
                            INSERT INTO \
                                search_pages (id, all_tsv) \
                                VALUES (new.page_id, strip(to_tsvector('english', COALESCE(new.sec_title,''))) ); \
                        END IF; \
                        IF char_length( new.sec_text) > 0 THEN \
                            INSERT INTO \
                                search_pages (id, all_tsv) \
                                VALUES (new.page_id, strip(to_tsvector('english', COALESCE(new.sec_text,''))) ); \
                        END IF; \
                        RETURN new; \
                    END; \
                    $BODY$ \
                    language plpgsql;")
    cur.execute("CREATE TRIGGER trig_copy_section \
                    AFTER INSERT ON page_sections \
                    FOR EACH ROW \
                    EXECUTE PROCEDURE func_copy_section();")
    
    # create function to aggregate ts_vectors
    aggregate_function_string = "CREATE AGGREGATE tsvector_agg (tsvector) ( \
	                               STYPE = pg_catalog.tsvector, \
	                               SFUNC = tsvector_concat_raw, \
	                               INITCOND = '' \
                                );"
    cur.execute(aggregate_function_string)
    
    # create function for word count in tsvectors
    lexeme_occurrences_function = "CREATE OR REPLACE FUNCTION lexeme_occurrences ( \
                IN _lexemes tsvector \
            ,   IN _word text \
            ,   IN _config regconfig \
            ,   OUT lexeme_count int \
            ,   OUT lexeme_positions int[] \
            ) RETURNS RECORD \
            AS $$ \
            DECLARE \
                _searched_lexeme tsvector := strip ( to_tsvector ( _config, _word ) ); \
                _occurences_pattern text := _searched_lexeme::text || ':([0-9A-D,]+)'; \
                _occurences_list text := substring ( _lexemes::text, _occurences_pattern ); \
            BEGIN \
                SELECT \
                    count ( a ) \
                ,   array_agg ( REGEXP_REPLACE(a, '[A-D]', '')::int ) \
                FROM regexp_split_to_table ( _occurences_list, ',' ) a \
                WHERE _searched_lexeme::text != '' \
                INTO \
                    lexeme_count \
                ,   lexeme_positions; \
                RETURN; \
            END $$ LANGUAGE plpgsql;"
    cur.execute(lexeme_occurrences_function)
    
    conn.commit()
    print("succefully created tables!")


def db_create_init_file(database_name, user_name):
    config_file_name = "database.ini"
    if path.exists(config_file_name):
        print("file " + config_file_name + " already exists")
    else:
        f = open(config_file_name,"w+")
        f.write("[postgresql]\n")
        f.write("database=" + database_name + "\n")
        f.write("user=" + user_name)


##
#   creates the tables and functions
##
def db_create_tables():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # check if tables already exist
        cur.execute("SELECT EXISTS ( \
                        SELECT FROM information_schema.tables \
                            WHERE table_schema='public' AND \
                                  table_name='wiki_pages');")
        wiki_pages_exist = cur.fetchone()[0]
        cur.execute("SELECT EXISTS ( \
                        SELECT FROM information_schema.tables \
                            WHERE table_schema='public' AND \
                                  table_name='page_sections');")
        page_sections_exist = cur.fetchone()[0]
        if wiki_pages_exist or page_sections_exist:
            print("wiki_page or page_sections table already exist")
        else:
            create_tables(conn, cur)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")
    

##
#   prints all rows of the documents table
##
def db_show_tables():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        print("wiki_pages:")
        # cur.execute("SELECT * FROM wiki_pages;")
        cur.execute("SELECT id, page_title, num_sections, num_words FROM wiki_pages;")
        rows = cur.fetchall()
        for row in rows:
            print(row)
        print("")
        print("page_sections:")
        cur.execute("SELECT id, page_id, sec_title, num_words FROM page_sections;")
        rows = cur.fetchall()
        for row in rows:
            print(row)
        print("")
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")

##
#   inserts a row into the documents table
##
def db_insert_wiki(wiki_category, num_pages, batch_size):
    wiki = WikiScratcher2(category=wiki_category)
    batch_iter = num_pages // batch_size
    
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # add typecast for tsv
        cur.execute("SELECT NULL::TSVECTOR")
        tsv_oid = cur.description[0][1]
        TSV = psycopg2.extensions.new_type((tsv_oid,), "TSVECTOR", cast_tsv)
        psycopg2.extensions.register_type(TSV)
        
        for it in range(batch_iter):
            print('batch: ' + str(it*batch_size))
            wiki_data = wiki.get_data(num_pages=batch_size)
            for page_name, section_data in wiki_data.items():
                ## Insert into the table wiki_pages
                sec_titles = section_data.keys()
                sql_string = "INSERT INTO wiki_pages \
                                    (page_title, num_sections) \
                                VALUES \
                                    (%s, %s) \
                                RETURNING id, tsvector_num_lexemes(to_tsvector('english', COALESCE(%s,'')));"
                cur.execute(sql_string, (page_name, len(sec_titles), page_name))
                tuple = cur.fetchall()[0]
                id_new_row = tuple[0]
                num_lex_page_title = tuple[1]
                num_word_page_title = len(page_name)
                num_words = num_word_page_title
                num_lex = num_lex_page_title
                
                # create word_matrix table
                sql_string = "CREATE TABLE %s ( \
                                id SERIAL PRIMARY KEY, \
                                lexeme text UNIQUE NOT NULL, \
                                positions  integer ARRAY);"
                cur.execute(sql.SQL("CREATE TABLE {} ( \
                                id SERIAL PRIMARY KEY, \
                                lexeme text UNIQUE NOT NULL, \
                                positions  integer ARRAY);")
                            .format(sql.Identifier(page_name)))
                # insert page name in word_matrix table
                sql_string = "SELECT to_tsvector('english', COALESCE(%s,''))"
                cur.execute(sql_string, (page_name, ))
                title_vector = cur.fetchone()[0]
                for lexeme, positions in title_vector.items():
                    cur.execute(sql.SQL(" \
                        INSERT INTO {} (lexeme, positions) \
                            VALUES \
                            	( \
                            		%s, \
                            		%s \
                            	) \
                            ON CONFLICT (lexeme) \
                            DO \
                            		UPDATE \
                            	  SET positions = {}.positions || EXCLUDED.positions;"
                    ).format(sql.Identifier(page_name), sql.Identifier(page_name)), [lexeme, positions])
                
                ## Insert into the table page_sections
                for sec_title, sec_text in section_data.items():
                    
                    ## word count
                    sec_num_words_title = len(sec_title)
                    sec_num_words_text = len(sec_text)
                    sec_num_words = sec_num_words_title + sec_num_words_text
                    ## lexem lexeme_count
                    sql_string = "select tsvector_num_lexemes(to_tsvector('english', COALESCE(%s,''))), \
                                         tsvector_num_lexemes(to_tsvector('english', COALESCE(%s,'')))"
                    cur.execute(sql_string, (sec_title, sec_text))
                    tuple = cur.fetchall()[0]
                    num_lex_title = tuple[0]
                    num_lex_text = tuple[1]
                    
                    ## add words to word_matrix_table
                    # add words from section title
                    if len(sec_title) > 0:
                        sql_string = "SELECT tsvector_add_offset(to_tsvector('english', COALESCE(%s,'')), %s)"
                        cur.execute(sql_string, (sec_title, num_lex))
                        sec_title_vector = cur.fetchone()[0]
                        for lexeme, positions in sec_title_vector.items():
                            cur.execute(sql.SQL(" \
                                INSERT INTO {} (lexeme, positions) \
                                    VALUES \
                                    	( \
                                    		%s, \
                                    		%s \
                                    	) \
                                    ON CONFLICT (lexeme) \
                                    DO \
                                    		UPDATE \
                                    	  SET positions = {}.positions || EXCLUDED.positions;"
                            ).format(sql.Identifier(page_name), sql.Identifier(page_name)), [lexeme, positions])
                    # add words from section text
                    if len(sec_text) > 0:
                        sql_string = "SELECT tsvector_add_offset(to_tsvector('english', COALESCE(%s,'')), %s)"
                        cur.execute(sql_string, (sec_text, num_lex + num_lex_title))
                        sec_text_vector = cur.fetchone()[0]
                        for lexeme, positions in sec_text_vector.items():
                            cur.execute(sql.SQL(" \
                                INSERT INTO {} (lexeme, positions) \
                                    VALUES \
                                    	( \
                                    		%s, \
                                    		%s \
                                    	) \
                                    ON CONFLICT (lexeme) \
                                    DO \
                                    		UPDATE \
                                    	  SET positions = {}.positions || EXCLUDED.positions;"
                            ).format(sql.Identifier(page_name), sql.Identifier(page_name)), [lexeme, positions])
                    
                    # set text in page_sections
                    sql_string = "INSERT INTO page_sections \
                                        (page_id, sec_title, sec_text, num_words) \
                                    VALUES \
                                        (%s, %s, %s, %s);"
                    cur.execute(sql_string,
                        (id_new_row, sec_title, sec_text, sec_num_words))
                    
                    # update num lexemes and num_words
                    num_lex += num_lex_title + num_lex_text
                    num_words += sec_num_words
                sql_string = "UPDATE wiki_pages SET num_words = %s, num_lexems = %s WHERE id = %s;"
                cur.execute(sql_string, (num_words, num_lex, id_new_row))
            conn.commit()
            
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


##
# seaches the table for a specific term in title plus content and returns the id of the document
##
def db_search_term(search_term):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql_string = "SELECT \
                        res.id, \
                        pag.num_words, \
                        pag.page_title \
                     FROM wiki_pages AS pag \
                     INNER JOIN \
                     (SELECT \
                        id \
                      FROM \
                        search_pages, to_tsquery('english', %s) query \
                      WHERE \
                        query @@ all_tsv \
                      GROUP BY \
                        id) \
                      AS res \
                        on pag.id = res.id;"
        cur.execute(sql_string, (search_term,))
        res = cur.fetchall()
        
        # get search_lexeme_string
        cur.execute("select to_tsquery('english', %s)", (search_term,))
        search_lexeme_string = cur.fetchone()[0]
        # TODO in seperate function -> remove ''
        search_lexeme_string = search_lexeme_string.replace("'", "")
        print(search_lexeme_string)
        search_lexemes = []
        if "&" in search_lexeme_string:
            search_lexemes = search_lexeme_string.split(" & ")
        elif "|" in search_lexeme_string:
            search_lexemes = search_lexeme_string.split(" | ")
        else:
            search_lexemes = [search_lexeme_string]
        
        for row in res:
            num_words = row[1]
            page_title = row[2]
            positions = []
            # get positions
            for lexeme in search_lexemes:
                cur.execute(sql.SQL("SELECT positions FROM {} WHERE lexeme = %s")
                    .format(sql.Identifier(page_title)), [lexeme])
                positions.append(cur.fetchone()[0])
            ranking = rank(search_lexeme_string, positions, num_words)
            print("title: " + page_title + " rank: " + str(ranking))
            
        print("num results: " + str(len(res)))
        
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


##
#   deletes all table entries
##
def db_reset():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM wiki_pages;")
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


##
#   deletes all tables and rules
##
def db_drop_all_tables():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # Delete all word_matrix_tables
        cur.execute("SELECT page_title FROM wiki_pages;")
        res = cur.fetchall()
        for row in res:
            title = row[0]
            cur.execute(sql.SQL("DROP TABLE {} CASCADE;")
                        .format(sql.Identifier(title)))
        
        cur.execute("DROP AGGREGATE tsvector_agg (tsvector) CASCADE;")
        cur.execute("DROP FUNCTION lexeme_occurrences CASCADE;")
        cur.execute("DROP TABLE wiki_pages CASCADE;")
        cur.execute("DROP TABLE page_sections CASCADE;")
        cur.execute("DROP TABLE search_pages CASCADE;")
        conn.commit()
        
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")
