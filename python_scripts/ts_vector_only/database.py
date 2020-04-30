import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from os import path

from config import config
import sys
sys.path.append("../wiki")
from wiki_pipeline import WikiScratcher2

# for statistics
import json


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
        dict[word] = locations
    
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
                    page_title TEXT UNIQUE NOT NULL, \
                    weighted_title_tsv TSVECTOR, \
                    num_words_title INTEGER, \
                    num_sections SMALLINT, \
                    num_words INTEGER, \
                    num_lexems INTEGER);")
                    
    # create table page sections: section title is weight as B and section text as C
    cur.execute("CREATE TABLE page_sections( \
                    id BIGSERIAL PRIMARY KEY, \
                    sec_id INTEGER NOT NULL, \
                    page_id INTEGER NOT NULL, \
                    type TEXT, \
                    tsv TSVECTOR, \
                    num_words INTEGER, \
                    num_lexemes INTEGER, \
                    FOREIGN KEY (page_id) REFERENCES wiki_pages (id) ON DELETE CASCADE);")
    
    # create table for searching
    cur.execute("CREATE TABLE search_pages ( \
                    id BIGSERIAL PRIMARY KEY, \
                    page_id INTEGER NOT NULL, \
                    sec_id INTEGER NOT NULL, \
                    num_words INTEGER NOT NULL, \
                    all_tsv TSVECTOR, \
                    FOREIGN KEY (page_id) REFERENCES wiki_pages (id) ON DELETE CASCADE \
                );")
    # gin index on tsv
    cur.execute("CREATE INDEX idx_all_tsv \
                    ON search_pages \
                    USING GIN (all_tsv);")
    # Update triggers
    cur.execute("CREATE OR REPLACE FUNCTION func_copy_page() RETURNS TRIGGER AS \
                    $BODY$ \
                    BEGIN \
                        IF length( new.weighted_title_tsv) > 0 THEN \
                            INSERT INTO \
                                search_pages (page_id, sec_id, num_words, all_tsv) \
                                VALUES (new.id, 0, new.num_words_title, new.weighted_title_tsv); \
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
                        IF length( new.tsv) > 0 THEN \
                            INSERT INTO \
                                search_pages (page_id, sec_id, num_words, all_tsv) \
                                VALUES (new.page_id, new.sec_id, new.num_words, new.tsv); \
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
        
        for it in range(batch_iter):
            print('batch: ' + str(it*batch_size))
            wiki_data = wiki.get_data(num_pages=batch_size)
            for page_name, section_data in wiki_data.items():
                ## Insert into the table wiki_pages
                sec_titles = section_data.keys()
                num_word_page_title = len(page_name)
                num_words = num_word_page_title
                sql_string = "INSERT INTO wiki_pages \
                                    (page_title, num_sections, weighted_title_tsv, num_words_title) \
                                VALUES \
                                    (%s, %s, setweight(to_tsvector('english', COALESCE(%s,'')), 'A'), %s) \
                                RETURNING id, tsvector_num_lexemes(weighted_title_tsv);"
                cur.execute(sql_string, (page_name, len(sec_titles), page_name, num_word_page_title))
                tuple = cur.fetchall()[0]
                id_new_row = tuple[0]
                num_lex_page_title = tuple[1]
                num_lex = num_lex_page_title
                
                ## Insert into the table page_sections
                sec_id = 1
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
                    num_lex += num_lex_title + num_lex_text
                    
                    sql_string = "INSERT INTO page_sections \
                                        (sec_id, page_id, type, tsv, num_words, num_lexemes) \
                                    VALUES \
                                        (%s, %s, %s, \
                                        setweight(to_tsvector('english', COALESCE(%s,'')), 'B'), \
                                        %s, %s);"
                    cur.execute(sql_string,
                        (sec_id, id_new_row, "title", sec_title, sec_num_words_title, num_lex_title))
                    
                    sql_string = "INSERT INTO page_sections \
                                        (sec_id, page_id, type, tsv, num_words, num_lexemes) \
                                    VALUES \
                                        (%s, %s, %s, \
                                        setweight(to_tsvector('english', COALESCE(%s,'')), 'B'), \
                                        %s, %s);"
                    cur.execute(sql_string,
                        (sec_id+1, id_new_row, "text", sec_text, sec_num_words_text, num_lex_text))
                    num_words += sec_num_words
                    sec_id += 2
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
def db_rank_page(term):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        sql_string = "SELECT \
                        pag.id, \
                        res.rank / pag.num_words AS rank, \
                        pag.num_words \
                     FROM wiki_pages AS pag \
                     INNER JOIN \
                     (SELECT \
                        page_id, \
                        sum(ts_rank_cd(array[0.1, 0.2, 0.4, 1.0], all_tsv, to_tsquery('english', %s), 0)) AS rank \
                      FROM \
                        search_pages, to_tsquery('english', %s) query \
                      WHERE \
                        query @@ all_tsv \
                      GROUP BY \
                        page_id) \
                      AS res \
                        on pag.id = res.page_id \
                      ORDER BY pag.id DESC;"
        cur.execute(sql_string, (term, term))
        res = cur.fetchall()
        #for comparing
        data = {}
        data['pages'] = []
        print("top 10 ranking documents:")
        for row in res:
            page_id = row[0]
            rank = row[1]
            page_word_count = row[2]
            data['pages'].append({
                'page_id': page_id,
                'rank': rank
            })
            #print("page id: " + str(page_id) + " ranking: " + str(rank))
        print("num results: " + str(len(res)))
        with open("../../rating_comparison/data/page_" + term + ".txt", "w") as outfile:
            json.dump(data, outfile)
        
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


def db_rank_sec(term):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        sql_string = "SELECT \
                        page_id, \
                        sec_id, \
                        (ts_rank_cd(array[0.1, 0.2, 0.4, 1.0], all_tsv, \
                            to_tsquery('english', %s), 0) / num_words) AS rank \
                      FROM \
                        search_pages, to_tsquery('english', %s) query \
                      WHERE \
                        query @@ all_tsv \
                      ORDER BY page_id ASC, sec_id ASC;"
        cur.execute(sql_string, (term, term))
        res = cur.fetchall()
        #for comparing
        data = {}
        data['sections'] = []
        print("top 10 ranking sections:")
        for row in res:
            page_id = row[0]
            section_id = row[1]
            rank = row[2]
            data['sections'].append({
                'page_id': page_id,
                'section_id': section_id,
                'rank': rank
            })
            #print("page_id: " + str(page_id) + " section_id: " + str(section_id) + " ranking: " + str(rank))
        print("num results: " + str(len(res)))
        with open("../../rating_comparison/data/section_" + term + ".txt", "w") as outfile:
            json.dump(data, outfile)
        
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
        '''
        cur.execute("DROP FUNCTION page_title_weighted_tsv_trigger CASCADE;")
        cur.execute("DROP FUNCTION sec_title_weighted_tsv_trigger CASCADE;")
        '''
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
