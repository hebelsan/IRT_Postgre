import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from os import path

from config import config
from wiki_pipeline import WikiScratcher2


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
                    page_title text UNIQUE NOT NULL, \
                    weighted_title_tsv tsvector, \
                    num_sections smallint, \
                    num_words integer, \
                    num_lexems integer);")
    '''
    cur.execute("CREATE FUNCTION page_title_weighted_tsv_trigger() RETURNS trigger AS $$ \
                    begin \
                    new.weighted_title_tsv := \
                    setweight(to_tsvector('english', COALESCE(new.page_title,'')), 'A'); \
                    return new; \
                    end \
                    $$ LANGUAGE plpgsql;")
    cur.execute("CREATE TRIGGER upd_page_title_tsvector BEFORE INSERT OR UPDATE \
                    ON wiki_pages FOR EACH ROW EXECUTE PROCEDURE page_title_weighted_tsv_trigger();")
    cur.execute("CREATE INDEX idx_page_title \
                    ON wiki_pages \
                    USING GIN (weighted_title_tsv);")
    '''
                    
    # create table page sections: section title is weight as B and section text as C
    cur.execute("CREATE TABLE page_sections( \
                    id BIGSERIAL PRIMARY KEY, \
                    page_id INTEGER NOT NULL, \
                    sec_title text, \
                    sec_title_tsv tsvector, \
                    sec_text_tsv tsvector, \
                    num_words integer, \
                    FOREIGN KEY (page_id) REFERENCES wiki_pages (id) ON DELETE CASCADE);")
    '''
    cur.execute("CREATE FUNCTION sec_title_weighted_tsv_trigger() RETURNS trigger AS $$ \
                    begin \
                    new.sec_title_tsv := \
                    setweight(to_tsvector('english', COALESCE(new.sec_title,'')), 'B'); \
                    return new; \
                    end \
                    $$ LANGUAGE plpgsql;")
    cur.execute("CREATE TRIGGER upd_sec_title_tsvector BEFORE INSERT OR UPDATE \
                    ON page_sections FOR EACH ROW EXECUTE PROCEDURE sec_title_weighted_tsv_trigger();")
    cur.execute("CREATE INDEX idx_sec_text \
                    ON page_sections \
                    USING GIN (sec_text_tsv);")
    cur.execute("CREATE INDEX idx_sec_title \
                    ON page_sections \
                    USING GIN (sec_title_tsv);")
    '''
    
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
                        IF length( new.weighted_title_tsv) > 0 THEN \
                            INSERT INTO \
                                search_pages (id, all_tsv) \
                                VALUES (new.id, new.weighted_title_tsv); \
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
                        IF length( new.sec_title_tsv) > 0 THEN \
                            INSERT INTO \
                                search_pages (id, all_tsv) \
                                VALUES (new.page_id, new.sec_title_tsv); \
                        END IF; \
                        IF length( new.sec_text_tsv) > 0 THEN \
                            INSERT INTO \
                                search_pages (id, all_tsv) \
                                VALUES (new.page_id, new.sec_text_tsv); \
                        END IF; \
                        RETURN new; \
                    END; \
                    $BODY$ \
                    language plpgsql;")
    cur.execute("CREATE TRIGGER trig_copy_section \
                    AFTER INSERT ON page_sections \
                    FOR EACH ROW \
                    EXECUTE PROCEDURE func_copy_section();")
    
    
    '''
    # create view
    sql_view_string = "CREATE VIEW search_pages AS \
                       SELECT \
                            text 'page_title' AS origin_table, id AS id, weighted_title_tsv AS all_tsv \
                       FROM \
                            wiki_pages \
                       UNION ALL \
                       SELECT \
                            text 'sec_text' AS origin_table, page_id AS id, sec_text_tsv AS s_text \
                       FROM \
                            page_sections \
                       UNION ALL \
                       SELECT \
                            text 'sec_title' AS origin_table, page_id AS id, sec_title_tsv AS s_title \
                       FROM \
                            page_sections;"
    cur.execute(sql_view_string)
    '''
    
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
                sql_string = "INSERT INTO wiki_pages \
                                    (page_title, num_sections, weighted_title_tsv) \
                                VALUES \
                                    (%s, %s, setweight(to_tsvector('english', COALESCE(%s,'')), 'A')) \
                                RETURNING id, tsvector_num_lexemes(weighted_title_tsv);"
                cur.execute(sql_string, (page_name, len(sec_titles), page_name))
                tuple = cur.fetchall()[0]
                id_new_row = tuple[0]
                num_lex_page_title = tuple[1]
                num_word_page_title = len(page_name)
                num_words = num_word_page_title
                num_lex = num_lex_page_title
                
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
                    num_lex += num_lex_title + num_lex_text
                    
                    sql_string = "INSERT INTO page_sections \
                                        (page_id, sec_title, sec_title_tsv, sec_text_tsv, num_words) \
                                    VALUES \
                                        (%s, %s, \
                                        setweight(to_tsvector('english', COALESCE(%s,'')), 'B'), \
                                        setweight(to_tsvector('english', COALESCE(%s,'')), 'C'), \
                                        %s);"
                    cur.execute(sql_string,
                        (id_new_row, sec_title, sec_title, sec_text, sec_num_words))
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
#   inserts some test data to play around with
##
def db_insert_testdata():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # first page
        sql_string = "INSERT INTO \
                        wiki_pages (page_title, num_sections, num_words) \
                      VALUES \
                        ('first page test title', 2, 30) RETURNING id;"
        cur.execute(sql_string)
        id_of_new_row = cur.fetchone()[0]
        sql_string = "INSERT INTO page_sections (page_id, sec_title, \
                                                 sec_text_tsv, num_words) \
                        VALUES (%s, 'first page first section title', \
                            setweight(to_tsvector('english', COALESCE('hello from the first section', '')), 'C'), 10);"
        cur.execute(sql_string, (id_of_new_row,))
        sql_string = "INSERT INTO page_sections (page_id, sec_title, \
                                                 sec_text_tsv, num_words) \
                        VALUES (%s, 'first page second section title', \
                            setweight(to_tsvector('english', COALESCE('this is just a test', '')), 'C'), 10);"
        cur.execute(sql_string, (id_of_new_row,))
        
        # second page
        sql_string = "INSERT INTO \
                        wiki_pages (page_title, num_sections, num_words) \
                      VALUES \
                            ('second page test title about viruses', 1, 100) RETURNING id;"
        cur.execute(sql_string)
        id_of_new_row = cur.fetchone()[0]
        sql_string = "INSERT INTO page_sections (page_id, sec_title, \
                                                 sec_text_tsv, num_words) \
                        VALUES (%s, 'viruses are bad', \
                            setweight(to_tsvector('english', COALESCE('there are various different \
                            types of viruses for example the virus named corona', '')), 'c'), 10);"
        cur.execute(sql_string, (id_of_new_row,))
        
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
def db_search_term(term):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # add typecast for tsv
        cur.execute("SELECT NULL::TSVECTOR")
        tsv_oid = cur.description[0][1]
        TSV = psycopg2.extensions.new_type((tsv_oid,), "TSVECTOR", cast_tsv)
        psycopg2.extensions.register_type(TSV)
        
        # get term frequency
        '''
        sql_term_frequency = "SELECT ndoc, nentry \
                                FROM ts_stat('SELECT \
                                                res.all_tsv \
                                             FROM wiki_pages AS pag \
                                             INNER JOIN \
                                             (SELECT \
                                                id, \
                                                tsvector_agg(all_tsv) AS all_tsv \
                                              FROM \
                                                search_pages \
                                              GROUP BY \
                                                id) \
                                              AS res \
                                                on pag.id = res.id') \
                                WHERE word = %s;"
        cur.execute(sql_term_frequency, (term,))
        pair = cur.fetchone()
        ndoc = pair[0] #  the number of documents (tsvectors) the word occurred in
        nentry = pair[1] # the total number of occurrences of the word.
        print(pair)
        '''

        sql_string = "SELECT \
                        res.id, \
                        res.rank / pag.num_words AS rank, \
                        pag.num_words \
                     FROM wiki_pages AS pag \
                     INNER JOIN \
                     (SELECT \
                        id, \
                        sum(ts_rank_cd(array[0.1, 0.2, 0.4, 1.0], all_tsv, to_tsquery('english', %s), 0)) AS rank \
                      FROM \
                        search_pages, to_tsquery('english', %s) query \
                      WHERE \
                        query @@ all_tsv \
                      GROUP BY \
                        id) \
                      AS res \
                        on pag.id = res.id;"
        cur.execute(sql_string, (term, term))
        res = cur.fetchall()
        #print(res)
        for row in res:
            page_id = row[0]
            rank = row[1]  # origins could be 'page_title', 'sec_title' or 'sec_text'
            #print(rank)
            page_word_count = row[2]
        print("nunm results: " + str(len(res)))
        
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
