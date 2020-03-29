import psycopg2
from os import path
from config import config
from wiki_pipeline import WikiScratcher


def get_connection():
    params = config()
    # connect to the PostgreSQL server
    print("Connecting to the PostgreSQL database...")
    return psycopg2.connect(**params)


def create_tables(conn, cur):
    # create table wiki_pages: page title is weight as A
    cur.execute("CREATE TABLE wiki_pages( \
                    id SERIAL PRIMARY KEY, \
                    page_title VARCHAR (50) UNIQUE NOT NULL, \
                    weighted_title_tsv tsvector, \
                    num_sections smallint, \
                    num_words integer);")
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
                    
    # create table page sections: section title is weight as B and section text as C
    cur.execute("CREATE TABLE page_sections( \
                    id BIGSERIAL PRIMARY KEY, \
                    page_id INTEGER NOT NULL, \
                    sec_title VARCHAR (50), \
                    sec_title_tsv tsvector, \
                    sec_text_tsv tsvector, \
                    num_words integer, \
                    FOREIGN KEY (page_id) REFERENCES wiki_pages (id) ON DELETE CASCADE);")
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
                    
    # create view
    sql_view_string = "CREATE VIEW search_pages AS \
                       SELECT \
                            text 'wiki_pages' AS origin_table, id AS id, weighted_title_tsv AS unified_tsv \
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
    

# prints all rows of the documents table
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


# inserts a row into the documents table
def db_insert_wiki(wiki_category, num_pages):
    wiki = WikiScratcher(category=wiki_category)
    wiki_data = wiki.get_sections(num_pages=num_pages)
    
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        for page_name, section_data in wiki_data.items():
            page_num_words = 0
            sec_titles = section_data.keys()
            sql_string = "INSERT INTO wiki_pages (page_title, num_sections) \
                            VALUES (%s, %s) RETURNING id;"
            cur.execute(sql_string, (page_name, len(sec_titles)))
            id_of_new_row = cur.fetchone()[0]
            
            for sec_title, sec_text in section_data.items():
                sec_num_words = len(sec_text)
                page_num_words += sec_num_words
                sql_string = "INSERT INTO page_sections (page_id, sec_title, \
                                                         sec_text_tsv, num_words) \
                                VALUES (%s, %s, setweight(to_tsvector('english', COALESCE(%s,'')), 'C'), %s);"
                cur.execute(sql_string, (id_of_new_row, sec_title, sec_text, sec_num_words))
            sql_string = "UPDATE wiki_pages SET num_words = %s WHERE id = %s;"
            cur.execute(sql_string, (page_num_words, id_of_new_row))
            conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


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


# seaches the table for a specific term in title and content and returns the id of the document
def db_search_term(term):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        sql_string = "SELECT \
                        id, \
                        ARRAY_AGG(origin_table) AS origin_table, \
                        ARRAY_AGG(unified_tsv) AS unified_tsv \
                      FROM \
                        search_pages \
                      WHERE \
                        to_tsquery('english', %s) @@ unified_tsv \
                      GROUP BY \
                        id";
        cur.execute(sql_string, (term,))
        res = cur.fetchall()
        print(res)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


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


def db_drop_all_tables():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DROP FUNCTION page_title_weighted_tsv_trigger CASCADE;")
        cur.execute("DROP FUNCTION sec_title_weighted_tsv_trigger CASCADE;")
        cur.execute("DROP TABLE wiki_pages CASCADE;")
        cur.execute("DROP TABLE page_sections CASCADE;")
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")
