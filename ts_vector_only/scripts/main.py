from database import *


if __name__ == '__main__':
    while(True):
        print('0: exit')
        print('1: init database')
        print('2: create necessary tables')
        print('3: show tables')
        print('4: insert wiki pages')
        print('5: empty tables')
        print('6: search term')
        print('7: insert test data')
        print('8: drop all databases and functions')
        num = input('')
        if num == '0':
            exit()
        elif num == '1':
            db_name = input('database name: ')
            user_name = input('database user name: ')
            db_create_init_file(db_name, user_name)
        elif num == '2':
            db_create_tables()
        elif num == '3':
            db_show_tables()
        elif num == '4':
            category = input('wikipedia category: ')
            pages = input('number of pages: ')
            batch_size = input('batch size: ')
            if int(pages) % int(batch_size) != 0:
                print('number of pages must be dividable by batch_size')
                continue
            db_insert_wiki(wiki_category=category, num_pages=int(pages), batch_size=int(batch_size))
        elif num == '5':
            db_reset()
        elif num == '6':
            print('term could be a single word or multiple connected with &(AND), |(OR), !(NOT), <N>(Followed by)')
            term = input('term: ')
            db_search_term(term)
        elif num == '7':
            db_insert_testdata()
        elif num == '8':
            db_drop_all_tables()
        else:
            print('please insert a valid input')
    
    
    '''
    wiki = WikiScratcher(category='viruses')
    dic = wiki.get_sections(num_pages=15)
    for dics in dic.values():
        for secname in dics.keys():
            print(secname)
        print()
    '''
