from database import *


if __name__ == '__main__':
    while(True):
        print('0: exit')
        print('1: init database')
        print('2: create necessary tables')
        print('3: show tables')
        print('4: insert wiki pages')
        #print('5: insert from file')
        #print('6: delete row')
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
            db_insert_wiki(wiki_category=category, num_pages=int(pages))
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
