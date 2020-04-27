from mediawiki import MediaWiki
import time

##
#   WikiScratcher, wiki_api is buggy
#   WikiScratcher2, more flexibility
##


def ignore_section(section_title):
    return section_title in ['External links', 'Bibliography',
                             'References', 'See also', 'Sources',
                             'Further reading'] # 'Notes' still pass


class WikiScratcher:
    def __init__(self, category):
        self.wikipedia = MediaWiki(url='https://en.wikipedia.org/w/api.php',
                                   user_agent='wiki-data-loader', lang='en')
        self.category = category
    
    # returns {pagename: {sectionname: section}, ....}
    def get_sections(self, num_pages):
        res = {}
        page_titles = self.wikipedia.categorymembers(self.category,
                                    results=num_pages, subcategories=False)
        if (len(page_titles) < num_pages):
            print('Only ' + str(len(page_titles)) + ' pages found !!!')
        for p_title in page_titles:
            res[p_title] = {}
            p = self.wikipedia.page(p_title)
            # add the summary
            res[p_title]['summary'] = p.summary
            # add all other sections
            section_titles = p.sections
            for s_title in section_titles:
                # ignore sections like 'references' or 'see also'
                if (self._ignore_section(s_title)):
                    continue
                section_text = p.section(s_title)
                # ignore empty sections which are in fact most likely subheaders
                if len(section_text) > 0:
                    res[p_title][s_title] = section_text
        return res



import wikipediaapi

class WikiScratcher2:
    ##
    #   categroy    = string: of wikipedia category
    #   max_level   = int: max numbers of subcategories to search for
    #   max_pages   = int: the number of pages to be returned
    #   return_empty_sec   = boolean: should empty sections be returned -> because double captions
    ##
    def __init__(self, category, max_level=2, max_pages=10, return_empty_sec=False):
        self.wikipedia = wikipediaapi.Wikipedia(language='en', timeout=200.0)
        self.category = category
        self.return_empty_sec = return_empty_sec
        self.max_level = max_level
        self.max_pages = max_pages
        self.num_pages = 0
        self.pages = []
        self.result_dic = {}
    
    def __print_categorymembers(self, categorymembers, level=0):
        for c in categorymembers.values():
            if c.ns == wikipediaapi.Namespace.CATEGORY and level < self.max_level and self.num_pages < self.max_pages:
                self.__print_categorymembers(c.categorymembers, level=level + 1)
            elif c.ns == 0 and self.num_pages < self.max_pages: # could also try “main namespace” or “mainspace” instead of ns == 0 becuase main namespace also includes ns=12,112
                page_py = self.wikipedia.page(c.title)
                if (page_py.exists() and c.title not in self.pages):
                    self.num_pages += 1
                    self.pages.append(c.title)
                    self.result_dic[c.title] = {}
                    self.result_dic[c.title]['summary'] = page_py.summary
                    self.__print_sections(page_py.sections, c.title, 0)
    
    def __print_sections(self, sections, p_title, level=0):
        for s in sections:
            if not (s.text == "" and self.return_empty_sec) and not ignore_section(s.title):
                self.result_dic[p_title][s.title] = s.text
            self.__print_sections(s.sections, p_title, level + 1)
    
    # returns {pagename: {sectionname: section}, ....}
    def get_data(self, num_pages):
        self.result_dic = {}
        self.num_pages = 0
        self.max_pages = num_pages
        cat = self.wikipedia.page("Category:" + self.category)
        self.__print_categorymembers(cat.categorymembers)
        return self.result_dic


if __name__ == "__main__":
    wiki = WikiScratcher2(category='sports', max_level=2, max_pages=10, return_empty_sec=False)
    dic = wiki.get_data(num_pages=2)
    dic = wiki.get_data(num_pages=2)
