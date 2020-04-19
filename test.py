import wikipediaapi

class WikiScratcher:
    ##
    #   categroy    = string: of wikipedia category
    #   max_level   = int: max numbers of subcategories to search for
    #   max_pages   = int: the number of pages to be returned
    #   rm_empty_sec   = boolean: should empty sections be returned -> because double captions
    ##
    def __init__(self, category, max_level=2, max_pages=10, rm_empty_sec=False):
        self.wikipedia = wikipediaapi.Wikipedia(language='en', timeout=200.0)
        self.category = category
        self.rm_empty_sec = rm_empty_sec
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
                    print("%s: %s (ns: %d)" % ("*" * (level + 1), c.title, c.ns))
                    self.num_pages += 1
                    self.pages.append(c.title)
                    self.result_dic[c.title] = {}
                    self.result_dic[c.title]['summary'] = page_py.summary
                    self.__print_sections(page_py.sections, c.title, 0)
    
    def __print_sections(self, sections, p_title, level=0):
        for s in sections:
            if not (s.text == "" and self.rm_empty_sec):
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
    wiki = WikiScratcher(category='sports', max_level=2, max_pages=10, rm_empty_sec=False)
    dic = wiki.get_data(num_pages=2)
    dic = wiki.get_data(num_pages=2)
    
