from mediawiki import MediaWiki

'''
The user agent string to use when making requests defults to a library version
but per the MediaWiki API documentation it recommends setting a
unique one and not using the library’s default user-agent string
'''


class WikiScratcher:
    def __init__(self, category):
        self.wikipedia = MediaWiki(url='https://en.wikipedia.org/w/api.php',
                                   user_agent='wiki-data-loader', lang='en')
        self.category = category
    
    def _ignore_section(self, section_title):
        return section_title in ['External links', 'Bibliography',
                                 'References', 'See also'] # 'Notes' still pass
    
    # returns {pagename: {sectionname: section}, ....}
    def get_sections(self, num_pages):
        res = {}
        page_titles = self.wikipedia.categorymembers(self.category,
                                    results=num_pages, subcategories=False)
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
