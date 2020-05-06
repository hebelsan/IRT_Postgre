import sys
sys.path.append("./wiki")
from wiki_pipeline import WikiScratcher2

import pysolr
import json



##
#   inserts a row into the documents table
##
def solr_insert_wiki(solr, categroy, num_pages, batch_size):
    wiki = WikiScratcher2(category=categroy)
    batch_iter = num_pages // batch_size
    sec_id = 0
    
    for it in range(batch_iter):
        print('batch: ' + str(it*batch_size))
        wiki_data = wiki.get_data(num_pages=batch_size)
        
        for page_name, section_data in wiki_data.items():
            solr.add([
            {
                "id": sec_id,
                "section_text": page_name
            }])
            sec_id += 1
            
            for sec_title, sec_text in section_data.items():
                solr.add([
                {
                    "id": sec_id,
                    "section_text": sec_title
                }])
                sec_id += 1
                
                solr.add([
                {
                    "id": sec_id,
                    "section_text": sec_text
                }])
                sec_id += 1
            
            
def insert_test(solr):
    solr.add([
    {
        "id": 0,
        "section_text": "This is a section text"
    }])



if __name__ == "__main__":
    solr = pysolr.Solr('http://localhost:8983/solr/mycore/', always_commit=True)
    print(solr.ping())
    solr_insert_wiki(solr, "sports", 2000, 200)
