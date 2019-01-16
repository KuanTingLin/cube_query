from urllib.request import urlopen
import urllib.parse
import json
from project.DAO.dao import web_urls


def gallery_items(current_query):

    solr_tuples = [
        # text in search box
        ('q', "mens shirt gap"),
        # how many products do I want to return
        ('rows', current_query['rows_per_page']),
        # offset for pagination
        ('start', current_query['start_row'] * current_query['rows_per_page']),
        # example of a default sort,
        # for search phrase leave blank to allow
        # for relevancy score sorting
        ('sort', 'price asc, popularity desc'),
        # which fields do I want returned
        ('fl', 'product_title, price, code, image_file'),
        # enable facets and facet.pivots
        ('facet', 'on'),
        # allow for unlimited amount of facets in results
        ('facet.limit', '-1'),
        # a facet has to have at least one
        # product in it to be a valid facet
        ('facet.mincount', '1'),
        # regular facets
        ('facet.fields', ['gender', 'style', 'material']),
        # nested facets
        ('facet.pivot', 'brand,collection'),
        # edismax is Solr's multifield phrase parser
        ('defType', 'edismax'),
        # fields to be queried
        # copyall: all facets of a product with basic stemming
        # copyallphonetic: phonetic spelling of facets
        ('qf', 'copyall copyallphonetic'),
        # give me results that match most fields
        # in qf [copyall, copyallphonetic]
        ('tie', '1.0'),
        # format response as JSON
        ('wt', 'json')
    ]

    solr_url = web_urls['SOLR_URL']
    # enocde for URL format
    encoded_solr_tuples = urllib.parse.urlencode(solr_tuples)
    complete_url = solr_url + encoded_solr_tuples
    connection = urlopen(complete_url)
    raw_response = json.loads(connection)


def apply_facet_filters(self):
    if self.there_are_facets():
        for facet, facet_arr in self.facet_filter.items():
            if len(facet_arr) > 0:
                new_facet_arr = []
                for a_facet in facet_arr:
                    new_facet_arr.append("{0}: \"{1}\"".format(facet, a_facet))
                self.solr_args.append(('fq', ' OR '.join(new_facet_arr)))