
#Add a document to the index. You need (at least) two parameters:
#doc: The document to be indexed.
#index: The location of the local index storing the discovered documents.
def index(doc, index):
    #TODO: Implement me
    pass


#Crawl the web. You need (at least) two parameters:
#frontier: The frontier of known URLs to crawl. You will initially populate this with your seed set of URLs and later maintain all discovered (but not yet crawled) URLs here.
#index: The location of the local index storing the discovered documents.
def crawl(frontier, index):
    #TODO: Implement me

    # seed set
    # open database & fill frontier
    # make requests
    # database layout
    # language detection
        # HTML attribute
    # determine if document is relevant
        # to Tübingen
        # for a search engine (ignore javascript, css?)
        # detect duplicate
    # save document to database
        # last modified?
    # extract URLs & add to frontier
        # check whether URLs have been visited already

    pass
