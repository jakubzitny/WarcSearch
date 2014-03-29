## WarcSearch

WarcSearch searches Web Archives (WARC) and based on a user's query retrieves results ranked by TFIDF. Information Retrieval HW 1 by Thanduxolo Zwane and Jakub Zitny.

#### INDEXING STATS
- small archive - 100 records 2MB
- large archive - 37k records 1GB
- 2 cores OS X (2 thread) - small 2.5s, large 180s
- 4 cores Ubuntu VPS (1t) - small 2.8s, large 70.2s
- 4 cores Ubuntu VPS (2t) - small 2.0s, large 64.7s
- 4 cores Ubuntu VPS (4t) - small 1.9s, large 63.7s <-
- 4 cores Ubuntu VPS (8t) - small 2.3s, large 69.5s

###### Original sequential
- small archive 6-10s to parse and index
- large archive 20m to parse and index!
- sequential version profiling - *jsoup.Parse()* and *indexWriter.addDocument()*

## Sample data

- [here](http://140.124.183.31/en0000/)
- [here](https://bitbucket.org/jakubzitny/warcsearch/downloads)

#### Usage

	java -jar warcsearch.jar -a /path/to/archive.warc -q query [-t number_of_threads]

#### Thx to
1. [Apache Lucene](https://lucene.apache.org/core/4_7_0/index.html) - indexing and search
2. [Java Web Archive Toolkit](https://sbforge.org/display/JWAT/Documentation) - parsing WARC
3. [Apache Commons CLI](http://commons.apache.org/proper/commons-cli/) - parsing cli arguments
4. my mother