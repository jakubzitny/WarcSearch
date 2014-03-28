package warcsearch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.lucene.queryparser.classic.QueryParser;

/**
 * Indexer class handles index creation and search
 * indexed entity is Document inserted via write methid in ArrayList
 * configured statically to use TFIDFSimilarity for ranking
 */
public class Indexer {
	
	/** number of results */
	private static final int HITS_PER_PAGE = 10;
	
	/** default search field in Document */
	private static final String DEFAULT_SEARCH_FIELD = "text";
	
	/** system-specific temp directory with place to write indexes */
	private static final String TMP_DIR = System.getProperty("java.io.tmpdir") + "/indexer";
	
	/** lucene analyzer */
	private static StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
	
	/** lucene directory - use RAMDirectory to store indexes in memory, FSDirectory in file */
	//private static Directory _index = new RAMDirectory();
	private FSDirectory _dir;
	
	/** lucene index writer, reader, searcher, collector */
	private IndexWriterConfig _config;
	private IndexWriter _writer;
	private IndexReader _reader;
	private IndexSearcher _searcher;
	private TopScoreDocCollector _collector;
	
	/**
	 * Indexer constructor
	 * prepares Indexer default needs
	 * IndexWriterConfig specifies IndexWriters behaviour
	 * FSDirectory opens tmp file as index storage
	 */
	public Indexer() {
		_config = new IndexWriterConfig(Version.LUCENE_47, analyzer);
		_config.setOpenMode(OpenMode.CREATE); // to overwrite existing indexes
		_config.setSimilarity(new DefaultSimilarity()); // DefaultSimilarity is subclass of TFIDFSimilarity
		try {	
			_dir = FSDirectory.open(new File(TMP_DIR));
		} catch (IOException e) {
			System.err.println("There was a problem with tmp dir in your system.");
			System.err.println(e.getMessage());
			e.getStackTrace();
		}
	}

	/**
	 * input point
	 * indexes array of Documents
	 * @param docs array of Lucene Documents
	 */
	public void write(ArrayList<Document> docs) {
		try {
			openWriter();
			for (Document doc: docs) {
				_writer.addDocument(doc);
			}
			closeWriter();
		} catch (IOException e) {
			System.err.println("There was a problem with indexing Documents.");
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	/**
	 * output point
	 * searches query in indexed Documents
	 * @param querystr string o user's query
	 * @return array of Result objects sorted by ranking
	 */
	public ArrayList<Result> search(String querystr) {
		Query query = prepareQuery(querystr);
		ArrayList<Result> results = new ArrayList<Result>();
		try {
			openReader();
			_searcher.search(query, _collector);
			ScoreDoc[] hits = _collector.topDocs().scoreDocs;
			for (int i = 0; i < hits.length; ++i) {
				int docId = hits[i].doc;
				Document d = _searcher.doc(docId);
				results.add(new Result(d, docId, hits[i].score));
			}
			closeReader();
		} catch (IOException e) {
			System.err.println("There was a problem with searching Documents.");
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		return results;
	}
	
	/**
	 * creates Query from user's input query string
	 * @param querystr
	 * @return Query
	 */
	private Query prepareQuery(String querystr) {
		Query query = null;
		try {
			query = new QueryParser(Version.LUCENE_47, DEFAULT_SEARCH_FIELD, analyzer).parse(querystr);
		} catch (org.apache.lucene.queryparser.classic.ParseException e) {
			System.err.println("There was a problem with parsing your query.");
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		return query;
	}
	
	/**
	 * opens configured IndexWriter for indexing
	 * @throws IOException
	 */
	private void openWriter() throws IOException {
		_writer = new IndexWriter(_dir, _config);
	}
	
	/**
	 * closes opened IndexWriter
	 * @throws IOException
	 */
	private void closeWriter() throws IOException {
		_writer.close();
	}
	
	/**
	 * opens configured IndexReader for indexing
	 * @throws IOException
	 */
	private void openReader() throws IOException {
		try {
			_reader = DirectoryReader.open(_dir);
			_searcher = new IndexSearcher(_reader);
			_collector = TopScoreDocCollector.create(HITS_PER_PAGE, true);
		} catch (Exception e) {
			// TODO
			e.printStackTrace();
		}
	}
	
	/**
	 * closes opened IndexReader
	 * @throws IOException
	 */
	private void closeReader() throws IOException {
		_reader.close();
	}
	
}
