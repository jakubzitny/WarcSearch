package warcsearch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
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
public class Indexer implements Runnable {
	
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
	private IndexWriterConfig _config; //TODO fix
	//private IndexWriter _writer;
	private IndexReader _reader;
	private IndexSearcher _searcher;
	private TopScoreDocCollector _collector;
	
	/** produced documents go to this shared queue */
	private final LinkedBlockingQueue<Document> _queue;
	
	/**
	 * Indexer constructor
	 * prepares Indexer default needs
	 * IndexWriterConfig specifies IndexWriters behaviour
	 * FSDirectory opens tmp file as index storage
	 */
	public Indexer(LinkedBlockingQueue<Document> queue) {
		_queue = queue;
		_config = new IndexWriterConfig(Version.LUCENE_47, analyzer);
		//_config.setOpenMode(OpenMode.CREATE); // to overwrite existing indexes
		_config.setSimilarity(new DefaultSimilarity()); // DefaultSimilarity is subclass of TFIDFSimilarity
		try {	
			_dir = FSDirectory.open(new File(TMP_DIR));
			// TODO check if works!!
			// reset the indexfile
			IndexWriter iw = openWriter(OpenMode.CREATE);
			closeWriter(iw);
		} catch (IOException e) {
			System.err.println("There was a problem with tmp dir in your system.");
			System.err.println(e.getMessage());
			e.getStackTrace();
		}
	}

	/**
	 * runs the indexing task
	 */
	@Override
	public void run() {
		int i = 0;
		while(true) {
		    try {
                write(_queue.take(), OpenMode.APPEND);
                i++;
                if (i%500 == 0) {
                	System.out.println("INFO indexed documents: " + i);
                }
            } catch (InterruptedException e) {
            	// TODO better
                break;
            }
        }
		final LinkedList<Document> remainingDocuments = new LinkedList<Document>();
		_queue.drainTo(remainingDocuments);
		for (Document doc: remainingDocuments){
			write(doc, OpenMode.APPEND);
		}
	}
	
	/**
	 * input point
	 * indexes array of Documents
	 * @param docs array of Lucene Documents
	 */
	public void write(Document doc, OpenMode openMode) {
		try {
			IndexWriter iw = openWriter(openMode);
			iw.addDocument(doc);
			closeWriter(iw);
		} catch (IOException e) {
			System.err.println("There was a problem with indexing Document.");
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
	private IndexWriter openWriter(OpenMode openMode) throws IOException {
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, analyzer);
		config.setOpenMode(openMode);
		config.setSimilarity(new DefaultSimilarity());
		return new IndexWriter(_dir, config);
	}
	
	/**
	 * closes opened IndexWriter
	 * @throws IOException
	 */
	private void closeWriter(IndexWriter indexWriter) throws IOException {
		indexWriter.close();
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
