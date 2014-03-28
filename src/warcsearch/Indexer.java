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

public class Indexer {
	
	private static final int HITS_PER_PAGE = 10;
	private static final String DEFAULT_SEARCH_FIELD = "text";
	private static final String TMP_DIR = System.getProperty("java.io.tmpdir") + "/indexer";
	
	private static StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
	//private static Directory _index = new RAMDirectory();
	private FSDirectory _dir;
	private IndexWriterConfig _config;
	private IndexWriter _writer;
	private IndexReader _reader;
	private IndexSearcher _searcher;
	private TopScoreDocCollector _collector;
	
	public Indexer() {
		_config = new IndexWriterConfig(Version.LUCENE_47, analyzer);
		_config.setOpenMode(OpenMode.CREATE); // to overwrite existing indexes
		_config.setSimilarity(new DefaultSimilarity()); // DefaultSimilarity is subclass of TFIDFSimilarity
		try {	
			_dir = FSDirectory.open(new File(TMP_DIR));
		} catch (IOException e) {
			System.err.println("There was a problem with tmp dir in your system. Please re-run with -i.");
			System.err.println(e.getMessage());
			System.err.println(e.getStackTrace());
		}
	}

	/**
	 * 
	 * @param docs
	 */
	public void write(ArrayList<Document> docs) {
		try {
			openWriter();
			for (Document doc: docs) {
				_writer.addDocument(doc);
			}
			closeWriter();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public ArrayList<Result> search(String querystr) throws IOException {
		Query query = prepareQuery(querystr);
		openReader();
		_searcher.search(query, _collector);
		ScoreDoc[] hits = _collector.topDocs().scoreDocs;
		ArrayList<Result> results = new ArrayList<Result>();
		for (int i = 0; i < hits.length; ++i) {
			int docId = hits[i].doc;
			Document d = _searcher.doc(docId);
			results.add(new Result(d, docId, hits[i].score));
		}
		closeReader();
		return results;
	}
	
	private Query prepareQuery(String querystr) {
		Query q = null;
		try {
			q = new QueryParser(Version.LUCENE_47, DEFAULT_SEARCH_FIELD, analyzer).parse(querystr);
		} catch (org.apache.lucene.queryparser.classic.ParseException e) {
			e.printStackTrace();
		}
		return q;
	}
	
	private void openWriter() throws IOException {
		_writer = new IndexWriter(_dir, _config);
	}
	
	private void closeWriter() throws IOException {
		_writer.close();
	}
	
	private void openReader() {
		try {
			_reader = DirectoryReader.open(_dir);
			_searcher = new IndexSearcher(_reader);
			_collector = TopScoreDocCollector.create(HITS_PER_PAGE, true);
		} catch (Exception e) {
			// TODO
			e.printStackTrace();
		}
	}
	
	private void closeReader() {
		try {
			_reader.close();
		} catch (Exception e) {
			// TODO
			e.printStackTrace();
		}
	}
	
}
