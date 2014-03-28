package warcsearch;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;

public class IndexerTask implements Runnable {

	private IndexWriter _writer;
	private ExtendedWarcRecord _rec;
	
	public IndexerTask(IndexWriter writer, ExtendedWarcRecord rec) {
		_writer = writer;
		_rec = rec;
	}
	
	@Override
	public void run() {
		try {
			_writer.addDocument(_rec.getLuceneDocument());
	    	_writer.commit();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
