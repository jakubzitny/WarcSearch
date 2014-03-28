package warcsearch;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.LinkedBlockingQueue;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

/**
 * WebArchive class for parsing the WARC full of WarcRecords
 * parses WARC file into array of ExtendedWarcRecords
 * is able to return the records as array of LuceneDocuments for indexing
 * TODO - ugly readFile!
 */
public class WebArchive implements Runnable {
	
	/** warc header types */
	private static final String WARC_TYPE = "WARC-Type";
	private static final String WARC_TYPE_RESPONSE = "response";
	private static final String WARC_TYPE_WARCINFO = "warcinfo";
	
	/** produced documents go to this shared queue */
	private final LinkedBlockingQueue<ExtendedWarcRecord> _queue;
	
	/** path to WARC archive */
	private final String _archiveLoc;
	
	/**
	 * constructor
	 * @param archiveLoc location of WARC archive
	 */
	public WebArchive(String archiveLoc, LinkedBlockingQueue<ExtendedWarcRecord> queue) {
		_queue = queue;
		_archiveLoc = archiveLoc;
	}
	
	/**
	 * runs this thread
	 *initiates the parsing of given archive
	 */
	@Override
	public void run() {
		readFile(_archiveLoc);
	}
	
	/**
	 * reads and parses WARC file into ExtendedWarcRecords
	 * a bit ugly though
	 * a LOT slow though
	 * possible enhancements
	 * - use db, solr or elasticsearch as storage
	 * - index continuously 
	 * - parallel producer-consumer pattern !!
	 * - ??
	 * @param archiveLoc
	 * @return parsed array of ExtendedWarcRecord
	 */
	private void readFile(String archiveLoc) {
		try {
			InputStream in = new FileInputStream(new File(archiveLoc));
			WarcReader reader = WarcReaderFactory.getReader(in);
			WarcRecord record;
			while ((record = reader.getNextRecord()) != null) {
				String type = record.getHeader(WARC_TYPE).value;
				if (type.equals(WARC_TYPE_RESPONSE)) {
					// TODO enhance
					_queue.put(new ExtendedWarcRecord(record));
				} else if (type.equals(WARC_TYPE_WARCINFO)) { 
					// nothing special just the warc main header
					// stupid library returns it as a payload
					// ignore
				} else {
					System.out.println("INFO found WARC-Type " + type);
				}
			}
			in.close();
			// indicate last record in queue with empty record
			_queue.put(new ExtendedWarcRecord());
		} catch (Exception e) {
			System.err.println("There was a problem with parsing the Web Archive.");
			System.err.println(e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}
	
}
