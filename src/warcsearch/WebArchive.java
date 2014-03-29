package warcsearch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

/**
 * WebArchive class for parsing the WARC full of WarcRecords
 * parses WARC file into array of ExtendedWarcRecords
 * is able to return the records as array of LuceneDocuments for indexing
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
	
	/** number of consumer threads */
	private final int _threadNo;
	
	/**
	 * constructor
	 * @param archiveLoc location of WARC archive
	 */
	public WebArchive(String archiveLoc, LinkedBlockingQueue<ExtendedWarcRecord> queue, int threadNo) {
		_queue = queue;
		_archiveLoc = archiveLoc;
		_threadNo = threadNo;
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
					_queue.put(new ExtendedWarcRecord(record)); // <--
				} else if (type.equals(WARC_TYPE_WARCINFO)) { 
					// nothing special just the warc main header
					// stupid library returns it as a payload
					// ignore
				} else {
					System.out.println("INFO found WARC-Type " + type);
				}
			}
			/** PARALLELIZE SOMEDAY
			List<Thread> threads = new ArrayList<Thread>();
			Semaphore semaphore = new Semaphore(1);
	    	for (int i = 0; i < _threadNo; i++) {
	    		threads.add(new Thread(new WebArchiveTask(reader, _queue, semaphore, i)));
	        }
	    	for (Thread t: threads)
	    		t.start();
	    	for (Thread t: threads)
	    		t.join();
			in.close();
			*/
			// indicate last record in queue with empty record
			// for each consumer thread (ugly hack, whatever)
			for (int i = 0; i < _threadNo; i++)
				_queue.put(new ExtendedWarcRecord());
		} catch (Exception e) {
			System.err.println("There was a problem with parsing the Web Archive.");
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	/**
	 * WebArchiveTask runnable for enables producing to be parallel
	 * but not really when used with WARC library, because it sucks
	 * meiyou deep copy(!) and meiyou thread safe (how come?)
	 */
	@SuppressWarnings("unused")
	private class WebArchiveTask implements Runnable {
	
		private final WarcReader _reader;
		private final LinkedBlockingQueue<ExtendedWarcRecord> _queue;
		private final int _threadNumber;
		private final Semaphore _semaphore;
		
		/**
		 * constructor keeping reference to opened reader, queue and semaphore
		 * keeps thread number for debugging purposes
		 * @param reader
		 * @param queue
		 * @param semaphore
		 * @param threadNumber
		 */
		public WebArchiveTask (WarcReader reader, LinkedBlockingQueue<ExtendedWarcRecord> queue,
				Semaphore semaphore, int threadNumber) {
			_reader = reader;
			_queue = queue;
			_threadNumber = threadNumber;
			_semaphore = semaphore;
		}
		
		/*
		 * runs the parallel producing
		 * the main task is the jsoup html parsing inside ExtendedWarcRecord()
		 */
		public void run() {
			try {
				while (true) {
					_semaphore.acquire();
					WarcRecord record = _reader.getNextRecord();
					_semaphore.release();
					if (record == null) break;
					String type = record.getHeader(WARC_TYPE).value;
					if (type.equals(WARC_TYPE_RESPONSE)) {
						_queue.put(new ExtendedWarcRecord(record));
					} else if (type.equals(WARC_TYPE_WARCINFO)) { 
						// nothing special just the warc main header
						// stupid library returns it as a payload
						// ignore
					} else {
						System.out.println("INFO found WARC-Type " + type);
					}
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
}
