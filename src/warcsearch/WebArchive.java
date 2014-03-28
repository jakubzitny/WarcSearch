package warcsearch;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import org.apache.lucene.document.Document;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

/**
 * WebArchive class for parsing the WARC full of WarcRecords
 * parses WARC file into array of ExtendedWarcRecords
 * is able to return the records as array of LuceneDocuments for indexing
 * TODO - ugly readFile!
 */
public class WebArchive {

	/** ExtendedWarcRecords */
	private ArrayList<ExtendedWarcRecord> _records = new ArrayList<ExtendedWarcRecord>();
	
	/**
	 * constructor
	 * initiates the parsing of given archive
	 * stores the parsed results
	 * @param archiveLoc location of WARC archive
	 */
	public WebArchive(String archiveLoc) {
		_records = readFile(archiveLoc);
	}
	
	/**
	 * returns the ExtendedWarcRecords as Lucene documents
	 * @return documents array of LuceneDocuments
	 */
	public ArrayList<Document> getLuceneDocuments() {
		ArrayList<Document> documents = new ArrayList<Document>();
		for (ExtendedWarcRecord r: _records) {
			documents.add(r.getLuceneDocument());
		}
		return documents;
	}
	
	/**
	 * reads and parses WARC file into ExtendedWarcRecords
	 * a bit ugly though
	 * @param archiveLoc
	 * @return parsed array of ExtendedWarcRecord
	 */
	private ArrayList<ExtendedWarcRecord> readFile(String archiveLoc) {
		try {
			InputStream in = new FileInputStream(new File(archiveLoc));
			WarcReader reader = WarcReaderFactory.getReader(in);
			ArrayList<ExtendedWarcRecord> records = new ArrayList<ExtendedWarcRecord>();
			WarcRecord record;
			int i = 0;
			while ((record = reader.getNextRecord()) != null ) {
				if (i > 0) // skip warc header (stupid library)
					records.add(new ExtendedWarcRecord(record));
				i++;
			}
			in.close();
			return records;
		} catch (Exception e) {
			System.err.println( "There was a problem with parsing the Web Archive.");
			System.err.println(e.getMessage());
			e.printStackTrace();
			System.exit(1);
			return null; // shut up eclipse
		}
	}
	
}
