package warcsearch;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import org.apache.lucene.document.Document;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

public class WebArchive {

	private ArrayList<ExtendedWarcRecord> _records = new ArrayList<ExtendedWarcRecord>();
	
	public WebArchive() {
		// TODO
	}
	
	public WebArchive(String archiveLoc) {
		_records = readFile(archiveLoc);
	}
	
	public ArrayList<Document> getLuceneDocuments() {
		ArrayList<Document> bodies = new ArrayList<Document>();
		for (ExtendedWarcRecord r: _records) {
			bodies.add(r.getLuceneDocument());
		}
		return bodies;
	}
	
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
			// TODO handle!
			//  if (record.hasErrors()) {
			//      errors += record.getValidationErrors().size();
			//  }
			}
			in.close();
			return records;
		} catch (Exception e) {
			System.err.println( "Loading file failed.  Reason: " + e.getMessage() );
			e.printStackTrace();
			System.exit(1);
			return null; // shut up eclipse
		}
	}
	
}
