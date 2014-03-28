package warcsearch;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.cli.*;

/**
 * Information Retrieval HW 1
 * WarcSearch searches Warc archives and based
 * on a user's query retrieves ranked results
 * 
 * @author Jakub Zitny <t102012001@ntut.edu.tw>
 * @author Thanduxolo Zwane <tTODO@ntut.edu.tw>
 * 
 * TODO
 * - parallel
 * - messages
 * - comments
 * 
 */
public class WarcSearch {

	private static CommandLineParser parser = new GnuParser();
	private static HelpFormatter formatter = new HelpFormatter();
	private static Options options = new Options();
	private static Option[] option_array = new Option[] {
		new Option("h", "help", false, "displays this help message"),
		new Option("i", "interactive", false, "runs the program in interactive mode"),
		new Option("a", "archive", true, "path to warc file"),
		new Option("q", "query", true, "query"),
	};
	
	public static void run(String query, String archive) throws IOException {
		WebArchive wa = new WebArchive(archive);
		Indexer indexer = new Indexer();
		indexer.write(wa.getLuceneDocuments());
		ArrayList<Result> results = indexer.search(query);
		// display
		System.out.println("Found " + results.size() + " hits for query " + query);
		System.out.println("===========================");
		System.out.println("Rank\tDocNumber\tDocId\tScore");
		int i = 0;
		for (Result r: results) {
			System.out.println(++i + "\t"+ r.toString());
		}
	}
	
	/**
	 * @param args command line arguments
	 */
	public static void main(String[] args) {
		for (Option o: option_array) {
			options.addOption(o);
		}
		
		try {
	        CommandLine cli = parser.parse(options, args);
			if (cli.hasOption("a") && cli.hasOption("q")) {
				String query = cli.getOptionValue("q");
				String archive = cli.getOptionValue("a");
				// TODO check input?
				run(query, archive);
				
			} else if (cli.hasOption("i")) {
				// TODO ask user for input
			} else {
				formatter.printHelp("WarcSearch", options );
			}
		} catch (IOException e) {
	        System.err.println("Loading file failed.  Reason: " + e.getMessage() );
	    } catch (ParseException exp) {
	        System.err.println("Parsing failed.  Reason: " + exp.getMessage() );
	    }
		
	}

}
