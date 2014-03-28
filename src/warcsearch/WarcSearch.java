package warcsearch;

import java.util.ArrayList;
import org.apache.commons.cli.*;

/**
 * Information Retrieval HW 1
 * WarcSearch searches Warc archives and based
 * on a user's query retrieves ranked results
 * 
 * @author Jakub Zitny <t102012001@ntut.edu.tw>
 * @author Thanduxolo Zwane <tTODO@ntut.edu.tw>
 * @since Fri Mar 28 18:41:59 HKT 2014
 * 
 * TODO
 * - parallel
 * - messages
 * - comments
 * 
 */
public class WarcSearch {

	/** CLI parser options */
	private static CommandLineParser parser = new GnuParser();
	private static HelpFormatter formatter = new HelpFormatter();
	private static Options options = new Options();
	private static Option[] option_array = new Option[] {
		new Option("h", "help", false, "displays this help message"),
		new Option("i", "interactive", false, "runs the program in interactive mode"),
		new Option("a", "archive", true, "path to warc file"),
		new Option("q", "query", true, "query"),
	};
	
	/**
	 * parses archive into ExtendedWarcRecords and to LuceneDocuments
	 * indexes prepared LuceneDocuments
	 * runs a search for given query
	 * displays results
	 * @param query
	 * @param archive
	 */
	public static void run(String query, String archive) {
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
	 * main
	 * parses the command line arguments
	 * if run in interctive mode then asks for query and archive and runs search
	 * if run with proper arguments then just runs search
	 * otherwise displays help message
	 * @param args command line arguments
	 */
	public static void main(String[] args) {
		// prepares options (stupid library)
		for (Option o: option_array) {
			options.addOption(o);
		}
		// parses the options
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
		} catch (ParseException e) {
	        System.err.println("There was a problem with parsing arguments failed.");
	        System.err.println(e.getMessage());
	        e.getStackTrace();
	    }
		
	}

}
