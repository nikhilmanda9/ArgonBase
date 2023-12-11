import java.io.*;
import java.util.Scanner;

/**
 * 	Main driver for ArgonBase, a rudimentary database engine.
 *  @author Team Argon
 *  @version 1.0
 *  <b>
 *  <p>This is an example of how to create an interactive prompt</p>
 *  </b>
 *
 */
public class ArgonBase {

    /*
     *  The Scanner class is used to collect user commands from the prompt
     *  There are many ways to do this. This is just one.
     *
     *  Each time the semicolon ; delimiter is entered, the userCommand
     *  String is re-populated.
     */
    static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    /** ***********************************************************************
     *  Main method
     */
    public static void main(String[] args) throws IOException {

        //Initialize the user data directory
        File dataDir = new File("data");

        //Check if the user data directory doesn't exist
        if (!dataDir.exists()){
            System.out.println("The data directory doesn't exist. Trying to create it now...");
            Directory.createDirectory(dataDir);
        }
        else {
            Directory.createCatalogDirectory();
        }

        /* Display the welcome screen */
        Utils.splashScreen();

        /* Variable to hold user input from the prompt */
        String userCommand;

        while(!Settings.isExit()) {
            System.out.print(Settings.getPrompt());
            /* Strip newlines and carriage returns */
            userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim();
            Commands.parseUserCommand(userCommand);
        }
        System.out.println("Exiting...");
    }
}