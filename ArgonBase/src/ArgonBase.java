import java.io.*;
import java.util.Scanner;
//import java.nio.ByteBuffer;
//import java.io.File;
//import java.io.FileReader;



/**
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
	 *  Each time the semicolon (;) delimiter is entered, the userCommand 
	 *  String is re-populated.
	 */
    static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    /** ***********************************************************************
	 *  Main method
	 */
    public static void main(String[] args) {
        
        //Initialize and create a new data directory if it doesn't already exist
        //This directory will store the metadata

        /* Display the welcome screen */
		Utils.splashScreen();

		/* Variable to hold user input from the prompt */
		String userCommand = ""; 

		while(!Settings.isExit()) {
			System.out.print(Settings.getPrompt());
			/* Strip newlines and carriage returns */
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim();
			Commands.parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");

    }
}
