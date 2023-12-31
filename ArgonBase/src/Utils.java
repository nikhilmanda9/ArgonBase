public class Utils {

    /**
     *  Display the splash screen
     */
    public static void splashScreen() {
        System.out.println(printSeparator("-",80));
        // Display the string.
        System.out.println("Welcome to ArgonBase");
        System.out.println("ArgonBase Version " + Settings.getVersion());
        System.out.println(Settings.getCopyright());
        System.out.println("\nType \"help;\" to display supported commands.");
        System.out.println(printSeparator("-",80));
    }

    public static String printSeparator(String s, int len) {
        return String.valueOf(s).repeat(Math.max(0, len));
    }

}
