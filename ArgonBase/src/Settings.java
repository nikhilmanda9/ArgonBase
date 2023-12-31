public class Settings {
    static String prompt = "argonsql> ";
    static String version = "v1.0";
    static String copyright = "(c) 2023 Team Argon";
    static boolean isExit = false;
    static String userDataDir = "data/userData";
    static String catalogDir = "data/catalog";

    static String argonBaseTables = "argonbase_tables";
    static String argonBaseColumns = "argonbase_columns";


    public static boolean isExit() {
        return isExit;
    }

    public static void setExit(boolean e) {
        isExit = e;
    }

    public static String getPrompt() {
        return prompt;
    }

    public static void setPrompt(String s) {
		prompt = s;
	}

    public static String getVersion() {
        return version;
    }

    public static void setVersion(String version) {
		Settings.version = version;
	}

    public static String getCopyright() {
        return copyright;
    }

    public static void setCopyright(String copyright) {
		Settings.copyright = copyright;
	}

    public static String getUserDataDirectory(){
        return userDataDir;
    }
    public static String getCatalogDirectory(){
        return catalogDir;
    }

    /** ***********************************************************************
	 *  Static method definitions
	 */

    /**
     * @param s The String to be repeated
     * @param num The number of time to repeat String s.
     * @return String A String object, which is the String s appended to itself num times.
     */
    public static String line(String s,int num) {
        return String.valueOf(s).repeat(Math.max(0, num));
    }
}