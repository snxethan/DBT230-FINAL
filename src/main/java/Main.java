/**
 * @author jbrincefield, etownsend, vkeeler, esmith
 * @createdOn 10/2/2024 at 6:11 PM
 * @projectName RDBL1
 * @packageName PACKAGE_NAME;
 */
public class Main {
    public static void main(String[] args) {
        // Ensure the path to the large XML file is provided as an argument
        if (args.length < 1) {
            System.err.println("Please provide the path to the XML file as an argument.");
            System.exit(1);
        }

        // Call the XMLParser main method with the provided file path
        XMLParser.main(args);
    }
}