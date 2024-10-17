/**
 * @author jbrincefield, etownsend, vkeeler, esmith
 * @createdOn 10/2/2024 at 6:11 PM
 * @projectName RDBL1
 * @packageName PACKAGE_NAME;
 */
public class Main {
    public static void main(String[] args) {
        DataObjectController.connectNEO4J();
        //DataObjectController.createDataObjectFromFile();
        DataObjectController.closeNEO4J();
    }
}
