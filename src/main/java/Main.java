/**
 * @author jbrincefield, etownsend, vkeeler, esmith
 * @createdOn 10/2/2024 at 6:11 PM
 * @projectName RDBL1
 * @packageName PACKAGE_NAME;
 */
public class Main {
    public static void main(String[] args) {
        String filePath = "path/to/your/xmlfile.xml";
        Customers customers = XML_Parser.parseXML(filePath);

        if (customers != null) {
            Neo4jUploader uploader = new Neo4jUploader("bolt://localhost:7687", "neo4j", "password");
            for (Customer customer : customers.getCustomers()) {
                uploader.uploadCustomerData(customer);
            }
            uploader.close();
        }
    }
}