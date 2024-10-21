import org.neo4j.driver.*;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.FileInputStream;
import java.util.*;

public class XMLToNeo4jCustomerOrder {

    //neo4j connection details
    private static final String NEO4J_URI = "bolt://localhost:7687";
    private static final String NEO4J_USER = "neo4j";
    private static final String NEO4J_PASSWORD = System.getenv("NEO4J_PASSWORD");

    //hash sets for caching
    private static Set<String> customerCache = new HashSet<>();
    private static Set<String> orderCache = new HashSet<>();
    private static Set<String> productCache = new HashSet<>();

    //batch size vars
    private static final int BATCH_SIZE = 5000;
    private static int processedCount = 0;

    //the current order id used for each order
    private static String currentOrderId = null;

    //batches for storing data
    private static List<Map<String, Object>> customerBatch = new ArrayList<>();
    private static List<Map<String, Object>> orderBatch = new ArrayList<>();
    private static List<Map<String, Object>> orderLineBatch = new ArrayList<>();

    // Temporary storage for order lines until OrderId is available
    private static List<Map<String, Object>> deferredOrderLines = new ArrayList<>();

    public static void main(String[] args) {
        try {
            FileInputStream fis = new FileInputStream(System.getenv("XML_FILE_PATH"));
            XMLInputFactory factory = XMLInputFactory.newInstance();
            XMLStreamReader reader = factory.createXMLStreamReader(fis);

            try (Driver driver = GraphDatabase.driver(NEO4J_URI, AuthTokens.basic(NEO4J_USER, NEO4J_PASSWORD));
                 Session session = driver.session()) {

                // Initialize variables for reading XML
                String currentElement = null;
                String customerId = null;
                String name = null;
                String email = null;
                String age = null;
                String orderTotal = null;
                String orderLineId = null;
                String productId = null;
                String qty = null;
                String price = null;
                String lineTotal = null;

                // Start transaction
                Transaction tx = session.beginTransaction();

                // Read XML data
                while (reader.hasNext()) {
                    int event = reader.next();

                    // Process XML elements
                    switch (event) {
                        case XMLStreamConstants.START_ELEMENT: // Start of an element
                            currentElement = reader.getLocalName();

                            // Extract Customer information
                            if ("CustomerId".equals(currentElement)) { // CustomerId
                                customerId = reader.getElementText(); // Read CustomerId
                                System.out.println("CustomerId: " + customerId);
                            } else if ("Name".equals(currentElement)) { // Name
                                name = reader.getElementText(); // Read Name
                                System.out.println("Name: " + name);
                            } else if ("Email".equals(currentElement)) { // Email
                                email = reader.getElementText(); // Read Email
                                System.out.println("Email: " + email);
                            } else if ("Age".equals(currentElement)) { // Age
                                age = reader.getElementText(); // Read Age
                                System.out.println("Age: " + age);
                            }

                            // Extract Order information
                            else if ("OrderId".equals(currentElement)) {
                                currentOrderId = reader.getElementText();  // Store active orderId
                                System.out.println("OrderId: " + currentOrderId);

                                // Now process any deferred order lines after we have the OrderId
                                if (currentOrderId != null && !currentOrderId.isEmpty()) {
                                    for (Map<String, Object> orderLine : deferredOrderLines) {
                                        orderLine.put("orderId", currentOrderId);  // Attach the correct OrderId to each order line
                                        orderLineBatch.add(orderLine);
                                    }
                                    deferredOrderLines.clear();  // Clear the deferred list after processing
                                }
                            }

                            // Extract OrderLine information and temporarily store it until OrderId is available
                            else if ("OrderLineId".equals(currentElement)) { // OrderLineId
                                orderLineId = reader.getElementText(); // Read OrderLineId
                                System.out.println("OrderLineId: " + orderLineId);
                            } else if ("ProductId".equals(currentElement)) { // ProductId
                                productId = reader.getElementText(); // Read ProductId
                                System.out.println("ProductId: " + productId);
                            } else if ("Qty".equals(currentElement)) { // Qty
                                qty = reader.getElementText(); // Read Qty
                                System.out.println("Qty: " + qty);
                            } else if ("Price".equals(currentElement)) { // Price
                                price = reader.getElementText(); // Read Price
                                System.out.println("Price: " + price);
                            } else if ("Total".equals(currentElement) && orderLineId != null) { // Total
                                lineTotal = reader.getElementText(); // Read Total
                                System.out.println("Total: " + lineTotal);
                            }
                            break;

                        case XMLStreamConstants.END_ELEMENT: // End of an element
                            String endElement = reader.getLocalName(); // Get the name of the end element

                            // Handle the end of Customer
                            if ("Customer".equals(endElement)) { // End of Customer
                                if (customerId != null && !customerId.isEmpty()) { // Check for valid customerId
                                    Map<String, Object> customerMap = new HashMap<>(); // Create a new customer map
                                    customerMap.put("customerId", customerId); // Add customerId to the map
                                    customerMap.put("name", name); // Add name to the map
                                    customerMap.put("email", email); // Add email to the map
                                    customerMap.put("age", age); // Add age to the map
                                    customerBatch.add(customerMap); // Add the customer map to the batch

                                    processedCount++; // Increment processed count
                                } else {
                                    System.out.println("Skipping customer with null or empty customerId.");
                                }

                                // Reset Customer fields
                                customerId = null;
                                name = null;
                                email = null;
                                age = null;
                            }

                            // Handle the end of Order
                            if ("Order".equals(endElement)) {
                                if (currentOrderId != null && !currentOrderId.isEmpty()) { // Check for valid orderId
                                    Map<String, Object> orderMap = new HashMap<>(); // Create a new order map
                                    orderMap.put("orderId", currentOrderId); // Add orderId to the map
                                    orderMap.put("orderTotal", orderTotal); // Add orderTotal to the map
                                    orderMap.put("customerId", customerId);  // Link to customer
                                    orderBatch.add(orderMap); // Add the order map to the batch

                                    processedCount++;
                                } else {
                                    System.out.println("Skipping order with null or empty orderId.");
                                }

                                // Reset Order fields
                                currentOrderId = null;
                                orderTotal = null;
                            }

                            // Handle the end of OrderLine
                            if ("OrderLine".equals(endElement)) {
                                if (orderLineId != null && !orderLineId.isEmpty() &&
                                        productId != null && !productId.isEmpty()) {

                                    // Prepare the order line data for batch insertion, defer processing until OrderId is available
                                    Map<String, Object> orderLineMap = new HashMap<>();
                                    orderLineMap.put("orderLineId", orderLineId);
                                    orderLineMap.put("price", price);
                                    orderLineMap.put("qty", qty);
                                    orderLineMap.put("lineTotal", lineTotal);
                                    orderLineMap.put("productId", productId);

                                    // If OrderId is already available, process the order line immediately
                                    if (currentOrderId != null && !currentOrderId.isEmpty()) {
                                        orderLineMap.put("orderId", currentOrderId);
                                        orderLineBatch.add(orderLineMap);
                                    } else {
                                        // Otherwise, defer processing until the OrderId is read
                                        deferredOrderLines.add(orderLineMap);
                                    }

                                    processedCount++;
                                } else {
                                    System.out.println("Skipping order line due to missing data: " +
                                            "OrderLineId=" + orderLineId + ", ProductId=" + productId);
                                }

                                // Reset OrderLine fields after processing
                                orderLineId = null;
                                productId = null;
                                qty = null;
                                price = null;
                                lineTotal = null;
                            }

                            // Commit batch if the batch size is reached
                            if (processedCount % BATCH_SIZE == 0) { //this is where we commit the batch, mod checks if we have reached the batch size
                                executeBatches(tx);  // Execute the batch insertions
                                tx.commit(); // Commit the transaction
                                System.out.println("Committed " + processedCount + " records."); // Log the commit

                                // Clear caches and reset transaction
                                customerCache.clear();
                                orderCache.clear();
                                productCache.clear();
                                System.gc();

                                tx = session.beginTransaction(); // Start a new transaction
                            }
                    }
                }

                // Final commit after processing all data
                executeBatches(tx); // Execute any remaining batches
                tx.commit(); // Commit the final transaction
            } catch (Exception e) {
                e.printStackTrace(); // Print any exceptions
            }

            reader.close(); // Close the XML reader

        } catch (Exception e) {
            e.printStackTrace(); // Print any exceptions
        }
    }

    private static void executeBatches(Transaction tx) {
        if (!customerBatch.isEmpty()) {
            // Insert customers
            tx.run("UNWIND $batchData as row " +
                            "MERGE (c:Customer {customerId: row.customerId}) " +
                            "SET c.name = row.name, c.email = row.email, c.age = row.age",
                    Collections.singletonMap("batchData", customerBatch));
            customerBatch.clear();
        }

        if (!orderBatch.isEmpty()) {
            // Insert orders
            tx.run("UNWIND $batchData as row " +
                            "MERGE (o:Order {orderId: row.orderId}) " +
                            "SET o.total = row.orderTotal " +
                            "MERGE (c:Customer {customerId: row.customerId}) " +
                            "MERGE (c)-[:PLACED]->(o)",
                    Collections.singletonMap("batchData", orderBatch));
            orderBatch.clear();
        }

        if (!orderLineBatch.isEmpty()) {
            // Insert order lines
            tx.run("UNWIND $batchData as row " +
                            "MERGE (ol:OrderLine {orderLineId: row.orderLineId}) " +
                            "SET ol.price = row.price, ol.qty = row.qty, ol.total = row.lineTotal " +
                            "MERGE (o:Order {orderId: row.orderId}) " +
                            "MERGE (p:Product {productId: row.productId}) " +
                            "MERGE (o)-[:CONTAINS]->(ol) " +
                            "MERGE (ol)-[:PRODUCT_OF]->(p)",
                    Collections.singletonMap("batchData", orderLineBatch));
            orderLineBatch.clear();
        }
    }
}
