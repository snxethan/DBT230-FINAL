import org.neo4j.driver.*;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

public class XMLToNeo4jCustomerOrder {

    // Neo4j URI, username, and password
    private static final String NEO4J_URI = "bolt://localhost:7687";
    private static final String NEO4J_USER = "neo4j";
    private static final String NEO4J_PASSWORD = System.getenv("NEO4J_PASSWORD");

    // Cache to store already processed nodes (reset after each batch)
    private static Map<String, Boolean> customerCache = new HashMap<>();
    private static Map<String, Boolean> orderCache = new HashMap<>();
    private static Map<String, Boolean> productCache = new HashMap<>();

    private static final int BATCH_SIZE = 500000;  // Increased batch size
    private static int processedCount = 0;  // Keep track of processed records

    public static void main(String[] args) {
        // Set up StAX reader for large XML file
        try {
            FileInputStream fis = new FileInputStream(System.getenv("XML_FILE_PATH"));
            XMLInputFactory factory = XMLInputFactory.newInstance();
            XMLStreamReader reader = factory.createXMLStreamReader(fis);

            // Connect to Neo4j
            try (Driver driver = GraphDatabase.driver(NEO4J_URI, AuthTokens.basic(NEO4J_USER, NEO4J_PASSWORD));
                 Session session = driver.session()) {

                String currentElement = null;
                String customerId = null;
                String name = null;
                String email = null;
                String age = null;
                String orderId = null;
                String orderTotal = null;
                String orderLineId = null;
                String productId = null;
                String qty = null;
                String price = null;
                String lineTotal = null;

                // Use transaction batching for efficient insertion
                Transaction tx = session.beginTransaction();

                while (reader.hasNext()) {
                    int event = reader.next();

                    switch (event) {
                        case XMLStreamConstants.START_ELEMENT:
                            currentElement = reader.getLocalName();

                            // Extract Customer information
                            if ("CustomerId".equals(currentElement)) {
                                customerId = reader.getElementText();
                            } else if ("Name".equals(currentElement)) {
                                name = reader.getElementText();
                            } else if ("Email".equals(currentElement)) {
                                email = reader.getElementText();
                            } else if ("Age".equals(currentElement)) {
                                age = reader.getElementText();
                            }

                            // Extract Order information (make sure you handle nested structure correctly)
                            else if ("OrderId".equals(currentElement)) {
                                orderId = reader.getElementText();
                            } else if ("Total".equals(currentElement) && orderId != null) {
                                orderTotal = reader.getElementText();
                            }

                            // Extract OrderLine information within Lines -> OrderLine
                            else if ("OrderLineId".equals(currentElement)) {
                                orderLineId = reader.getElementText();
                            } else if ("ProductId".equals(currentElement)) {
                                productId = reader.getElementText();
                            } else if ("Qty".equals(currentElement)) {
                                qty = reader.getElementText();
                            } else if ("Price".equals(currentElement)) {
                                price = reader.getElementText();
                            } else if ("Total".equals(currentElement) && orderLineId != null) {
                                lineTotal = reader.getElementText();
                            }
                            break;

                        case XMLStreamConstants.END_ELEMENT:
                            String endElement = reader.getLocalName();

                            // Handle the end of OrderLine
                            if ("OrderLine".equals(endElement)) {
                                if (orderLineId != null && !orderLineId.isEmpty() && orderId != null && !orderId.isEmpty() && productId != null && !productId.isEmpty()) {
                                    // Batch execution of queries to reduce transaction overhead
                                    String query = "MERGE (ol:OrderLine {orderLineId: $orderLineId, price: $price, qty: $qty, total: $lineTotal}) " +
                                            "MERGE (o:Order {orderId: $orderId}) " +
                                            "MERGE (p:Product {productId: $productId}) " +
                                            "MERGE (o)-[:CONTAINS]->(ol) " +
                                            "MERGE (ol)-[:PRODUCT_OF]->(p)";
                                    tx.run(query, Values.parameters("orderLineId", orderLineId, "price", price, "qty", qty, "lineTotal", lineTotal, "orderId", orderId, "productId", productId));

                                    processedCount++;
                                }

                                // Reset variables for the next order line
                                orderLineId = null;
                                productId = null;
                                qty = null;
                                price = null;
                                lineTotal = null;

                                // Commit the transaction every BATCH_SIZE entries
                                if (processedCount % BATCH_SIZE == 0) {
                                    tx.commit(); // Commit the transaction
                                    System.out.println("Committed " + processedCount + " records.");

                                    // Clean up after batch
                                    customerCache.clear();
                                    orderCache.clear();
                                    productCache.clear();
                                    System.gc();  // Force garbage collection

                                    tx = session.beginTransaction(); // Start a new transaction
                                }
                            }

                            // Handle the end of Order
                            if ("Order".equals(endElement)) {
                                if (orderId != null && !orderId.isEmpty()) {
                                    String query = "MERGE (o:Order {orderId: $orderId, total: $orderTotal}) " +
                                            "MERGE (c:Customer {customerId: $customerId}) " +
                                            "MERGE (c)-[:PLACED]->(o)";
                                    tx.run(query, Values.parameters("orderId", orderId, "orderTotal", orderTotal, "customerId", customerId));
                                }

                                orderId = null;
                                orderTotal = null;
                            }

                            // Handle the end of Customer
                            if ("Customer".equals(endElement)) {
                                if (customerId != null && !customerId.isEmpty() && !customerCache.containsKey(customerId)) {
                                    // Create the Customer node if it doesn't exist in the cache
                                    String query = "MERGE (c:Customer {customerId: $customerId, name: $name, email: $email, age: $age})";
                                    tx.run(query, Values.parameters("customerId", customerId, "name", name, "email", email, "age", age));
                                    customerCache.put(customerId, true);
                                }

                                // Reset customer variables after processing
                                customerId = null;
                                name = null;
                                email = null;
                                age = null;
                            }
                            break;
                    }
                }

                // Final commit after the last batch
                tx.commit();
                System.out.println("Final commit: Total processed records: " + processedCount);

                // Final memory cleanup
                customerCache.clear();
                orderCache.clear();
                productCache.clear();
                System.gc();  // Final garbage collection

            } catch (Exception e) {
                e.printStackTrace();
            }

            reader.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
