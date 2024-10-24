import org.neo4j.driver.*;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.FileInputStream;
import java.util.*;

public class XMLToNeo4jCustomerOrder {

    // Neo4j connection details
    private static final String NEO4J_URI = "bolt://localhost:7687";
    private static final String NEO4J_USER = "neo4j";
    private static final String NEO4J_PASSWORD = System.getenv("NEO4J_PASSWORD");

    // Caches for batching
    private static Set<String> customerCache = new HashSet<>();
    private static Set<String> orderCache = new HashSet<>();
    private static Set<String> productCache = new HashSet<>();

    // Batch size and processed count
    private static final int BATCH_SIZE = 15000;
    private static int totalProcessedCount = 0; // Tracks total processed records
    private static int currentBatchCount = 0; // Tracks current batch processed records

    // Current order ID used for processing
    private static String currentOrderId = null;

    // Batches for storing customer, order, and order line data
    private static List<Map<String, Object>> customerBatch = new ArrayList<>();
    private static List<Map<String, Object>> orderBatch = new ArrayList<>();
    private static List<Map<String, Object>> orderLineBatch = new ArrayList<>();

    // Temporary storage for order lines until OrderId is available
    private static List<Map<String, Object>> deferredOrderLines = new ArrayList<>();

    // Class-level session and transaction variables
    private static Session session;
    private static Transaction tx;

    public static void main(String[] args) {
        try {
            System.out.println("Starting XML to Neo4j import...");
            FileInputStream fis = new FileInputStream(System.getenv("XML_FILE_PATH"));
            XMLInputFactory factory = XMLInputFactory.newInstance();
            XMLStreamReader reader = factory.createXMLStreamReader(fis);

            try (Driver driver = GraphDatabase.driver(NEO4J_URI, AuthTokens.basic(NEO4J_USER, NEO4J_PASSWORD))) {
                session = driver.session();  // Initialize session
                tx = session.beginTransaction();  // Start transaction

                String currentElement = null;
                String customerId = null, name = null, email = null, age = null;
                String orderTotal = null, orderLineId = null, productId = null, qty = null, price = null, lineTotal = null;

                while (reader.hasNext()) {
                    int event = reader.next();

                    switch (event) {
                        case XMLStreamConstants.START_ELEMENT:
                            currentElement = reader.getLocalName();

                            // Process Customer
                            if ("CustomerId".equals(currentElement)) {
                                customerId = reader.getElementText();
                            } else if ("Name".equals(currentElement)) {
                                name = reader.getElementText();
                            } else if ("Email".equals(currentElement)) {
                                email = reader.getElementText();
                            } else if ("Age".equals(currentElement)) {
                                age = reader.getElementText();
                            }

                            // Process Order
                            else if ("OrderId".equals(currentElement)) {
                                currentOrderId = reader.getElementText();
                                if (currentOrderId != null && !currentOrderId.isEmpty()) {
                                    for (Map<String, Object> orderLine : deferredOrderLines) {
                                        orderLine.put("orderId", currentOrderId);
                                        orderLineBatch.add(orderLine);
                                    }
                                    deferredOrderLines.clear();
                                }
                            }

                            // Process OrderLine
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

                            // End Customer
                            if ("Customer".equals(endElement)) {
                                if (customerId != null && !customerId.isEmpty()) {
                                    Map<String, Object> customerMap = new HashMap<>();
                                    customerMap.put("customerId", customerId);
                                    customerMap.put("name", name);
                                    customerMap.put("email", email);
                                    customerMap.put("age", age);
                                    customerBatch.add(customerMap);
                                    currentBatchCount++;
                                }
                                customerId = null; name = null; email = null; age = null;
                            }

                            // End Order
                            if ("Order".equals(endElement)) {
                                if (currentOrderId != null && !currentOrderId.isEmpty()) {
                                    Map<String, Object> orderMap = new HashMap<>();
                                    orderMap.put("orderId", currentOrderId);
                                    orderMap.put("orderTotal", orderTotal);
                                    orderMap.put("customerId", customerId);
                                    orderBatch.add(orderMap);
                                    currentBatchCount++;
                                }
                                currentOrderId = null; orderTotal = null;
                            }

                            // End OrderLine
                            if ("OrderLine".equals(endElement)) {
                                if (orderLineId != null && !orderLineId.isEmpty() && productId != null && !productId.isEmpty()) {
                                    Map<String, Object> orderLineMap = new HashMap<>();
                                    orderLineMap.put("orderLineId", orderLineId);
                                    orderLineMap.put("price", price);
                                    orderLineMap.put("qty", qty);
                                    orderLineMap.put("lineTotal", lineTotal);
                                    orderLineMap.put("productId", productId);

                                    if (currentOrderId != null && !currentOrderId.isEmpty()) {
                                        orderLineMap.put("orderId", currentOrderId);
                                        orderLineBatch.add(orderLineMap);
                                    } else {
                                        deferredOrderLines.add(orderLineMap);
                                    }
                                    currentBatchCount++;
                                }
                                orderLineId = null; productId = null; qty = null; price = null; lineTotal = null;
                            }

                            // Commit the batch after reaching batch size
                            checkBatchLimitAndCommit();  // Now without parameters
                            break;
                    }
                }

                // Final commit for any remaining records
                if (!customerBatch.isEmpty() || !orderBatch.isEmpty() || !orderLineBatch.isEmpty()) {
                    executeBatches();
                    tx.commit();
                    totalProcessedCount += currentBatchCount;  // Update total processed count with the remaining records
                    System.out.println("Final commit of remaining records. Total processed: " + totalProcessedCount);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Method to reset the batch variables
    private static void resetBatches() {
        customerBatch.clear();
        orderBatch.clear();
        orderLineBatch.clear();
        deferredOrderLines.clear();
        System.gc();
    }

    // Method to execute batched transactions
    private static void executeBatches() {
        if (!customerBatch.isEmpty()) {
            tx.run("UNWIND $batchData as row " +
                            "MERGE (c:Customer {customerId: row.customerId}) " +
                            "SET c.name = row.name, c.email = row.email, c.age = row.age",
                    Collections.singletonMap("batchData", customerBatch));
        }

        if (!orderBatch.isEmpty()) {
            tx.run("UNWIND $batchData as row " +
                            "MERGE (o:Order {orderId: row.orderId}) " +
                            "SET o.total = row.orderTotal " +
                            "MERGE (c:Customer {customerId: row.customerId}) " +
                            "MERGE (c)-[:PLACED]->(o)",
                    Collections.singletonMap("batchData", orderBatch));
        }

        if (!orderLineBatch.isEmpty()) {
            tx.run("UNWIND $batchData as row " +
                            "MERGE (ol:OrderLine {orderLineId: row.orderLineId}) " +
                            "SET ol.price = row.price, ol.qty = row.qty, ol.total = row.lineTotal " +
                            "MERGE (o:Order {orderId: row.orderId}) " +
                            "MERGE (p:Product {productId: row.productId}) " +
                            "MERGE (o)-[:CONTAINS]->(ol) " +
                            "MERGE (ol)-[:PRODUCT_OF]->(p)",
                    Collections.singletonMap("batchData", orderLineBatch));
        }
    }

    // Commit batches for all types if any of them reaches the batch size
    private static void checkBatchLimitAndCommit() {
        if (currentBatchCount >= BATCH_SIZE) {
            executeBatches();
            tx.commit();
            totalProcessedCount += currentBatchCount;
            System.out.println("Committed " + totalProcessedCount + " records.");
            resetBatches();  // Clear caches and reset transaction state
            currentBatchCount = 0;  // Reset batch counter
            tx = session.beginTransaction();  // Start a new transaction for the next batch
        }
    }
}
