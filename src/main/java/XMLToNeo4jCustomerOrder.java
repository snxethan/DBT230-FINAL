import org.neo4j.driver.*;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.FileInputStream;
import java.util.*;

public class XMLToNeo4jCustomerOrder {

    private static final String NEO4J_URI = "bolt://localhost:7687";
    private static final String NEO4J_USER = "neo4j";
    private static final String NEO4J_PASSWORD = System.getenv("NEO4J_PASSWORD");

    private static Set<String> customerCache = new HashSet<>();
    private static Set<String> orderCache = new HashSet<>();
    private static Set<String> productCache = new HashSet<>();

    private static final int BATCH_SIZE = 5000;
    private static int processedCount = 0;

    private static String currentOrderId = null;

    private static List<Map<String, Object>> customerBatch = new ArrayList<>();
    private static List<Map<String, Object>> orderBatch = new ArrayList<>();
    private static List<Map<String, Object>> orderLineBatch = new ArrayList<>();

    public static void main(String[] args) {
        try {
            FileInputStream fis = new FileInputStream(System.getenv("XML_FILE_PATH"));
            XMLInputFactory factory = XMLInputFactory.newInstance();
            XMLStreamReader reader = factory.createXMLStreamReader(fis);

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

                Transaction tx = session.beginTransaction();

                while (reader.hasNext()) {
                    int event = reader.next();

                    switch (event) {
                        case XMLStreamConstants.START_ELEMENT:
                            currentElement = reader.getLocalName();

                            // Extract Customer information
                            if ("CustomerId".equals(currentElement)) {
                                customerId = reader.getElementText();
                                System.out.println("CustomerId: " + customerId);
                            } else if ("Name".equals(currentElement)) {
                                name = reader.getElementText();
                                System.out.println("Name: " + name);
                            } else if ("Email".equals(currentElement)) {
                                email = reader.getElementText();
                                System.out.println("Email: " + email);
                            } else if ("Age".equals(currentElement)) {
                                age = reader.getElementText();
                                System.out.println("Age: " + age);
                            }

                            // Extract Order information
                            else if ("OrderId".equals(currentElement)) {
                                currentOrderId = reader.getElementText();  // Store active orderId
                                System.out.println("OrderId: " + currentOrderId);
                            }

                            // Extract OrderLine information
                            else if ("OrderLineId".equals(currentElement)) {
                                orderLineId = reader.getElementText();
                                System.out.println("OrderLineId: " + orderLineId);
                            } else if ("ProductId".equals(currentElement)) {
                                productId = reader.getElementText();
                                System.out.println("ProductId: " + productId);
                            } else if ("Qty".equals(currentElement)) {
                                qty = reader.getElementText();
                                System.out.println("Qty: " + qty);
                            } else if ("Price".equals(currentElement)) {
                                price = reader.getElementText();
                                System.out.println("Price: " + price);
                            } else if ("Total".equals(currentElement) && orderLineId != null) {
                                lineTotal = reader.getElementText();
                                System.out.println("Total: " + lineTotal);
                            }
                            break;

                        case XMLStreamConstants.END_ELEMENT:
                            String endElement = reader.getLocalName();

                            // Handle the end of Customer
                            if ("Customer".equals(endElement)) {
                                if (customerId != null && !customerId.isEmpty()) {
                                    Map<String, Object> customerMap = new HashMap<>();
                                    customerMap.put("customerId", customerId);
                                    customerMap.put("name", name);
                                    customerMap.put("email", email);
                                    customerMap.put("age", age);
                                    customerBatch.add(customerMap);

                                    processedCount++;
                                }

                                // Reset Customer fields
                                customerId = null;
                                name = null;
                                email = null;
                                age = null;
                            }

                            // Handle the end of Order
                            if ("Order".equals(endElement)) {
                                if (currentOrderId != null) {
                                    Map<String, Object> orderMap = new HashMap<>();
                                    orderMap.put("orderId", currentOrderId);
                                    orderMap.put("orderTotal", orderTotal); // You should ensure you capture orderTotal from XML
                                    orderBatch.add(orderMap);
                                    processedCount++;
                                }
                                currentOrderId = null; // Reset for the next order
                            }

                            // Handle the end of OrderLine
                            if ("OrderLine".equals(endElement)) {
                                if (orderLineId != null && !orderLineId.isEmpty() && currentOrderId != null && productId != null && !productId.isEmpty()) {
                                    Map<String, Object> orderLineMap = new HashMap<>();
                                    orderLineMap.put("orderLineId", orderLineId);
                                    orderLineMap.put("price", price);
                                    orderLineMap.put("qty", qty);
                                    orderLineMap.put("lineTotal", lineTotal);
                                    orderLineMap.put("orderId", currentOrderId);  // Use the current orderId
                                    orderLineMap.put("productId", productId);
                                    orderLineBatch.add(orderLineMap);

                                    processedCount++;
                                }

                                // Reset OrderLine fields
                                orderLineId = null;
                                productId = null;
                                qty = null;
                                price = null;
                                lineTotal = null;

                                if (processedCount % BATCH_SIZE == 0) {
                                    executeBatches(tx);
                                    tx.commit();
                                    System.out.println("Committed " + processedCount + " records.");

                                    customerCache.clear();
                                    orderCache.clear();
                                    productCache.clear();
                                    System.gc();

                                    tx = session.beginTransaction();
                                    currentOrderId = null;
                                }
                            }
                    }
                }

                // Final commit after processing all data
                executeBatches(tx);
                tx.commit();
            } catch (Exception e) {
                e.printStackTrace();
            }

            reader.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void executeBatches(Transaction tx) {
        if (!customerBatch.isEmpty()) {
            tx.run("UNWIND $batchData as row " +
                            "MERGE (c:Customer {customerId: row.customerId}) " +
                            "SET c.name = row.name, c.email = row.email, c.age = row.age",
                    Collections.singletonMap("batchData", customerBatch));
            customerBatch.clear();
        }

        if (!orderBatch.isEmpty()) {
            tx.run("UNWIND $batchData as row " +
                            "MERGE (o:Order {orderId: row.orderId}) " +
                            "SET o.total = row.orderTotal " +
                            "MERGE (c:Customer {customerId: row.customerId}) " +
                            "MERGE (c)-[:PLACED]->(o)",
                    Collections.singletonMap("batchData", orderBatch));
            orderBatch.clear();
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
            orderLineBatch.clear();
        }
    }

}