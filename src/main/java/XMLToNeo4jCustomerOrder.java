import org.neo4j.driver.*;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.FileInputStream;

public class XMLToNeo4jCustomerOrder {

    // Neo4j URI, username, and password
    private static final String NEO4J_URI = "bolt://localhost:7687";
    private static final String NEO4J_USER = "neo4j";
    private static final String NEO4J_PASSWORD = "password123";

    public static void main(String[] args) {
        // Set up StAX reader for large XML file
        try {
            FileInputStream fis = new FileInputStream("C:\\NEU\\Y2\\Q1\\PRO335-SB1\\M3\\customers.xml");
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

                while (reader.hasNext()) {
                    int event = reader.next();

                    switch (event) {
                        case XMLStreamConstants.START_ELEMENT:
                            currentElement = reader.getLocalName();

                            // Extract Customer information
                            if ("CustomerId".equals(currentElement)) {
                                customerId = reader.getElementText();
                                if (customerId == null || customerId.isEmpty()) {
                                    System.out.println("Warning: CustomerId is missing or empty!");
                                }
                            } else if ("Name".equals(currentElement)) {
                                name = reader.getElementText();
                            } else if ("Email".equals(currentElement)) {
                                email = reader.getElementText();
                            } else if ("Age".equals(currentElement)) {
                                age = reader.getElementText();
                            }

                            // Extract Order information
                            else if ("OrderId".equals(currentElement)) {
                                orderId = reader.getElementText();
                                if (orderId == null || orderId.isEmpty()) {
                                    System.out.println("Warning: OrderId is missing or empty!");
                                }
                            } else if ("Total".equals(currentElement) && orderId != null) {
                                orderTotal = reader.getElementText();
                            }

                            // Extract OrderLine information
                            else if ("OrderLineId".equals(currentElement)) {
                                orderLineId = reader.getElementText();
                                if (orderLineId == null || orderLineId.isEmpty()) {
                                    System.out.println("Warning: OrderLineId is missing or empty!");
                                }
                            } else if ("ProductId".equals(currentElement)) {
                                productId = reader.getElementText();
                                if (productId == null || productId.isEmpty()) {
                                    System.out.println("Warning: ProductId is missing or empty!");
                                }
                            } else if ("Qty".equals(currentElement)) {
                                qty = reader.getElementText();
                            } else if ("Price".equals(currentElement)) {
                                price = reader.getElementText();
                            } else if ("Total".equals(currentElement) && orderLineId != null) {
                                lineTotal = reader.getElementText();
                            }
                            break;

                        case XMLStreamConstants.END_ELEMENT:
                            // Handle the end of OrderLine
                            if ("OrderLine".equals(reader.getLocalName())) {
                                // Enhanced logging: Display which fields are missing
                                if (orderLineId == null || orderLineId.isEmpty()) {
                                    System.out.println("Skipping OrderLine: Missing OrderLineId");
                                }
                                if (orderId == null || orderId.isEmpty()) {
                                    System.out.println("Skipping OrderLine: Missing OrderId");
                                }
                                if (productId == null || productId.isEmpty()) {
                                    System.out.println("Skipping OrderLine: Missing ProductId");
                                }

                                if (orderLineId != null && !orderLineId.isEmpty() && orderId != null && !orderId.isEmpty() && productId != null && !productId.isEmpty()) {
                                    // Create an OrderLine node, link it to Order, and create Product node
                                    String orderLineQuery = "MERGE (ol:OrderLine {orderLineId: $orderLineId, price: $price, qty: $qty, total: $lineTotal}) " +
                                            "MERGE (o:Order {orderId: $orderId}) " +
                                            "MERGE (p:Product {productId: $productId}) " +
                                            "MERGE (o)-[:CONTAINS]->(ol) " +
                                            "MERGE (ol)-[:PRODUCT_OF]->(p)";
                                    session.run(orderLineQuery, Values.parameters("orderLineId", orderLineId, "price", price, "qty", qty,
                                            "lineTotal", lineTotal, "orderId", orderId, "productId", productId));

                                } else {
                                    System.out.println("Skipping OrderLine due to missing OrderLineId, OrderId, or ProductId.");
                                }

                                // Reset for the next order line
                                orderLineId = null;
                                productId = null;
                                qty = null;
                                price = null;
                                lineTotal = null;
                            }

                            // Handle the end of Order
                            if ("Order".equals(reader.getLocalName())) {
                                if (orderId != null && !orderId.isEmpty() && customerId != null && !customerId.isEmpty()) {
                                    // Create an Order node and link it to Customer
                                    String orderQuery = "MERGE (o:Order {orderId: $orderId, total: $orderTotal}) " +
                                            "MERGE (c:Customer {customerId: $customerId}) " +
                                            "MERGE (c)-[:PLACED]->(o)";
                                    session.run(orderQuery, Values.parameters("orderId", orderId, "orderTotal", orderTotal, "customerId", customerId));
                                } else {
                                    System.out.println("Skipping Order due to missing OrderId or CustomerId.");
                                }

                                // Reset for the next order
                                orderId = null;
                                orderTotal = null;
                            }

                            // Handle the end of Customer
                            if ("Customer".equals(reader.getLocalName())) {
                                if (customerId != null && !customerId.isEmpty()) {
                                    // Create the Customer node
                                    String customerQuery = "MERGE (c:Customer {customerId: $customerId, name: $name, email: $email, age: $age})";
                                    session.run(customerQuery, Values.parameters("customerId", customerId, "name", name, "email", email, "age", age));
                                } else {
                                    System.out.println("Skipping Customer due to missing CustomerId.");
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

                System.out.println("Data successfully loaded into Neo4j!");

            } catch (Exception e) {
                e.printStackTrace();
            }

            reader.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
