import org.neo4j.driver.*;

public class Neo4jUploader {
    private final Driver driver;

    public Neo4jUploader(String uri, String user, String password) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    public void close() {
        driver.close();
    }

    public void uploadCustomerData(Customer customer) {
        try (Session session = driver.session()) {
            session.writeTransaction(tx -> {
                tx.run("CREATE (c:Customer {CustomerId: $CustomerId, Age: $Age, Email: $Email, Name: $Name})",
                        Values.parameters("CustomerId", customer.getCustomerId(),
                                "Age", customer.getAge(),
                                "Email", customer.getEmail(),
                                "Name", customer.getName()));

                for (Order order : customer.getOrders()) {
                    tx.run("CREATE (o:Order {OrderId: $OrderId, Total: $Total, CustomerId: $CustomerId})",
                            Values.parameters("OrderId", order.getOrderId(),
                                    "Total", order.getTotal(),
                                    "CustomerId", order.getCustomerId()));

                    tx.run("MATCH (c:Customer {CustomerId: $CustomerId}), (o:Order {OrderId: $OrderId}) " +
                                    "CREATE (c)-[:PLACED]->(o)",
                            Values.parameters("CustomerId", customer.getCustomerId(),
                                    "OrderId", order.getOrderId()));

                    for (OrderLine line : order.getLines()) {
                        tx.run("CREATE (l:OrderLine {OrderLineId: $OrderLineId, Price: $Price, ProductId: $ProductId, Qty: $Qty, Total: $Total, OrderId: $OrderId})",
                                Values.parameters("OrderLineId", line.getOrderLineId(),
                                        "Price", line.getPrice(),
                                        "ProductId", line.getProductId(),
                                        "Qty", line.getQty(),
                                        "Total", line.getTotal(),
                                        "OrderId", order.getOrderId()));

                        tx.run("MATCH (o:Order {OrderId: $OrderId}), (l:OrderLine {OrderLineId: $OrderLineId}) " +
                                        "CREATE (o)-[:CONTAINS]->(l)",
                                Values.parameters("OrderId", order.getOrderId(),
                                        "OrderLineId", line.getOrderLineId()));
                    }
                }
                return null;
            });
        }
    }
}