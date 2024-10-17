import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import java.util.Map;

public class Neo4jImporter {
    private static final Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "neo12345"));

    public static void importToNeo4j(Customer customer) {
        try (Session session = driver.session()) {
            session.writeTransaction(tx -> {
                tx.run("CREATE (c:Customer {CustomerId: $CustomerId, Age: $Age, Email: $Email, Name: $Name})",
                        Map.of("CustomerId", customer.getCustomerId(), "Age", customer.getAge(), "Email", customer.getEmail(), "Name", customer.getName()));

                for (Order order : customer.getOrders()) {
                    tx.run("CREATE (o:Order {OrderId: $OrderId, Total: $Total})",
                            Map.of("OrderId", order.getOrderId(), "Total", order.getTotal()));
                    tx.run("MATCH (c:Customer {CustomerId: $CustomerId}), (o:Order {OrderId: $OrderId}) CREATE (c)-[:PLACED]->(o)",
                            Map.of("CustomerId", customer.getCustomerId(), "OrderId", order.getOrderId()));

                    for (OrderLine line : order.getLines()) {
                        tx.run("CREATE (l:OrderLine {OrderLineId: $OrderLineId, Price: $Price, ProductId: $ProductId, Qty: $Qty, Total: $Total})",
                                Map.of("OrderLineId", line.getOrderLineId(), "Price", line.getPrice(), "ProductId", line.getProductId(), "Qty", line.getQty(), "Total", line.getTotal()));
                        tx.run("MATCH (o:Order {OrderId: $OrderId}), (l:OrderLine {OrderLineId: $OrderLineId}) CREATE (o)-[:CONTAINS]->(l)",
                                Map.of("OrderId", order.getOrderId(), "OrderLineId", line.getOrderLineId()));
                    }
                }
                return null;
            });
        }
    }
}