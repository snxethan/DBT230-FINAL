import java.util.concurrent.TimeUnit;
import org.neo4j.driver.*;

/**
 * @author jbrincefield
 * @createdOn 10/2/2024 at 6:22 PM
 * @projectName RDBL1
 * @packageName PACKAGE_NAME;
 */
public class Neo4jController {
    private static Driver driver; // NEO4J driver

    public static void connectNEO4J(){
        final String dbUri = "neo4j://localhost:7687";
        final String dbUser = "neo4j";
        final String dbPassword = "neo12345";
        Config config = Config.builder() // Configuring the connection
                .withMaxConnectionPoolSize(50)
                .withConnectionAcquisitionTimeout(60, TimeUnit.SECONDS)
                .withMaxTransactionRetryTime(15, TimeUnit.SECONDS)
                .build();

        Driver _driver = GraphDatabase.driver(dbUri, AuthTokens.basic(dbUser, dbPassword), config); // Creating the driver
        try  {
            _driver.verifyConnectivity(); // Verifying the connection
            System.out.println("Connected to NEO4J database");
        } catch (Exception e) {
            System.out.println("Error connecting to NEO4J database" + e.getMessage());
        }
        driver = _driver;
    }

    public static void closeNEO4J() {
        if (driver != null) {
            driver.close(); // Close the driver
        }
    }

    public static void ensureConnection(){
        if(driver == null){
            connectNEO4J(); // Connect to the database
        }
    }

    
}
