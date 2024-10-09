import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
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
    private static final int BATCH_SIZE = 1000; // Define your batch size here


    public static void connectNEO4J() {
        final String dbUri = "neo4j://localhost:7687";
        final String dbUser = "neo4j";
        final String dbPassword = "neo12345";
        Config config = Config.builder() // Configuring the connection
                .withMaxConnectionPoolSize(50)
                .withConnectionAcquisitionTimeout(60, TimeUnit.SECONDS)
                .withMaxTransactionRetryTime(15, TimeUnit.SECONDS)
                .build();

        Driver _driver = GraphDatabase.driver(dbUri, AuthTokens.basic(dbUser, dbPassword), config); // Creating the driver
        try {
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

    public static void ensureConnection() {
        if (driver == null) {
            connectNEO4J(); // Connect to the database
        }
    }

    public static void createDataObjectFromFile() {
        String path = "C:\\NEU\\Y2\\Q1\\PRO335-SB1\\M1\\nw.data.1.AllData.txt";

        try {
            File file = new File(path);
            if (file.exists()) {
                BufferedReader reader = new BufferedReader(new FileReader(file));
                String line;
                List<DataObject> batch = new ArrayList<>(); // Store objects in batch

                while ((line = reader.readLine()) != null) {
                    String[] data = line.split("\\s+");
                    if (data.length >= 5) {
                        DataObject object = new DataObject(data[0], data[1], data[2], data[3], data[4]);
                        System.out.println("Adding to batch: " + object);
                        batch.add(object);
                    }

                    // Process batch when it reaches the defined size
                    if (batch.size() == BATCH_SIZE) {
                        createNeoDataObjects(batch); // Process batch
                        batch.clear(); // Clear batch after processing
                    }
                }

                // Process any remaining records in the batch
                if (!batch.isEmpty()) {
                    createNeoDataObjects(batch);
                }

                reader.close();
            } else {
                System.out.println("File not found: " + path);
            }
        } catch (FileNotFoundException e) {
            System.out.println("File not found: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Error reading file: " + e.getMessage());
        }
    }

    // Batch processing for Neo4j
    public static void createNeoDataObjects (List < DataObject > dataObjects) {
        ensureConnection();
        try (Session session = driver.session()) {
            session.writeTransaction(tx -> {
                for (DataObject dataObject : dataObjects) {
                    tx.run("CREATE (a:occupationSalary {industryCode: $industryCode, occupationCode: $occupationCode, year: $year, period: $period, value: $value})",
                            Values.parameters("industryCode", dataObject.getIndustryCode(),
                                    "occupationCode", dataObject.getOccupationCode(),
                                    "year", dataObject.getYear(),
                                    "period", dataObject.getPeriod(),
                                    "value", dataObject.getValue()));
                }
                return null; // Return null for the transaction
            });
        }
    }
}
