import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    private static final int BATCH_SIZE = 900000; // Define your batch size here
    //took 7 minutes and 20 seconds +- 5 seconds to process 38,861,474 records


    public static void connectNEO4J() {
        final String dbUri = "neo4j://localhost:7687";
        final String dbUser = "neo4j";
        final String dbPassword = "neo12345";
        Config config = Config.builder() // Configuring the connection
                .withMaxConnectionPoolSize(10000)
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
        String path = "C:\\Users\\jbrincefield\\Neumont classes\\Year 2\\Quarter 1\\Persisntence project\\nw.data.1.AllData.txt";

        try {
            File file = new File(path);
            if (file.exists()) {
                BufferedReader reader = new BufferedReader(new FileReader(file));
                String line;
                List<DataObject> batch = new ArrayList<>(); // Store objects in batch

                long count = 0;
                while ((line = reader.readLine()) != null) {
                    String[] data = line.split("\\s+");
                    if (data.length >= 4) {
                        count++;
                        if (data[3].matches("-")) data[3] = "0";

                        else if (!data[3].contains(".")) data[3] = data[3].substring(0, data[3].length() - 2) + "." + data[3].substring(data[3].length() - 2);

                        String occupationID = data[0].substring(17, 23);
                        DataObject object = new DataObject(data[0], Integer.parseInt(data[1]), data[2], Double.parseDouble(data[3]), occupationID);
                        batch.add(object);
                    }

                    // Process batch when it reaches the defined size
                    if (batch.size() == BATCH_SIZE) {
                        System.out.println("Batch processed " + count);
                        createNeoDataObjects(batch); // Process batch
                        batch.clear(); // Clear batch after processing
                        System.out.println("Batch cleared");
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
    public static void createNeoDataObjects(List<DataObject> dataObjects) {
        ensureConnection();
        try (Session session = driver.session()) {
            session.writeTransaction(tx -> {
                List<Map<String, Object>> params = new ArrayList<>();
                for (DataObject dataObject : dataObjects) {
                    Map<String, Object> param = Values.parameters(
                            "seriesID", dataObject.getSeriesID(),
                            "year", dataObject.getYear(),
                            "period", dataObject.getMonth(),
                            "value", dataObject.getValue(),
                            "occupationID", dataObject.getOccupationID()
                    ).asMap();
                    params.add(param);
                }

                tx.run("UNWIND $batch AS row " +
                                "CREATE (a:occupationSalary {seriesID: row.seriesID, year: row.year, period: row.period, value: row.value, occupationID: row.occupationID})",
                        Values.parameters("batch", params));

                return null;
            });
        }
    }

}
