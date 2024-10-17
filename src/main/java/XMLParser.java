import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class XMLParser {
    public static void main(String[] args) {
        try {
            FileInputStream fileInputStream = new FileInputStream("path/to/largefile.xml");
            XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
            XMLStreamReader xmlStreamReader = xmlInputFactory.createXMLStreamReader(fileInputStream);

            JAXBContext jaxbContext = JAXBContext.newInstance(Customer.class);
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();

            while (xmlStreamReader.hasNext()) {
                if (xmlStreamReader.isStartElement() && "Customer".equals(xmlStreamReader.getLocalName())) {
                    Customer customer = (Customer) unmarshaller.unmarshal(xmlStreamReader);
                    // Process the customer object
                    importToNeo4j(customer);
                }
                xmlStreamReader.next();
            }
        } catch (FileNotFoundException | XMLStreamException | JAXBException e) {
            e.printStackTrace();
        }
    }

    private static void importToNeo4j(Customer customer) {
        Neo4jImporter.importToNeo4j(customer);
    }
}