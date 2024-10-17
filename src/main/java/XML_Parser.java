import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.File;

public class XML_Parser {
    public static Customer parseXML(String filePath) {
        try {
            JAXBContext context = JAXBContext.newInstance(Customer.class);
            Unmarshaller unmarshaller = context.createUnmarshaller();
            return (Customer) unmarshaller.unmarshal(new File(filePath));
        } catch (JAXBException e) {
            e.printStackTrace();
            return null;
        }
    }
}