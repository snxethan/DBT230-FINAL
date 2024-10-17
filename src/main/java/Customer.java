import javax.xml.bind.annotation.*;
import java.util.List;

@XmlRootElement(name = "Customer")
@XmlAccessorType(XmlAccessType.FIELD)
public class Customer {
    private int Age;
    private int CustomerId;
    private String Email;
    private String Name;
    @XmlElementWrapper(name = "Orders")
    @XmlElement(name = "Order")
    private List<Order> Orders;

    // Getters and setters
}

