import javax.xml.bind.annotation.*;
import java.util.List;

@XmlRootElement(name = "Customers")
@XmlAccessorType(XmlAccessType.FIELD)
public class Customers {
    @XmlElement(name = "Customer")
    private List<Customer> customers;

    // Getters and setters
    public List<Customer> getCustomers() {
        return customers;
    }

    public void setCustomers(List<Customer> customers) {
        this.customers = customers;
    }
}