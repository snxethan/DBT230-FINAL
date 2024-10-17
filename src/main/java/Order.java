import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
public class Order {
    private int CustomerId;
    private int OrderId;
    private int Total;
    @XmlElementWrapper(name = "Lines")
    @XmlElement(name = "OrderLine")
    private List<OrderLine> Lines;

    // Getters and setters
}
