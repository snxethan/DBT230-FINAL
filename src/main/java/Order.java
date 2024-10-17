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

    public int getCustomerId() {
        return CustomerId;
    }

    public void setCustomerId(int customerId) {
        CustomerId = customerId;
    }

    public int getOrderId() {
        return OrderId;
    }

    public void setOrderId(int orderId) {
        OrderId = orderId;
    }

    public int getTotal() {
        return Total;
    }

    public void setTotal(int total) {
        Total = total;
    }

    public List<OrderLine> getLines() {
        return Lines;
    }

    public void setLines(List<OrderLine> lines) {
        Lines = lines;
    }
}
