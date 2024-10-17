import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

@XmlAccessorType(XmlAccessType.FIELD)
class OrderLine {
    private int OrderLineId;
    private int Price;
    private int ProductId;
    private int Qty;
    private int Total;

    // Getters and setters

    public int getOrderLineId() {
        return OrderLineId;
    }

    public void setOrderLineId(int orderLineId) {
        OrderLineId = orderLineId;
    }

    public int getPrice() {
        return Price;
    }

    public void setPrice(int price) {
        Price = price;
    }

    public int getProductId() {
        return ProductId;
    }

    public void setProductId(int productId) {
        ProductId = productId;
    }

    public int getQty() {
        return Qty;
    }

    public void setQty(int qty) {
        Qty = qty;
    }

    public int getTotal() {
        return Total;
    }

    public void setTotal(int total) {
        Total = total;
    }
}