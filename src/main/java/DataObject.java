/**
 * @author jbrincefield
 * @createdOn 10/3/2024 at 6:27 PM
 * @projectName RDBL1
 * @packageName PACKAGE_NAME;
 */
public class DataObject {
    private String seriesID;
    private int year;
    private String month;
    private double value;
    private String occupationID;


    public DataObject(String seriesID, int year, String month, double value,String occupationID) {
        this.seriesID = seriesID;
        this.year = year;
        this.month = month;
        this.value = value;
        this.occupationID = occupationID;
    }

    public String getSeriesID() {
        return seriesID;
    }

    public int getYear() {
        return year;
    }

    public String getMonth() {
        return month;
    }

    public double getValue() {
        return value;
    }

    public String getOccupationID() {
        return occupationID;
    }
}
