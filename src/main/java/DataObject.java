/**
 * @author jbrincefield
 * @createdOn 10/3/2024 at 6:27 PM
 * @projectName RDBL1
 * @packageName PACKAGE_NAME;
 */
public class DataObject {
    private String industryCode;
    private String occupationCode;
    private String year;
    private String period;
    private String value;

    public DataObject(String industryCode, String occupationCode, String year, String period, String value) {
        this.industryCode = industryCode;
        this.occupationCode = occupationCode;
        this.year = year;
        this.period = period;
        this.value = value;
    }

    public String getIndustryCode() {
        return industryCode;
    }

    public String getOccupationCode() {
        return occupationCode;
    }

    public String getYear() {
        return year;
    }

    public String getPeriod() {
        return period;
    }

    public String getValue() {
        return value;
    }
}
