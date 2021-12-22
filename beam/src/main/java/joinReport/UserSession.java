package joinReport;



import org.joda.time.Instant;

import java.io.Serializable;

public class UserSession implements Serializable {

    private static final String[] FILE_HEADER_MAPPING = {
            "UserId","ItemID"
    };
    private static final String[] FILE_HEADER_MAPPING_WITH_TITLE = {
    		"UserId","ItemID","Cost"
    };
    private String UserId;
    private String ItemID;
    private String cost;
 

    public String getUserId() {
        return UserId;
    }

    public void setUserId(String UserId) {
        this.UserId = UserId;
    }

    public String getItemID() {
        return ItemID;
    }

    public void setItemID(String ItemID) {
        this.ItemID= ItemID;
    }

    public String getCost() {
        return cost;
    }

    public void setCost(String cost) {
        this.cost = cost;
    }

    
    public String getAsString() {
        String s = "";
        s += this.UserId + "\t "
                + this.ItemID + "\t "
            
                + this.cost;
        return s;
    }

    public String asCSVRow(String delimiter) {
        String s = "";
        s += this.UserId + delimiter + " "
                + this.ItemID + delimiter + " "
                + this.cost;
        return s;
    }

    public String asCSVRowWithTitle(String delimiter) {
        String s = "";
        s +=  this.UserId + delimiter + " "
                + this.ItemID + delimiter + " "
                + this.cost;
        return s;
    }

    public static String getCSVHeaders(){
        String s = "";
        for (String column: FILE_HEADER_MAPPING){
            s += column + ", ";
        }
        return s.substring(0,s.length()-2);
    }

    public static String getCSVHeadersWithTitle(){
        String s = "";
        for (String column: FILE_HEADER_MAPPING_WITH_TITLE){
            s += column + ", ";
        }
        return s.substring(0,s.length()-2);
    }
}
