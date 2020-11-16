package entity;



public class TableInfo {

    public String event_time;

    public String date_time;

    public String db_name;

    public String table_name;

    public String primary_key;

    public String params;

    public String action_type;


    public String getDate_time() {
        return date_time;
    }

    public void setDate_time(String date_time) {
        this.date_time = date_time;
    }

    @Override
    public String toString() {
        return "TableInfo{" +
                "event_time='" + event_time + '\'' +
                ", date_time='" + date_time + '\'' +
                ", db_name='" + db_name + '\'' +
                ", table_name='" + table_name + '\'' +
                ", primary_key='" + primary_key + '\'' +
                ", params='" + params + '\'' +
                ", action_type='" + action_type + '\'' +
                '}';
    }

    public String getEvent_time() {
        return event_time;
    }

    public void setEvent_time(String event_time) {
        this.event_time = event_time;
    }

    public String getDb_name() {
        return db_name;
    }

    public void setDb_name(String db_name) {
        this.db_name = db_name;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public String getPrimary_key() {
        return primary_key;
    }

    public void setPrimary_key(String primary_key) {
        this.primary_key = primary_key;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public String getAction_type() {
        return action_type;
    }

    public void setAction_type(String action_type) {
        this.action_type = action_type;
    }

    public TableInfo() {
    }

    public TableInfo(String event_time, String date_time, String db_name, String table_name, String primary_key, String params, String action_type) {
        this.event_time = event_time;
        this.date_time = date_time;
        this.db_name = db_name;
        this.table_name = table_name;
        this.primary_key = primary_key;
        this.params = params;
        this.action_type = action_type;
    }
}
