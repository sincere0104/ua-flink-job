public class BasicProp {
    private Long id;
    private String distinct_id;
    private String event_code;
    private String user_id ;
    private String _datetime;
    private String _date;

    public BasicProp() {
    }

    public BasicProp(Long id, String distinct_id, String event_code, String user_id, String _datetime, String _date) {
        this.id = id;
        this.distinct_id = distinct_id;
        this.event_code = event_code;
        this.user_id = user_id;
        this._datetime = _datetime;
        this._date = _date;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDistinct_id() {
        return distinct_id;
    }

    public void setDistinct_id(String distinct_id) {
        this.distinct_id = distinct_id;
    }

    public String getEvent_code() {
        return event_code;
    }

    public void setEvent_code(String event_code) {
        this.event_code = event_code;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String get_datetime() {
        return _datetime;
    }

    public void set_datetime(String _datetime) {
        this._datetime = _datetime;
    }

    public String get_date() {
        return _date;
    }

    public void set_date(String _date) {
        this._date = _date;
    }

    @Override
    public String toString() {
        return "BasicProp{" +
                "id='" + id + '\'' +
                ", distinct_id='" + distinct_id + '\'' +
                ", event_code='" + event_code + '\'' +
                ", user_id='" + user_id + '\'' +
                ", _datetime='" + _datetime + '\'' +
                ", _date='" + _date + '\'' +
                '}';
    }
}
