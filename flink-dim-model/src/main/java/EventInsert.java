public class EventInsert {
    private BasicProp basicProp;
    private ExtendProp properties;

    public EventInsert() {
    }

    public EventInsert(BasicProp basicProp, ExtendProp properties) {
        this.basicProp = basicProp;
        this.properties = properties;
    }

    public BasicProp getBasicProp() {
        return basicProp;
    }

    public void setBasicProp(BasicProp basicProp) {
        this.basicProp = basicProp;
    }

    public ExtendProp getExtendProp() {
        return properties;
    }

    public void setProperties(ExtendProp properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "EventInsert{" +
                "basicProp=" + basicProp +
                ", extendProp=" + properties +
                '}';
    }
}
