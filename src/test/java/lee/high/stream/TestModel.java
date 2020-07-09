package lee.high.stream;

public class TestModel {
    private long id;
    private String value;

    TestModel() {}

    public TestModel(final long id, final String value) {
        this.id = id;
        this.value = value;
    }

    public long getId() {
        return id;
    }

    public String getValue() {
        return value;
    }
}
