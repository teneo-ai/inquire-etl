package inquireetl.inquirehandler.v2.models;

public class AggregationMethod {

    private Method method;
    private Iterable<OrderKey> orderKeys;
    private Iterable<String> keys;

    public AggregationMethod() {
    }

    public void setMethod(Method method) {
        this.method = method;
    }

    public void setOrderKeys(Iterable<OrderKey> orderKeys) {
        this.orderKeys = orderKeys;
    }

    public void setKeys(Iterable<String> keys) {
        this.keys = keys;
    }

    public static class OrderKey {

        private String key;
        private Direction direction;

        public OrderKey() {
        }

        public void setKey(String key) {
            this.key = key;
        }

        public void setDirection(Direction direction) {
            this.direction = direction;
        }

        public enum Direction {
            ASCENDING,
            DESCENDING
        }
    }

    public enum Method {
        APPEND,
        REPLACE,
        UPSERT
    }
}
