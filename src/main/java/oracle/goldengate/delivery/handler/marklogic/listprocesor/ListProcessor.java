package oracle.goldengate.delivery.handler.marklogic.listprocesor;

import java.util.List;

public interface ListProcessor<T> {
    void process(List<T> items);
}
