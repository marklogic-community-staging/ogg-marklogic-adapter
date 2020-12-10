package oracle.goldengate.delivery.handler.marklogic.models;

import java.util.LinkedList;
import java.util.List;

public class PendingItems {
    List<WriteListItem> items;
    List<WriteListItem> binaryItems;

    public PendingItems() {
        this.items = new LinkedList<>();
        this.binaryItems = new LinkedList<>();
    }

    public List<WriteListItem> getItems() {
        return items;
    }

    public void setItems(List<WriteListItem> items) {
        this.items = items;
    }

    public List<WriteListItem> getBinaryItems() {
        return binaryItems;
    }

    public void setBinaryItems(List<WriteListItem> binaryItems) {
        this.binaryItems = binaryItems;
    }
}
