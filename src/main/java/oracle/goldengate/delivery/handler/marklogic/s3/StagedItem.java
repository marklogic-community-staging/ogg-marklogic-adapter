package oracle.goldengate.delivery.handler.marklogic.s3;

import oracle.goldengate.delivery.handler.marklogic.models.WriteListItem;

import java.nio.file.Path;

public class StagedItem {
    protected WriteListItem writeListItem;
    protected Path path;

    public StagedItem(Path path, WriteListItem writeListItem) {
        this.path = path;
        this.writeListItem = writeListItem;
    }

    public WriteListItem getWriteListItem() {
        return writeListItem;
    }

    public void setWriteListItem(WriteListItem writeListItem) {
        this.writeListItem = writeListItem;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }
}
