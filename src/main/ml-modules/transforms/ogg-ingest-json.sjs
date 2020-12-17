function newBaseDocument({ schema, table }) {
    const document = {
        envelope: {
            headers: {
                columnUpdatedAt: {}
            },
            triples: [],
            instance: {},
        }
    }

    document.envelope.instance[schema] = {};
    document.envelope.instance[schema][table] = {};

    return document;
}

function getBaseDocument({ schema, table, uri }) {
    const document = cts.doc(uri);
    return (document != null) ? document.toObject() : newBaseDocument({ schema, table });
}

function highestScn(previous, current) {
    if(previous == null) {
        return current;
    } else if (current == null) {
        return previous;
    } else if(current > previous) {
        return current;
    } else {
        return previous;
    }
}

function ifNewer({ previous, current, doIfNewer = () => null, doIfOlder = () => null }) {
    if(previous == null || current == null || current >= previous) {
        doIfNewer();
    } else {
        doIfOlder();
    }
}

exports.transform = function transform(context, params, content) {
    const root = content.toObject();
    if(root == null) {
        // probably a binary
        return content;
    }

    const uri = context.uri;
    const headers = root.envelope.headers;
    const { scn, operation, operationTimestamp, schema, table } = headers;

    const baseDocument = getBaseDocument({ schema, table, uri });

    const baseInstance = baseDocument.envelope.instance[schema][table];
    const baseHeaders = baseDocument.envelope.headers;
    const instance = root.envelope.instance[schema][table];

    baseHeaders.schema = schema;
    baseHeaders.table = table;

    ifNewer({
        previous: baseHeaders.scn,
        current: scn,
        doIfNewer: () => {
            baseHeaders.operation = operation;
            baseHeaders.scn = scn;
            baseHeaders.operationTimestamp = operationTimestamp;
        }
    })

    Object.keys(instance).forEach(key => {
        const value = instance[key];
        const oldScn = baseHeaders.columnUpdatedAt[key];
        if(scn == null || oldScn == null || oldScn < scn) {
            baseHeaders.columnUpdatedAt[key] = scn;
            baseInstance[key] = value;
        }
    });

    baseHeaders.ingestedOn = fn.currentDateTime().toString();
    return baseDocument;
}

/*
exports.transform = function transform(context, params, content) {
    const root = content.toObject();
    if(root == null) {
        // binary file
        return content;
    }

    const uri = context.uri;

    const headers = root.envelope.headers;
    const operation = headers.operation;

    const schemaName = headers.sourceSchemaName;
    const tableName = headers.sourceTableName;

    const baseDocument = (operation === "insert") ?
        newBaseDocument({ schemaName, tableName }) :
        getBaseDocument({ schemaName, tableName, uri: (headers.previousUri || uri) });
    baseDocument.envelope.headers.operations = [ headers, ...baseDocument.envelope.headers.operations ];

    const baseInstance = baseDocument.envelope.instance[headers.sourceSchemaName][headers.sourceTableName];
    const instance = root.envelope.instance[headers.sourceSchemaName][headers.sourceTableName];

    Object.keys(instance).forEach(key => {
        const value = instance[key];
        if(value === null) {
            delete baseInstance[key];
        } else {
            baseInstance[key] = value;
        }
    });

    return baseDocument;
};
*/
