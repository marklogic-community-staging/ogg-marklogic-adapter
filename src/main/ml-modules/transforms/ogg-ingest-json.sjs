function newBaseDocument({ schemaName, tableName }) {
    const document = {
        envelope: {
            headers: {
                operations: []
            },
            triples: [],
            instance: {}
        }
    };

    document.envelope.instance[schemaName] = {};
    document.envelope.instance[schemaName][tableName] = {};

    return document;
}

function getBaseDocument({ uri, schemaName, tableName }) {
    const document = cts.doc(uri);
    return (document != null) ? document.toObject() : newBaseDocument({ schemaName, tableName });
}

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

