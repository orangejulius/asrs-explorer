var through = require('through2');
var csv_parse = require('csv-parse');
var fs = require('fs');
var elasticsearch = require('elasticsearch');

var parser = csv_parse({delimiter: ','});
var client = new elasticsearch.Client({
    host: 'localhost:9200'
});

var filename = process.argv[2];

fs.createReadStream(filename)
.pipe(parser)
.pipe(through.obj(function(chunk, enc, callback) {
    if (chunk[0] === 'ITEM_ID') {
        console.log(chunk);
    } else {
        var id = parseInt(chunk[0]);
        var entity = chunk[1];
        var enumerator = chunk[2];
        var attribute = chunk[3];
        var value = chunk[4];

        var doc = {
            id: id,
            entity: entity,
            enumerator: enumerator,
            attribute: attribute,
            value: value
        };

        this.push(doc);
    }
    callback();
})).pipe(through.obj(function(chunk, encoding, callback) {
    var record = chunk;

    client.get({
        index: 'asrs',
        type: 'asrs',
        id: record.id
    }, function(error, response) {
        if (error && error.status !== '404') {
            console.log(error);
            console.log(response);
            return callback();
        } else {
            if (response.found) {
                var existing = response._source;
                existing[record.entity] = existing[record.entity] || {};
                existing[record.entity][record.enumerator] = existing[record.entity][record.enumerator] || {};
                existing[record.entity][record.enumerator][record.attribute] = record.value;
                client.update({
                    index: 'asrs',
                    type: 'asrs',
                    id: record.id,
                    body: {
                        doc: existing
                    }
                }, function(error2, response2) {
                    if (error2) console.log(error2);
                    return callback();
                });
            } else {
                var doc = {};
                doc[record.entity] = {};
                doc[record.entity][record.enumerator] = {};
                doc[record.entity][record.enumerator][record.attribute] = record.value;
                client.create({
                    index: 'asrs',
                    type: 'asrs',
                    id: record.id,
                    body: doc

                }, function(error2, response2) {
                    if (error2) console.log(error2);
                    return callback();
                });
            }
        }
    });
}));
