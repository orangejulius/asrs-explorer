var elasticsearch = require('elasticsearch');
var through = require('through2');

var client = new elasticsearch.Client({
    host: 'localhost:9200'
});

var pipe = through.obj(function(chunk, encoding, callback) {
    var record = chunk;

    client.get({
        index: 'asrs',
        type: 'record',
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
                    type: 'record',
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
                    type: 'record',
                    id: record.id,
                    body: doc

                }, function(error2, response2) {
                    if (error2) console.log(error2);
                    return callback();
                });
            }
        }
    });
});

module.exports = pipe;
