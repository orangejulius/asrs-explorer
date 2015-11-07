var through = require('through2');
var csv_parse = require('csv-parse');
var fs = require('fs');

var all_items_filename = '/home/spectre256/repos/asrs-data/extracted/ALL_ITEMS_DATA_TABLE.csv';
var text_filename = '~/repos/asrs-data/extracted/TEXT_DATA_TABLE.csv';

var parser = csv_parse({delimiter: ','});

var records = {};

var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
    host: 'localhost:9200',
    log: 'debug'
});

var i = 0;

fs.createReadStream(all_items_filename)
.pipe(parser)
.pipe(through.obj(function(chunk, enc, callback) {
    //[ 'ITEM_ID',
        //'ENTITY',
        //'ENUMERATOR',
        //'ATTRIBUTE',
        //'VALUE',
        //'DISPLAY_VALUE' ]
    if (chunk[0] === 'ITEM_ID') {
        console.log(chunk);
    } else {
        var doc;

        var id = parseInt(chunk[0]);
        var entity = chunk[1];
        var attribute = chunk[3];
        var value = chunk[4];

        if (!entity) {
            console.log("entity empty in record " + id);
        }

        if (!attribute) {
            console.log("attribute empty in record " + id);
        }

        if (!value) {
            console.log("value empty in record " + id + " ('" + value + "')");
        }

        if (records[id]) {
            doc = records[id];
        } else {
            doc = {
                id: id
            }
        }

        doc[entity] = doc[entity] || {};
        doc[entity][attribute] = value;

        records[id] = doc;
    }
    callback();
})).on('finish', function() {
    console.log(Object.keys(records).length + " objects imported");

    Object.keys(records).forEach(function(record_id) {
        var record = records[record_id];

        client.create({
            index: 'asrs',
            type: 'asrs',
            id: record.id,
            body: record
        }, function(error, response) {
            if (error) {
                console.log(response);
            }
        });
    });
});
