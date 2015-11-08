var through = require('through2');

var pipe = through.obj(function(chunk, enc, callback) {
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
});

module.exports = pipe;
