var through = require('through2');

var pipe = through.obj(function(chunk, encoding, callback) {
    if (chunk.attribute === 'Date') {
        var year = chunk.value.substring(0, 4);
        var month = chunk.value.substring(4);
        var day = '01'; // Records do not store day so invent one
        var date = year + '-' + month + '-' + day;
        chunk.value = date;
    }

    this.push(chunk);
    callback();
});

module.exports = pipe;
