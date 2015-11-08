var csv_parse = require('csv-parse');
var fs = require('fs');

var objectExtractorStream = require('./src/objectExtractorStream');
var esUpdateStream = require('./src/esUpdateStream');

var parser = csv_parse({delimiter: ','});

var filename = process.argv[2];

fs.createReadStream(filename)
.pipe(parser)
.pipe(objectExtractorStream)
.pipe(esUpdateStream)
