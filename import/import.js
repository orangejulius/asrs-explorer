var csv_parse = require('csv-parse');
var fs = require('fs');

var objectExtractorStream = require('./src/objectExtractorStream');
var dateFixStream = require('./src/dateFixStream');
var numberParsingStream = require('./src/numberParsingStream');
var esUpdateStream = require('./src/esUpdateStream');

var parser = csv_parse({delimiter: ','});

var filename = process.argv[2];

fs.createReadStream(filename)
.pipe(parser)
.pipe(objectExtractorStream)
.pipe(dateFixStream)
.pipe(esUpdateStream)
