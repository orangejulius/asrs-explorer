var through = require('through2');

var exceptions = [
    'Crew Size.Number Of Crew',
    'Batch Number',
    'Qualification.Flight Attendant'
];

var toWatch = [
    'Weather Elements / Visibility.Visibility',
];

var pipe = through.obj(function(chunk, encoding, callback) {
    if (exceptions.indexOf(chunk.attribute) === -1) {
        var parsedFloat = parseFloat(chunk.value);

        if (parsedFloat.toString() === chunk.value) {
            chunk.value = parsedFloat;
          // handle decimal values between 0 and 1 with no leading 0
        } else if (parsedFloat.toString() === '0' + chunk.value) {
            chunk.value = parsedFloat;
        }
    }

    this.push(chunk);
    callback();
});

module.exports = pipe
