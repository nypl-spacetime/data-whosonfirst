var fs = require('fs');
var path = require('path');
var H = require('highland');
var JSONStream = require('JSONStream');
var R = require('ramda');
var request = require('request');

var USE_THE_INTERNET = false;

var wofDataUrl = 'https://whosonfirst.mapzen.com/';
var wofDataPath = '/Volumes/LaCie/spacetime/whosonfirst-data/';

var concordancesUrl = 'https://raw.githubusercontent.com/whosonfirst/whosonfirst-data/master/meta/wof-concordances-latest.csv';
var concordancesPath = wofDataPath + 'meta/wof-concordances-20160120.csv';

var NY = '85688543';

function getFileStream(path) {
  return USE_THE_INTERNET ? request.get(path) : fs.createReadStream(path);
}

function getFile(path, callback) {
  return USE_THE_INTERNET ? request.get(path, callback) : fs.readFile(path, 'utf8', callback);
}

function getWofPath(wofId) {
  return R.apply(path.join, R.flatten(['data', R.splitEvery(3, wofId), wofId + '.geojson']));
}

function getWofFile(row, callback) {
  var wofId = row['wof:id'];
  var wofPath = getWofPath(wofId);

  var basePath = USE_THE_INTERNET ? wofDataUrl : wofDataPath;
  var path = basePath + wofPath;

  getFile(path, function(err, data) {
    if (data) {
      try {
        callback(null, JSON.parse(data));
      } catch (err) {
        console.log('JSON parse error', path);
        console.error(err);
        callback();
      }
    } else {
      console.log('getFile error', path);
      console.error(err);
      callback();
    }
  });
}

function hierarchyContains(wofId, geojson) {
  var belongsto = [];
  if (geojson.properties && geojson.properties['wof:belongsto']) {
    belongsto = geojson.properties['wof:belongsto'].map(function(n) {
      return n.toString();
    });
  }

  var includes = belongsto.indexOf(wofId) > -1;
  return includes;
}

function download(config, dir, writer, callback) {
  var first = true;
  var csvDelimiter = ',';
  var zipHeaders = R.identity;

  H(getFileStream(USE_THE_INTERNET ? concordancesUrl : concordancesPath))
    .split()
    .compact()
    .map(function(line) {
      var rows = line.split(csvDelimiter);
      if (first) {
        first = false;
        zipHeaders = R.zipObj(rows);
        return null;
      } else {
        return rows;
      }
    })
    .compact()
    .map(function(rows) {
      return zipHeaders(rows);
    })
    .map(H.curry(getWofFile))
    .nfcall([])
    .series()
    .compact()
    .filter(H.curry(hierarchyContains, NY))
    .map(JSON.stringify)
    .intersperse('\n')
    .pipe(fs.createWriteStream(path.join(dir, 'ny.ndjson')));
}

function convert(config, dir, writer, callback) {
  H(fs.createReadStream(path.join(dir, 'ny.ndjson')))
    .split()
    .map(JSON.parse)
    .map(function(wof) {
      return {
        id: wof.id,
        name: wof.properties['wof:name'],
        concordances: wof.properties['wof:concordances']
      };
    })
    .each(function(wof) {
      console.log(wof)
    })

}

// ==================================== API ====================================

module.exports.steps = [
  download,
  convert
];
