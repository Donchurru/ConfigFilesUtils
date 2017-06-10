var origenDestinoXls = "origen-destino.xlsx";
var origenDestinoJson = "origen-destino.json";
var sheetName = "Sheet";

var node_xj = require("xls-to-json");
  node_xj({
    input: origenDestinoXls,  // input xls 
    output: origenDestinoJson, // output json 
    sheet: sheetName  // specific sheetname 
  }, function(err, result) {
    if(err) {
      console.error(err);
    } else {
      console.log(result);
    }
});