const readJson = require('read-json-file');
const writeJsonFile = require('write-json-file');
const fs = require('fs');

const jsonFolder = './JSON/';
var files;
var file;

fs.readdir(jsonFolder, (err, filesAux) => {
  //files.forEach(file => {
	files = filesAux
	readNext();
	
  //});
});

var readNext = function (){
	file = files.pop();
	if (file!==undefined){
		var nameFile = jsonFolder+file+"/engineJSON.json";
		console.log("\nLEYENDO:  "+nameFile);
		readJson(nameFile, transformJson);
	}
}

var transformJson = function (error, data){
 	if (error) {
        throw error;
    }	
	
	var writeFolder = "/home/rromero/IdeaProjects/spark-akka-etl/src/it/resources/";
	var writeFile = writeFolder+file+"/engineConfig.json";
	writeJsonFile(writeFile, data).then(() => {
		console.log("\n ESCRITO: "+writeFile);
		readNext();
	});
	
}