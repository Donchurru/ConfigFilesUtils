const fs = require('fs');

var resourcesFolder = "/home/rromero/IdeaProjects/spark-akka-etl/src/it/resources/";
const readJson = require('read-json-file');
var fileRelationsPath = "codigo-interface-nombre-tabla.csv";

var fileRelations = fs.readFileSync(fileRelationsPath,'utf8');

var linesRealations = fileRelations.split("\n");
for (var lineID in linesRealations){
	var line = linesRealations[lineID];
	var lineArr = line.split(",");
		var tabla =
			{
				id: lineArr[0],
				name: lineArr[1]
			};
	
	var fileName = resourcesFolder+tabla.name+"/"+tabla.name+"_NEW.csv";
	var newFileName = resourcesFolder+tabla.name+"/"+tabla.name+".csv";
	
	if (fs.existsSync(fileName)){
		fs.renameSync(fileName, newFileName);
	}
	
}