const fs = require('fs');

var resourcesFolder = "/home/rromero/Proyectos/spark-akka-etl/src/it/resources/";
var bddsFolder = "/home/rromero/Proyectos/spark-akka-etl/src/it/scala/com/digitalknowledge/etl/bdd/";
const readJson = require('read-json-file');
var replaceall = require("replaceall");
var fileRelationsPath = "codigo-interface-nombre-tabla.csv";

var fileRelations = fs.readFileSync(fileRelationsPath,'utf8');

var linesRealations = fileRelations.split("\n");
for (var lineID in linesRealations){
	var line = linesRealations[lineID];
	var lineArr = line.split(",");
		var tabla =
			{
				id: lineArr[0],
				origen: lineArr[1],
				destino: lineArr[2]
			};
	
	//Cambiamos nombre de los ficheros de dentro de la carpeta
	var resourcesTableFolder = resourcesFolder+tabla.destino+"/";

	var fileRead1 = resourcesTableFolder+"engineConfig"+tabla.destino+".json";
	if (fs.existsSync(fileRead1)){
		var data = fs.readFileSync(fileRead1, 'utf8');
		var result = replaceall(tabla.origen, tabla.destino, data);
		fs.writeFileSync(fileRead1, result, 'utf8');
	}

	var fileRead2 = resourcesTableFolder+"engineConfig.json";
	if (fs.existsSync(fileRead2)){
		var data = fs.readFileSync(fileRead2, 'utf8');
		var result = replaceall(tabla.origen, tabla.destino, data);
		fs.writeFileSync(fileRead2, result, 'utf8');
	}

	var fileRead3 = bddsFolder+tabla.origen+"BddIT.scala";
	if (fs.existsSync(fileRead3)){
		var data = fs.readFileSync(fileRead3, 'utf8');
		var result = replaceall(tabla.origen, tabla.destino, data);
		fs.writeFileSync(fileRead3, result, 'utf8');
	}
	
}