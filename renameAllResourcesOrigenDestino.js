const fs = require('fs');

var resourcesFolder = "/home/rromero/Proyectos/spark-akka-etl/src/it/resources/";
var bddsFolder = "/home/rromero/Proyectos/spark-akka-etl/src/it/scala/com/digitalknowledge/etl/bdd/";
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
				origen: lineArr[1],
				destino: lineArr[2]
			};
	
	//Cambiamos nombre de los ficheros de dentro de la carpeta
	var resourcesTableFolder = resourcesFolder+tabla.origen+"/";
	renameFile(resourcesTableFolder+tabla.origen+".csv",resourcesTableFolder+tabla.destino+".csv");
	renameFile(resourcesTableFolder+tabla.origen+"_OLD.csv",resourcesTableFolder+tabla.destino+"_OLD.csv");
	renameFile(resourcesTableFolder+"engineConfig"+tabla.origen+".json",resourcesTableFolder+"engineConfig"+tabla.destino+".json");

	//Cambiamos el nombre de la carpeta
	if (tabla.origen!=""){
		renameFile(resourcesFolder+tabla.origen,resourcesFolder+tabla.destino);
	}

	//Cambiamos el Bdd de nombre
	renameFile(bddsFolder+tabla.origen+"BddIT.scala",bddsFolder+tabla.destino+"BddIT.scala");

	
}

function renameFile(origen,destino){
	
	if (fs.existsSync(origen)){
		fs.renameSync(origen, destino);
	}
}