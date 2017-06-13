var fileRelationsPath = "codigo-interface-nombre-tabla.csv";
var resourcesFolder = "/home/rromero/IdeaProjects/spark-akka-etl/src/it/resources/";
var origenDestJsonFile = "origen-destino.json";
var fs = require('fs');

const readJson = require('read-json-file');

var onReadedJson = function (error, origenDest){
	
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
		
		var actualTable = origenDest.filter(function(obj){return obj["dest.cod_interfaz"] == tabla.id});
		
		if (actualTable.length>0){
			
			var fileCsvName = resourcesFolder+tabla.name+"/"+tabla.name+"_OLD.csv";
			var fileCsvOut = resourcesFolder+tabla.name+"/"+tabla.name+".csv";
			if (fs.existsSync(fileCsvOut)){
				fs.unlinkSync(fileCsvOut);
			}
			if (fs.existsSync(fileCsvName)){
				//console.log(fileCsv);
				var csvFile = fs.readFileSync(fileCsvName,'utf8');
				var linesCsv = csvFile.split("\n");

				var numLine = 0;
				var nullColumns = [];
				for (var lineCsvId in linesCsv){

					var lineCsv = linesCsv[lineCsvId];
					if (lineCsv.trim()!=""){

						if (numLine==0){
							for (var idTab in actualTable){
								var fila = actualTable[idTab];	
								if (fila["origen.nom_interfaz"] == "NULL"){
									nullColumns.push(fila["dest.nom_fis_campo"]);
								}else{
									var columnaOrigen = (fila["origen.nom_interfaz"] + "." + fila["origen.nom_fis_campo"]).toLowerCase();
									var columnaDestino = fila["dest.nom_fis_campo"];
									lineCsv = lineCsv.replace(columnaOrigen,columnaDestino);
								}
							}	

							for (var nullId in nullColumns){
								lineCsv = lineCsv+","+nullColumns[nullId];
							}

							fs.appendFileSync(fileCsvOut, lineCsv);

						}else{
							for (var nullId in nullColumns){
								lineCsv = lineCsv+",";
							}
							fs.appendFileSync(fileCsvOut, "\n"+lineCsv);
						}
					
					}

					numLine++;
				}
				
				var line999 = "";
				for (var actId in actualTable){
					if (actId==0){
						line999=line999+"999"
					}else{
						line999=line999+",999"	
					}
				}
				line999=line999+",999"
				if (line999!=""){
					fs.appendFileSync(fileCsvOut, "\n"+line999);
				}
				//console.log(tabla.name + " ----> " + JSON.stringify(nullColumns));
				
			}
		}
	}
}


readJson(origenDestJsonFile, onReadedJson);