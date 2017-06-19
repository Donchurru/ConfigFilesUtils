const fs = require('fs');

var resourcesFolder = "/home/rromero/Proyectos/spark-akka-etl/src/it/resources/";
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

	var jsonRead = resourcesTableFolder+"engineConfig"+tabla.destino+".json";
	if (fs.existsSync(jsonRead)){
		//console.log("\n\n");
		var data = fs.readFileSync(jsonRead, 'utf8');
		var json = JSON.parse(data);
		json.steps.forEach(function(step){
			step.beforeAll.forEach(function(beforeAll){
				if (beforeAll.sql != null && beforeAll.sql != "" && beforeAll.sql != undefined){
					if (beforeAll.sql.indexOf("${basePath}")>-1){
						beforeAll.type="ParquetPath";
						beforeAll.path = "${basePath}"+tabla.destino;
						beforeAll.tableName=tabla.destino;
						delete beforeAll.sql;
					}else if (beforeAll.sql.indexOf("${basePathDomain}")>-1){
						beforeAll.type="ParquetPath";
						var tableDomain = beforeAll.sql.substring(beforeAll.sql.indexOf("TABLE ")+6,beforeAll.sql.indexOf(" USING "));
						beforeAll.path = "${basePathDomain}"+tableDomain.toLowerCase()+"/fch_corte_datos=${fch_corte_datos}/num_ver_proc=${num_ver_proc}/flg_acep=S";
						beforeAll.tableName=tableDomain;
						delete beforeAll.sql;
					}else if (beforeAll.sql.indexOf("${homogenizationsPathFile}")>-1){

					}
				}else if (beforeAll.path != null && beforeAll.path != "" && beforeAll.path != undefined){
					if (beforeAll.path.indexOf("${basePathDomain}")>-1){
						var tableDomain = beforeAll.path.substring(beforeAll.path.indexOf("${basePathDomain}")+17);
						beforeAll.path = "${basePathDomain}"+tableDomain.toUpperCase();
					}
				}
			});
		}); 
		
		fs.writeFileSync(jsonRead, JSON.stringify(json,null,2), 'utf8');
	}
}