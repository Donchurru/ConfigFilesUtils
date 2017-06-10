
	
const readJson = require('read-json-file');
const writeJsonFile = require('write-json-file');
const fs = require('fs');

const jsonFolder = './JSON/';
var files;

fs.readdir(jsonFolder, (err, filesAux) => {
  //files.forEach(file => {
	files = filesAux
	readNext();
	
  //});
});

var readNext = function (){
	var file = files.pop();
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
    var jsonOut = new Object();
	
	jsonOut.steps = [];
	var tableName = data.graph.label;
	
	var stepOut = {};
	var stepsDominio = [];
	var validationObj = null;
	var standarizationObj = null;
	for (var nodoId in data.graph.nodes){
		var nodo = data.graph.nodes[nodoId];
		
		
		if (nodo.id == 0){
			var beforeAllObj = {};
			beforeAllObj.type = "PlainSql";
			beforeAllObj.sql = "CREATE TEMPORARY TABLE "+tableName+" USING com.databricks.spark.csv OPTIONS (path \"${basePath}"+tableName+".csv\", header \"true\", inferSchema \"false\")"
			stepOut.beforeAll = [];
			stepOut.beforeAll.push(beforeAllObj);
			
			var sourceObj = {};
			sourceObj.sql = "select * from "+tableName;	
			stepOut.source=sourceObj;
		}else if (nodo.label == "Mandatory"){
			if (stepOut.actorsConfig == null){
				stepOut.actorsConfig = [];
			}
			
			if (validationObj == null){
				validationObj = {};
				validationObj.type = "Validation";
				validationObj.validations = [];
			}
			
			for (var mandId in nodo.metadata.validations){
				var mandOrig = nodo.metadata.validations[mandId];
				var mandatoryObj = {};
				mandatoryObj.type = mandOrig.type;
				mandatoryObj.fieldName = mandOrig.fieldName;
				validationObj.validations.push(mandatoryObj);
			}
			
		}else if (nodo.label == "Standarizer"){
			if (stepOut.actorsConfig == null){
				stepOut.actorsConfig = [];
			}
			
			if (standarizationObj == null){
				standarizationObj = {};
				standarizationObj.type = "Standardization";
				standarizationObj.standardizations = [];
			}
			
			for (var standId in nodo.metadata.standardizations){
				var standOrig = nodo.metadata.standardizations[standId];
				var standObj = {};
				standObj.type = standOrig.type;
				standObj.sourceField = standOrig.sourceField;
				standObj.targetField = standOrig.targetField;
				standObj.metadata = standOrig.metadata;
				standarizationObj.standardizations.push(standObj);
			}
		}else if (nodo.label == "DomainValidation"){
			
			var fromTable = tableName+"_standardized_and_validated";
			var domainsErrorTables = [];
			
			for (var domainId in nodo.metadata.validations){
				var domainOrig = nodo.metadata.validations[domainId];
				
				var stepDominio = {};
				stepDominio.beforeAll = [];
				var beforeAllObj = {};
				var tableNameDomain = domainOrig.actor.validations[0].metadata.referenceTable;
				var fieldNameAlias = domainOrig.actor.validations[0].fieldName;
				var fieldNameRef = fieldNameAlias.replace("REFERENCED_","");
				beforeAllObj.type = "PlainSql";
				beforeAllObj.sql = "CREATE TEMPORARY TABLE "+tableNameDomain+" USING com.databricks.spark.csv OPTIONS (path \"${basePathDomain}"+tableNameDomain+".csv\", header \"true\", inferSchema \"false\")"
				stepDominio.beforeAll.push(beforeAllObj);

				stepDominio.source = {};
				var sqlDomain =  domainOrig.sql;
				sqlDomain = sqlDomain.replace("*","base.*, ref."+fieldNameRef+" "+fieldNameAlias);
				sqlDomain = sqlDomain.replace("left join","left outer join");
				sqlDomain = sqlDomain.replace("from "+tableName,"from "+fromTable);
				stepDominio.source.sql = sqlDomain;
				
				stepDominio.actorsConfig = [];
				var actorConfig = {};
				actorConfig.type = "Validation";
				actorConfig.validations = [];
				var validation = {};
				validation.type = "Mandatory";
				validation.fieldName = fieldNameAlias;
				validation.metadata = {};
				validation.metadata.referenceTable = tableNameDomain;
				actorConfig.validations.push(validation);
				
				stepDominio.actorsConfig.push(actorConfig);
				
				stepDominio.target = {};
				stepDominio.target.resultsAlias = tableName+"_standardized_validated_and_domain_temp"+domainId;
				stepDominio.target.errorsAlias = "errors_"+tableName+"_standardized_and_validated_and_domain"+domainId;
				
				fromTable = stepDominio.target.resultsAlias;
				domainsErrorTables.push(stepDominio.target.errorsAlias);
				
				stepDominio.afterAll=[];
				
				stepsDominio.push(stepDominio);
			}
			
			if (stepsDominio.length>0){
				var stepDominioAfterAll = stepsDominio[stepsDominio.length-1];
				var afterAllOKObj = {};
				var afterAllKOObj = {};

				afterAllOKObj.type="TempTable";
				afterAllOKObj.tableName=tableName+"_domain_validated";
				afterAllOKObj.sql="select * from "+fromTable;
				
				afterAllKOObj.type="TempTable";
				afterAllKOObj.tableName=tableName+"_errors";
				
				var koSQL = "select * from errors_"+tableName+"_standardized_and_validated";
				
				for (var domErrId in domainsErrorTables){
					var domainErrorTable = domainsErrorTables[domErrId];
					koSQL = koSQL + " union all select * from "+domainErrorTable;
				}
				
				afterAllKOObj.sql=koSQL;
				
				stepDominioAfterAll.afterAll.push(afterAllOKObj);
				stepDominioAfterAll.afterAll.push(afterAllKOObj);
			}
		
		}else if (nodo.label == tableName+"_accepted"){
			if (stepOut.target==null){
				stepOut.target = {};
			}
			stepOut.target.resultsAlias = tableName+"_standardized_and_validated";
		}else if (nodo.label == tableName+"_rejected"){
			if (stepOut.target==null){
				stepOut.target = {};
			}
			stepOut.target.errorsAlias = "errors_"+tableName+"_standardized_and_validated";
		}
	}
	stepOut.afterAll= [];
	if (validationObj != null){
		stepOut.actorsConfig.push(validationObj);
	}
	if (standarizationObj != null){
		stepOut.actorsConfig.push(standarizationObj);
	}
	
	if (stepOut.beforeAll!=null){
		jsonOut.steps.push(stepOut);
	}
	if (stepsDominio.length>0){
		jsonOut.steps = jsonOut.steps.concat(stepsDominio);
	}
	
	var writeFolder = "/home/rromero/IdeaProjects/spark-akka-etl/src/it/resources/";
	var writeFile = writeFolder+tableName+"/engineConfig"+tableName+".json";
	writeJsonFile(writeFile, jsonOut).then(() => {
		console.log("\n ESCRITO: "+writeFile);
		readNext();
	});

	//console.log("\n"+JSON.stringify(jsonOut));	
	
}