function transform(line) {
var values = line.split(',');
var custObj = new Object();
obj.name = values[0];
obj.gender = values[1];
obj.age = values[2];
obj.city = values[3];
var outJson = JSON.stringify(custObj);
return outJson;
}
