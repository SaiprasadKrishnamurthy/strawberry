function transform(context) {
    print(JSON.stringify(help(context)));
}

function help(context) {
   var output = {
           "myId": context.configId
       };
   return output;
}