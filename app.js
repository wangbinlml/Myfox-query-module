var mm = require('./index');

mm.invoke('select * from numsplit where thedate=20110610 and cid=1',function(data){
 console.info(JSON.stringify(data))
 });
