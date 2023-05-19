const connect_to_db = require('./db');
talk = require('./schema');
module.exports.get_next = async (event, context, callback) => {
  context.callbackWaitsForEmptyEventLoop = false;
  if (event.titleNow) {
    body = event;
  }
  if (!body.titleNow) {
    callback(null, {
      statusCode: 500,
      headers: { 'Content-Type': 'text/plain' },
      body: 'non esiste title attuale',
    });
  } else {

    try {
      console.log("mi sto connettendo")
      await connect_to_db();
      console.log("connesso")
      
      const TalkDetail =  (await talk.findOne({title:body.titleNow}));
      console.log(TalkDetail.watch_next.length)
      
      const totalTalks =await TalkDetail.watch_next.length;
      const randomIndex = Math.floor(Math.random() * totalTalks);
      console.log("numero massimo next:"+totalTalks+",indice randomico:"+randomIndex)
      
      callback(null, {
        statusCode: 200,
        body: JSON.stringify(TalkDetail.watch_next[randomIndex]),
      });
    } catch (err) {
          callback(null, {
             statusCode: err.statusCode || 500,
             headers: { 'Content-Type': 'text/plain' },
             body: 'video non trovato',
          });
    }
    
    
    
  }
};