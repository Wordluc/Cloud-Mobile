const connect_to_db = require('./db');

module.exports.get_by_clock = async (event, context, callback) => {
  context.callbackWaitsForEmptyEventLoop = false;
  if (event.orario) {
    body = event;
  }
  if (!body.orario) {
    callback(null, {
      statusCode: 500,
      headers: { 'Content-Type': 'text/plain' },
      body: 'Orario non esistente',
    });
  } else {
    let talk;
    if (body.orario === 'morning') {
      talk = require('./RangeCostumers_morning');
    } else if (body.orario === 'afternoon') {
      talk = require('./RangeCostumers_afternoon');
    }

    try {
      await connect_to_db();
      console.log('get talk ' + body.orario);
      const totalTalks = await talk.countDocuments();

      // Generate a random index within the range of total documents
      const randomIndex = Math.floor(Math.random() * totalTalks);

      // Retrieve a random talk using .find() and .skip()
      const randomTalk = await talk.find()
        .skip(randomIndex)
        .limit(1);

      callback(null, {
        statusCode: 200,
        body: JSON.stringify(randomTalk),
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