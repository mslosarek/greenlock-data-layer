const { MongoClient } = require('mongodb');

const types = [
  'accounts',
  'certificates',
  'keypairs',
  'configs',
  'challenges',
];
let initialized = false;
const accessor = {};
let client;
let db;

module.exports = function(config = {}) {
  const { datastore = {} } = config;
  datastore.url = datastore.url || process.env.MONGODB_CONNECTION_URL || null;
  datastore.database = datastore.database || process.env.MONGODB_CONNECTION_DATABASE || 'greenlock';
  datastore.collectionPrefix = datastore.collectionPrefix || process.env.MONGODB_CONNECTION_COLLECTION_PREFIX || '';
  datastore.collectionPrefix = datastore.collectionPrefix ? `${datastore.collectionPrefix}_` : '';

  accessor.init = async () => {
    if (!initialized) {
      client = new MongoClient(datastore.url);
      await client.connect();
      db = client.db(datastore.database);
      await db.command({ ping: 1 });
      initialized = true;
    }

    return accessor;
  };

  accessor.read = async (type, id) => {
    await accessor.init();
    console.log(['read', type, id]);
    const results = await db.collection(`${datastore.collectionPrefix}${type}`).findOne({
      _id: id,
    });

    return results || null;
  };

  accessor.write = async (type, id, data) => {
    await accessor.init();
    console.log(['write', type, id]);
    await db.collection(`${datastore.collectionPrefix}${type}`)
    .updateOne(
      {
        _id: id,
      },
      {
        $set: data,
      },
      {
        upsert: true,
      }
    );
    return null;
  };

  accessor.delete = async (type, id) => {
    await accessor.init();
    console.log(['delete', type, id]);
    const previousValue = await accessor.read(type, id);
    await db.collection(`${datastore.collectionPrefix}${type}`)
    .deleteOne({
      _id: id,
    });
    return previousValue;
  };

  accessor.all = async (type) => {
    await accessor.init();
    console.log(['all', type]);
    return (await db.collection(`${datastore.collectionPrefix}${type}`).find().toArray())
    .map(record => {
      const {
        _id,
        ...data
      } = record;

      return data;
    });
  };

  accessor.close = async () => {
    await client.close();
    initialized = false;
    client = null;
    db = null;
  };

  return accessor;
};
