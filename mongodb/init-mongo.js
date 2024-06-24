db = db.getSiblingDB('movies');

db.createCollection('films', { capped: false });
