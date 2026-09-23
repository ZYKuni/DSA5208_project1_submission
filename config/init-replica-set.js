const replicaSetConfig = {
  _id: "rs0",
  members: [
    { _id: 0, host: "mongo1:27017" },
    { _id: 1, host: "mongo2:27017" },
    { _id: 2, host: "mongo3:27017" },
  ],
};

function replicaSetIsInitialized() {
  try {
    return rs.status().ok === 1;
  } catch (error) {
    if (error.code === 94 || error.codeName === "NotYetInitialized") {
      return false;
    }
    throw error;
  }
}

if (replicaSetIsInitialized()) {
  print("Replica set rs0 is already initialized; skipping rs.initiate().");
} else {
  print("Initializing replica set rs0...");
  const result = rs.initiate(replicaSetConfig);
  if (result.ok !== 1) {
    throw new Error(`rs.initiate() failed: ${JSON.stringify(result)}`);
  }
  printjson(result);
}
