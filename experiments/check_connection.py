import os

from pymongo import MongoClient

uri = os.environ["MONGODB_URI"]

with MongoClient(uri, serverSelectionTimeoutMS=5000) as client:
    # 检查数据库是否可以响应
    print("Ping:", client.admin.command("ping")["ok"])

    # 查看副本集和当前主节点
    hello = client.admin.command("hello")
    print("Replica set:", hello.get("setName"))
    print("Primary:", hello.get("primary"))
    print("Hosts:", hello.get("hosts", []))
    print("Passives:", hello.get("passives", []))
    print(
        "All data members:",
        sorted(set(hello.get("hosts", []) + hello.get("passives", []))),
    )

    # 写入并读取一条练习数据
    collection = client["member_b_practice"]["python_check"]

    result = collection.insert_one({
        "message": "Hello from Python",
        "version": 1,
    })

    document = collection.find_one({"_id": result.inserted_id})
    print("Read result:", document)

    assert document is not None, "未读取到刚写入的文档"
    assert document["version"] == 1, "读取到的版本不符合预期"

    print("PASS: Python 连接和基本读写成功")