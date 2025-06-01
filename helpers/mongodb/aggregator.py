def aggregate_data_mongo(db, collection_name, agg_func, field):
    pipeline = []

    if agg_func in ["sum", "avg", "min", "max"]:
        pipeline.append({
            "$group": {
                "_id": None,
                "result": {f"${agg_func}": f"${field}"}
            }
        })
    elif agg_func == "count":
        pipeline.append({
            "$group": {
                "_id": None,
                "result": {"$sum": 1}
            }
        })

    result = list(db[collection_name].aggregate(pipeline))
    return result[0]["result"] if result else None


def group_by_mongo(db, collection_name, group_field, agg_func, agg_field):
    pipeline = [
        {
            "$group": {
                "_id": f"${group_field}",
                "result": {f"${agg_func}": f"${agg_field}"}
            }
        },
        {
            "$sort": {"result": -1}
        }
    ]
    cursor = db[collection_name].aggregate(pipeline)
    return list(cursor)


def top_n_mongo(db, collection_name, field, n=5):
    pipeline = [
        {
            "$group": {
                "_id": f"${field}",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        },
        {
            "$limit": n
        }
    ]
    cursor = db[collection_name].aggregate(pipeline)
    return list(cursor)


def distinct_mongo(db, collection_name, field):
    return db[collection_name].distinct(field)