import pandas as pd

def get_collections(db):
    """
    Get all collection names in the database.
    
    :param db: MongoDB database object
    :return: List of collection names
    """
    return db.list_collection_names()

def mongodb_fetch_data(db, collection_name, columns=None, filter_query=None, limit=1000):
    """
    Fetch data from a collection and return as a DataFrame.
    """
    collection = db[collection_name]
    
    # Build projection if columns are specified
    if columns:
        # make sure columns is a list of strings
        columns = [str(col) for col in columns]
        projection = {col: 1 for col in columns}
    else:
        projection = None

    cursor = collection.find(filter_query or {}, projection).limit(limit)
    
    df = pd.DataFrame(list(cursor))
    
    # Drop _id if exists
    if '_id' in df.columns:
        df = df.drop(columns=['_id'])
        
    return df
